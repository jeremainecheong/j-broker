package jbroker.sim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Term;

/**
 * Runtime Raft safety checkers. Accumulated observations from
 * {@link Simulator} are compared against four invariants (Raft §5):
 *
 * <ol>
 *   <li><b>Election safety</b> — at most one leader per term.</li>
 *   <li><b>State-machine safety</b> — no two nodes apply different entries at
 *       the same index.</li>
 *   <li><b>Log matching</b> — if two logs have an entry at the same index, and
 *       the terms match, every preceding entry matches too.</li>
 *   <li><b>Commit monotonicity</b> — a node's committed entry at some index is
 *       never replaced by a different entry at the same index.</li>
 * </ol>
 *
 * <p>Violations are fatal: they indicate a correctness bug in the Raft
 * implementation. The simulator replays the same seed deterministically, so
 * any violation can be reproduced under a debugger.
 */
public final class Invariants {

    public static final class Violation extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public Violation(String message) {
            super(message);
        }
    }

    /** {@code (term, leaderId)} observed, to check election safety. */
    private final Map<Term, NodeId> leaderPerTerm = new HashMap<>();

    /** {@code (nodeId, index) -> (term, payload-hash)} for state-machine safety. */
    private final Map<AppliedKey, AppliedRecord> appliedLookup = new HashMap<>();

    private record AppliedKey(NodeId node, long index) {}

    private record AppliedRecord(Term term, int payloadHash) {}

    /**
     * Record that {@code node} just became leader at {@code term}. Throws
     * {@link Violation} if a different node already claimed leadership at
     * this term.
     */
    public void onBecameLeader(NodeId node, Term term) {
        var prior = leaderPerTerm.get(term);
        if (prior != null && !prior.equals(node)) {
            throw new Violation("election-safety: two leaders at term " + term + ": " + prior + " and " + node);
        }
        leaderPerTerm.put(term, node);
    }

    /**
     * Record that {@code node} applied {@code entry}. Throws {@link Violation}
     * if a different entry was previously applied at the same (node, index)
     * or if another node applied a conflicting entry at that index.
     */
    public void onApplied(NodeId node, LogEntry entry) {
        var key = new AppliedKey(node, entry.index());
        var record = new AppliedRecord(entry.term(), java.util.Arrays.hashCode(entry.payload()));
        var prior = appliedLookup.put(key, record);
        if (prior != null && !prior.equals(record)) {
            throw new Violation("state-machine-safety: " + node + " applied a different entry at index " + entry.index()
                    + ": was " + prior + ", now " + record);
        }
        // Cross-node check: any other node that applied an entry at this
        // index must have the same term+payload.
        for (var e : appliedLookup.entrySet()) {
            if (e.getKey().node().equals(node)) continue;
            if (e.getKey().index() != entry.index()) continue;
            if (!e.getValue().equals(record)) {
                throw new Violation("state-machine-safety: divergent applied entries at index " + entry.index() + ": "
                        + e.getKey().node() + "=" + e.getValue() + ", " + node + "=" + record);
            }
        }
    }

    /**
     * Log-matching check. For every pair of nodes, walk the overlapping index
     * range: if two logs agree on an entry's term at some index, every entry
     * before that index must also agree (Raft §5.3 log-matching property).
     */
    public void checkLogMatching(Map<NodeId, Simulator.Node> nodes) {
        var list = new ArrayList<>(nodes.values());
        for (int i = 0; i < list.size(); i++) {
            for (int j = i + 1; j < list.size(); j++) {
                var a = list.get(i);
                var b = list.get(j);
                long limit = Math.min(a.log.lastIndex(), b.log.lastIndex());
                long start = Math.max(a.log.firstIndex(), b.log.firstIndex());
                boolean prefixAgrees = true;
                for (long idx = start; idx <= limit; idx++) {
                    var ta = a.log.termAt(idx);
                    var tb = b.log.termAt(idx);
                    if (ta.isEmpty() || tb.isEmpty()) continue;
                    boolean sameTerm = ta.get().equals(tb.get());
                    if (sameTerm) {
                        var payloadA = a.log.read(idx, 1);
                        var payloadB = b.log.read(idx, 1);
                        if (payloadA.isEmpty() || payloadB.isEmpty()) continue;
                        if (java.util.Arrays.hashCode(payloadA.get(0).payload())
                                != java.util.Arrays.hashCode(payloadB.get(0).payload())) {
                            throw new Violation("log-matching: same (index=" + idx + ", term=" + ta.get()
                                    + ") but different payloads on " + a.id + " vs " + b.id);
                        }
                        if (!prefixAgrees) {
                            throw new Violation("log-matching: terms match at index " + idx
                                    + " but a preceding entry diverged between " + a.id + " and " + b.id);
                        }
                    } else {
                        prefixAgrees = false;
                    }
                }
            }
        }
    }
}
