package jbroker.bench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Pure loss/duplication accounting for thesoak invariant:
 * every <b>acked</b> record must be consumed exactly once, and <b>no</b>
 * record may appear more than once. Records that were sent but never
 * acked (client saw an error / broker died mid-produce) are allowed to
 * appear once or not at all — at-most-once for unacked, exactly-once for
 * acked.
 */
public final class SoakLedger {

    private SoakLedger() {}

    public record Result(List<String> missing, List<String> duplicated, int ackedCount, int consumedCount) {
        public boolean clean() {
            return missing.isEmpty() && duplicated.isEmpty();
        }

        public String summary() {
            return "acked=" + ackedCount + " consumed=" + consumedCount + " missing=" + missing.size() + " duplicated="
                    + duplicated.size();
        }
    }

    public static Result compare(List<String> acked, List<String> consumed) {
        var counts = new HashMap<String, Integer>();
        for (var c : consumed) counts.merge(c, 1, Integer::sum);

        var missing = new ArrayList<String>();
        for (var a : new HashSet<>(acked)) {
            if (!counts.containsKey(a)) missing.add(a);
        }
        var duplicated = new ArrayList<String>();
        counts.forEach((k, v) -> {
            if (v > 1) duplicated.add(k);
        });
        missing.sort(String::compareTo);
        duplicated.sort(String::compareTo);
        return new Result(List.copyOf(missing), List.copyOf(duplicated), acked.size(), consumed.size());
    }
}
