package jbroker.raft.core.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Raft {@code currentTerm} advanced. Emitted from the Raft core
 * immediately after the term flip; the persistent-state write completes
 * independently on the same thread so the event's timestamp is a useful
 * correlation anchor with surrounding log-commit events.
 */
@Name("jbroker.RaftTermChange")
@Label("Raft Term Change")
@Description("Raft currentTerm advanced. Fires once per transition.")
@Category({"j-broker", "Raft"})
@StackTrace(false)
public final class RaftTermChangeEvent extends Event {

    @Label("Old Term")
    public long oldTerm;

    @Label("New Term")
    public long newTerm;

    @Label("Reason")
    public String reason;
}
