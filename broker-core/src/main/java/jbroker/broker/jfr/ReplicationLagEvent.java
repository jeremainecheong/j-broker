package jbroker.broker.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * P9.2 — periodic follower-lag signal. Leader fires this every time it
 * records a follower's {@code ReplicaFetch} with a non-trivial lag; the
 * emit side applies a minimum-threshold guard so we don't spam under
 * steady-state replication.
 */
@Name("jbroker.ReplicationLag")
@Label("Replication Lag")
@Description("Follower LEO lag relative to leader LEO, in records.")
@Category({"j-broker", "Replication"})
@StackTrace(false)
public final class ReplicationLagEvent extends Event {

    @Label("Topic")
    public String topic;

    @Label("Partition")
    public int partition;

    @Label("Follower Broker Id")
    public int followerBrokerId;

    @Label("Lag Records")
    public long lagRecords;
}
