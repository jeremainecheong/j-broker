package jbroker.broker.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/** fired from {@code TopicManager.onPartitionChange} when the leader flips. */
@Name("jbroker.PartitionLeaderChange")
@Label("Partition Leader Change")
@Description("Leader broker changed for a topic-partition.")
@Category({"j-broker", "Replication"})
@StackTrace(false)
public final class PartitionLeaderChangeEvent extends Event {

    @Label("Topic")
    public String topic;

    @Label("Partition")
    public int partition;

    @Label("Old Leader")
    public int oldLeader;

    @Label("New Leader")
    public int newLeader;

    @Label("Leader Epoch")
    public int leaderEpoch;
}
