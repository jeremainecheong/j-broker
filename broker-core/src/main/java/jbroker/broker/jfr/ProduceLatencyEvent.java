package jbroker.broker.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

/** per-Produce RPC wall-clock latency + payload bytes. */
@Name("jbroker.ProduceLatency")
@Label("Produce Latency")
@Description("Server-observed produce RPC latency including ack wait for acks=all.")
@Category({"j-broker", "Data Plane"})
@StackTrace(false)
public final class ProduceLatencyEvent extends Event {

    @Label("Topic")
    public String topic;

    @Label("Partition")
    public int partition;

    @Label("Latency")
    @Timespan(Timespan.NANOSECONDS)
    public long latencyNanos;

    @Label("Bytes")
    public long bytes;

    @Label("Acks")
    public int acks;
}
