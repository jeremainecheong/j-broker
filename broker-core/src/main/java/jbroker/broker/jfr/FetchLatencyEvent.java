package jbroker.broker.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

/** per-Fetch RPC wall-clock latency + bytes returned to the client. */
@Name("jbroker.FetchLatency")
@Label("Fetch Latency")
@Description("Server-observed fetch RPC latency including zero-copy transfer time.")
@Category({"j-broker", "Data Plane"})
@StackTrace(false)
public final class FetchLatencyEvent extends Event {

    @Label("Topic")
    public String topic;

    @Label("Partition")
    public int partition;

    @Label("Latency")
    @Timespan(Timespan.NANOSECONDS)
    public long latencyNanos;

    @Label("Bytes")
    public long bytes;
}
