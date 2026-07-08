package jbroker.storage.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

/**
 * Wraps {@code LogSegment.force()} on the append path. One event
 * per invocation covers the log file, offset index, and time index flushes
 * together; separating them would triple the JFR event rate without
 * meaningful signal — they're always called as a batch.
 */
@Name("jbroker.FsyncDuration")
@Label("Fsync Duration")
@Description("Duration of a FileChannel.force() cycle across one segment's files.")
@Category({"j-broker", "Storage"})
@StackTrace(false)
public final class FsyncDurationEvent extends Event {

    @Label("Base Offset")
    public long baseOffset;

    @Label("Duration")
    @Timespan(Timespan.NANOSECONDS)
    public long durationNanos;

    @Label("Size Bytes")
    public long sizeBytes;
}
