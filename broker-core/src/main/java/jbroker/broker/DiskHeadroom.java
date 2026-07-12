package jbroker.broker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches usable space on the data volume. While it sits below the
 * configured headroom, leaders refuse client produces with retriable
 * {@code STORAGE_FULL}; fetch, replication, offset commits, and admin all
 * keep serving. The headroom is the buffer that keeps follower appends and
 * metadata commits alive while producers back off — without it the broker
 * runs the disk to zero and dies mid-write instead of degrading.
 *
 * <p>The produce path reads a volatile flag; a single background probe
 * refreshes it. The first probe runs synchronously in the constructor so a
 * broker booted onto a full volume degrades immediately rather than after
 * the first tick. Recovery is automatic: the next probe that sees space
 * above the watermark clears the flag (retention tick, topic deletion,
 * operator freeing space).
 */
public final class DiskHeadroom implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DiskHeadroom.class);

    /** Cluster default watermark: 1 GiB of usable space on the data volume. */
    public static final long DEFAULT_HEADROOM_BYTES = 1024L * 1024 * 1024;

    /** Default probe cadence. A statvfs-shaped syscall every 10s is noise. */
    public static final long DEFAULT_PROBE_INTERVAL_MILLIS = 10_000L;

    private static final DiskHeadroom DISABLED = new DiskHeadroom();

    private final long headroomBytes;
    private final LongSupplier usableBytes;
    private final ScheduledExecutorService probe;
    private volatile boolean low;
    private volatile long lastUsableBytes = Long.MAX_VALUE;

    /** Never trips. For tests and single-purpose tooling that has no data volume. */
    public static DiskHeadroom disabled() {
        return DISABLED;
    }

    private DiskHeadroom() {
        this.headroomBytes = 0;
        this.usableBytes = () -> Long.MAX_VALUE;
        this.probe = null;
    }

    /** Production shape: probe the file store backing {@code dataDir}. */
    public DiskHeadroom(Path dataDir, long headroomBytes) {
        this(usableSpaceSupplier(dataDir), headroomBytes, DEFAULT_PROBE_INTERVAL_MILLIS);
    }

    /**
     * Test shape: inject the space supplier and probe cadence. The supplier
     * runs on the probe thread only; a throwing supplier keeps the previous
     * state (a transient statvfs failure must not flap the gate).
     */
    public DiskHeadroom(LongSupplier usableBytes, long headroomBytes, long probeIntervalMillis) {
        if (headroomBytes < 1) {
            throw new IllegalArgumentException("headroomBytes must be ≥ 1, got " + headroomBytes);
        }
        this.headroomBytes = headroomBytes;
        this.usableBytes = usableBytes;
        probeOnce();
        this.probe = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = Thread.ofVirtual().unstarted(r);
            t.setName("disk-headroom-probe");
            return t;
        });
        this.probe.scheduleAtFixedRate(
                this::probeOnce, probeIntervalMillis, probeIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private static LongSupplier usableSpaceSupplier(Path dataDir) {
        return () -> {
            try {
                return Files.getFileStore(dataDir).getUsableSpace();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void probeOnce() {
        long usable;
        try {
            usable = usableBytes.getAsLong();
        } catch (RuntimeException e) {
            log.debug("disk-headroom probe failed; keeping previous state (low={})", low, e);
            return;
        }
        lastUsableBytes = usable;
        boolean nowLow = usable < headroomBytes;
        if (nowLow != low) {
            if (nowLow) {
                log.warn(
                        "data volume below headroom: {} usable < {} watermark — refusing client produces "
                                + "(fetch/replication/admin unaffected)",
                        usable,
                        headroomBytes);
            } else {
                log.info(
                        "data volume recovered: {} usable ≥ {} watermark — accepting produces again",
                        usable,
                        headroomBytes);
            }
            low = nowLow;
        }
    }

    /** True while usable space on the data volume sits below the watermark. */
    public boolean low() {
        return low;
    }

    /** Usable bytes observed by the most recent probe. */
    public long lastUsableBytes() {
        return lastUsableBytes;
    }

    /** The configured watermark. */
    public long headroomBytes() {
        return headroomBytes;
    }

    @Override
    public void close() {
        if (probe != null) {
            probe.shutdownNow();
        }
    }
}
