package jbroker.bench;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import org.HdrHistogram.Histogram;

/**
 * Formats + prints a single perf run summary. Both the producer and the
 * consumer paths emit the same shape so downstream tooling (README table,
 * quick diffs between runs) can parse without switching modes.
 */
final class PerfReport {

    private PerfReport() {}

    /**
     * @param mode            "producer" or "consumer" — gates the throughput
     *                        unit label only
     * @param histogram       HdrHistogram with values in nanoseconds
     * @param records         total records produced/consumed
     * @param bytes           total payload bytes (excluding gRPC framing)
     * @param elapsedNanos    wall-clock duration of the run
     * @param csvFile         optional — when non-null, append one CSV row
     */
    static void emit(String mode, Histogram histogram, long records, long bytes, long elapsedNanos, Path csvFile)
            throws Exception {
        double elapsedSec = elapsedNanos / 1e9;
        double rps = records / elapsedSec;
        double bps = bytes / elapsedSec;

        System.out.printf(
                Locale.ROOT,
                "=== %s perf summary ===%n"
                        + "records   : %,d%n"
                        + "bytes     : %,d%n"
                        + "elapsed   : %.3fs%n"
                        + "throughput: %,.0f records/s  (%s)%n"
                        + "latency   : p50=%s  p99=%s  p999=%s  max=%s%n",
                mode,
                records,
                bytes,
                elapsedSec,
                rps,
                humanBytesPerSec(bps),
                humanMicros(histogram.getValueAtPercentile(50.0)),
                humanMicros(histogram.getValueAtPercentile(99.0)),
                humanMicros(histogram.getValueAtPercentile(99.9)),
                humanMicros(histogram.getMaxValue()));

        if (csvFile != null) {
            Files.createDirectories(csvFile.toAbsolutePath().getParent());
            boolean fresh = !Files.exists(csvFile);
            try (BufferedWriter w = Files.newBufferedWriter(
                    csvFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE)) {
                if (fresh) {
                    w.write("mode,records,bytes,elapsed_s,records_per_s,bytes_per_s,p50_us,p99_us,p999_us,max_us\n");
                }
                w.write(String.format(
                        Locale.ROOT,
                        "%s,%d,%d,%.6f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f%n",
                        mode,
                        records,
                        bytes,
                        elapsedSec,
                        rps,
                        bps,
                        histogram.getValueAtPercentile(50.0) / 1_000.0,
                        histogram.getValueAtPercentile(99.0) / 1_000.0,
                        histogram.getValueAtPercentile(99.9) / 1_000.0,
                        histogram.getMaxValue() / 1_000.0));
            }
        }
    }

    private static String humanMicros(long nanos) {
        double us = nanos / 1_000.0;
        if (us < 1_000) return String.format(Locale.ROOT, "%.1fµs", us);
        if (us < 1_000_000) return String.format(Locale.ROOT, "%.2fms", us / 1_000.0);
        return String.format(Locale.ROOT, "%.2fs", us / 1_000_000.0);
    }

    private static String humanBytesPerSec(double bps) {
        if (bps < 1024) return String.format(Locale.ROOT, "%.0f B/s", bps);
        if (bps < 1024 * 1024) return String.format(Locale.ROOT, "%.1f KiB/s", bps / 1024);
        return String.format(Locale.ROOT, "%.2f MiB/s", bps / (1024 * 1024));
    }
}
