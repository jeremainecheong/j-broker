package jbroker.bench;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import org.HdrHistogram.Histogram;

/**
 * One perf-run summary: human-readable stdout block plus one
 * environment-stamped CSV row. Every scenario emits through this class so
 * {@code docs/bench/results.csv} stays a single schema.
 *
 * <p>Latency semantics are carried per row in {@code latency_kind}
 * (per_rpc, per_record, per_fetch_rpc) — two kinds must never share a
 * column-blind comparison, so a scenario holding two histograms emits two
 * rows with identical throughput fields.
 *
 * <p>Percentile validity: p99 requires ≥ {@value #P99_MIN_SAMPLES}
 * histogram samples and p999 requires ≥ {@value #P999_MIN_SAMPLES};
 * below the threshold the cell is left EMPTY and a warning is printed to
 * stderr. The {@code samples} column always carries the histogram count
 * so suppression is auditable. Inapplicable fields are empty cells, never
 * zero.
 */
final class PerfReport {

    static final int P99_MIN_SAMPLES = 100;
    static final int P999_MIN_SAMPLES = 1_000;

    static final String HEADER = "timestamp,git_sha,hostname,os,cpu_model,jdk_version,mode,latency_kind,acks,"
            + "partitions,replication_factor,min_insync_replicas,payload_size,batch_size,linger_ms,warmup_records,"
            + "compression,flush_messages,records,bytes,elapsed_s,records_per_s,bytes_per_s,samples,"
            + "p50_us,p99_us,p999_us,max_us";

    private PerfReport() {}

    static Row row(String mode) {
        return new Row(mode);
    }

    static final class Row {
        private final String mode;
        private String latencyKind;
        private String acks;
        private Integer partitions;
        private Integer replicationFactor;
        private Integer minInsyncReplicas;
        private Integer payloadSize;
        private Long batchSize;
        private Long lingerMs;
        private Long warmupRecords;
        private String compression;
        private Long flushMessages;
        private long records;
        private long bytes;
        private long elapsedNanos;
        private Histogram histogram;

        private Row(String mode) {
            this.mode = mode;
        }

        Row latencyKind(String v) {
            this.latencyKind = v;
            return this;
        }

        Row acks(String v) {
            this.acks = v;
            return this;
        }

        Row partitions(Integer v) {
            this.partitions = v;
            return this;
        }

        Row replicationFactor(Integer v) {
            this.replicationFactor = v;
            return this;
        }

        Row minInsyncReplicas(Integer v) {
            this.minInsyncReplicas = v;
            return this;
        }

        Row payloadSize(Integer v) {
            this.payloadSize = v;
            return this;
        }

        Row batchSize(Long v) {
            this.batchSize = v;
            return this;
        }

        Row lingerMs(Long v) {
            this.lingerMs = v;
            return this;
        }

        Row warmupRecords(Long v) {
            this.warmupRecords = v;
            return this;
        }

        Row compression(String v) {
            this.compression = v;
            return this;
        }

        /** Topic-level {@code flush.messages} override; empty cell = broker default (lazy). */
        Row flushMessages(Long v) {
            this.flushMessages = v;
            return this;
        }

        Row records(long v) {
            this.records = v;
            return this;
        }

        Row bytes(long v) {
            this.bytes = v;
            return this;
        }

        Row elapsedNanos(long v) {
            this.elapsedNanos = v;
            return this;
        }

        Row histogram(Histogram v) {
            this.histogram = v;
            return this;
        }

        void emit(Path csvFile) throws IOException {
            emit(csvFile, BenchEnv.detect(), Instant.now());
        }

        void emit(Path csvFile, BenchEnv env, Instant now) throws IOException {
            long samples = histogram == null ? 0 : histogram.getTotalCount();
            double elapsedSec = elapsedNanos / 1e9;
            double rps = elapsedSec > 0 ? records / elapsedSec : 0;
            double bps = elapsedSec > 0 ? bytes / elapsedSec : 0;

            String p50 = samples >= 1 ? us(histogram.getValueAtPercentile(50.0)) : "";
            String max = samples >= 1 ? us(histogram.getMaxValue()) : "";
            String p99 = "";
            String p999 = "";
            if (histogram != null && samples < P99_MIN_SAMPLES) {
                warn("p99", P99_MIN_SAMPLES, samples);
            } else if (samples >= P99_MIN_SAMPLES) {
                p99 = us(histogram.getValueAtPercentile(99.0));
            }
            if (histogram != null && samples < P999_MIN_SAMPLES) {
                warn("p999", P999_MIN_SAMPLES, samples);
            } else if (samples >= P999_MIN_SAMPLES) {
                p999 = us(histogram.getValueAtPercentile(99.9));
            }

            System.out.printf(
                    Locale.ROOT,
                    "=== %s perf summary ===%n"
                            + "records   : %,d  (warmup excluded: %s)%n"
                            + "bytes     : %,d%n"
                            + "elapsed   : %.3fs%n"
                            + "throughput: %,.0f records/s  (%s)%n"
                            + "latency   : kind=%s samples=%d p50=%s p99=%s p999=%s max=%s%n",
                    mode,
                    records,
                    warmupRecords == null ? "n/a" : String.format(Locale.ROOT, "%,d", warmupRecords),
                    bytes,
                    elapsedSec,
                    rps,
                    humanBytesPerSec(bps),
                    latencyKind == null ? "-" : latencyKind,
                    samples,
                    p50.isEmpty() ? "-" : p50 + "µs",
                    p99.isEmpty() ? "-" : p99 + "µs",
                    p999.isEmpty() ? "-" : p999 + "µs",
                    max.isEmpty() ? "-" : max + "µs");

            if (csvFile == null) return;

            Files.createDirectories(csvFile.toAbsolutePath().getParent());
            boolean fresh = !Files.exists(csvFile) || Files.size(csvFile) == 0;
            try (BufferedWriter w = Files.newBufferedWriter(
                    csvFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)) {
                if (fresh) {
                    w.write(HEADER);
                    w.write('\n');
                }
                var cells = new String[] {
                    now.truncatedTo(ChronoUnit.SECONDS).toString(),
                    BenchEnv.sanitize(env.gitSha()),
                    BenchEnv.sanitize(env.hostname()),
                    BenchEnv.sanitize(env.os()),
                    BenchEnv.sanitize(env.cpuModel()),
                    BenchEnv.sanitize(env.jdkVersion()),
                    mode,
                    cell(latencyKind),
                    cell(acks),
                    cell(partitions),
                    cell(replicationFactor),
                    cell(minInsyncReplicas),
                    cell(payloadSize),
                    cell(batchSize),
                    cell(lingerMs),
                    cell(warmupRecords),
                    cell(compression),
                    cell(flushMessages),
                    Long.toString(records),
                    Long.toString(bytes),
                    String.format(Locale.ROOT, "%.6f", elapsedSec),
                    String.format(Locale.ROOT, "%.2f", rps),
                    String.format(Locale.ROOT, "%.2f", bps),
                    Long.toString(samples),
                    p50,
                    p99,
                    p999,
                    max,
                };
                w.write(String.join(",", cells));
                w.write('\n');
            }
        }

        private void warn(String percentile, int threshold, long samples) {
            System.err.printf(
                    Locale.ROOT,
                    "warning: %s omitted for mode=%s: histogram has %d samples, %d required%n",
                    percentile,
                    mode,
                    samples,
                    threshold);
        }
    }

    private static String cell(Object v) {
        return v == null ? "" : String.valueOf(v);
    }

    private static String us(long nanos) {
        return String.format(Locale.ROOT, "%.2f", nanos / 1_000.0);
    }

    private static String humanBytesPerSec(double bps) {
        if (bps < 1024) return String.format(Locale.ROOT, "%.0f B/s", bps);
        if (bps < 1024 * 1024) return String.format(Locale.ROOT, "%.1f KiB/s", bps / 1024);
        return String.format(Locale.ROOT, "%.2f MiB/s", bps / (1024 * 1024));
    }
}
