package jbroker.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PerfReportTest {

    private static final BenchEnv ENV = new BenchEnv("abc1234", "test-host", "test-os", "test-cpu", "21.0.1");
    private static final Instant NOW = Instant.parse("2026-01-02T03:04:05Z");

    private static final List<String> COLUMNS = Arrays.asList(PerfReport.HEADER.split(",", -1));

    @TempDir
    Path tmp;

    private static Histogram histogramWith(int samples) {
        var h = new Histogram(60L * 1_000_000_000L, 3);
        for (int i = 0; i < samples; i++) {
            h.recordValue(1_000L * (i + 1));
        }
        return h;
    }

    private List<String> emitAndReadCells(PerfReport.Row row) throws IOException {
        Path csv = tmp.resolve("out.csv");
        Files.deleteIfExists(csv);
        row.emit(csv, ENV, NOW);
        var lines = Files.readAllLines(csv);
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).isEqualTo(PerfReport.HEADER);
        return Arrays.asList(lines.get(1).split(",", -1));
    }

    private static String cell(List<String> cells, String column) {
        int i = COLUMNS.indexOf(column);
        assertThat(i).as("column %s exists", column).isNotNegative();
        return cells.get(i);
    }

    @Test
    void headerIsExactAndWrittenOnce() throws IOException {
        Path csv = tmp.resolve("h.csv");
        PerfReport.row("producer").records(1).bytes(1).elapsedNanos(1_000_000).emit(csv, ENV, NOW);
        PerfReport.row("producer").records(1).bytes(1).elapsedNanos(1_000_000).emit(csv, ENV, NOW);
        var lines = Files.readAllLines(csv);
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0))
                .isEqualTo("timestamp,git_sha,hostname,os,cpu_model,jdk_version,mode,latency_kind,acks,"
                        + "partitions,replication_factor,min_insync_replicas,payload_size,batch_size,linger_ms,"
                        + "warmup_records,compression,records,bytes,elapsed_s,records_per_s,bytes_per_s,samples,"
                        + "p50_us,p99_us,p999_us,max_us");
        assertThat(lines.get(1)).doesNotContain("timestamp");
    }

    @Test
    void p99RefusedBelowHundredSamples() throws IOException {
        var cells = emitAndReadCells(PerfReport.row("producer")
                .latencyKind("per_rpc")
                .records(99)
                .bytes(99)
                .elapsedNanos(1_000_000_000L)
                .histogram(histogramWith(99)));
        assertThat(cell(cells, "samples")).isEqualTo("99");
        assertThat(cell(cells, "p50_us")).isNotEmpty();
        assertThat(cell(cells, "p99_us")).isEmpty();
        assertThat(cell(cells, "p999_us")).isEmpty();
        assertThat(cell(cells, "max_us")).isNotEmpty();
    }

    @Test
    void p99EmittedAtExactlyHundredSamples() throws IOException {
        var cells = emitAndReadCells(PerfReport.row("producer")
                .latencyKind("per_rpc")
                .records(100)
                .bytes(100)
                .elapsedNanos(1_000_000_000L)
                .histogram(histogramWith(100)));
        assertThat(cell(cells, "samples")).isEqualTo("100");
        assertThat(cell(cells, "p99_us")).isNotEmpty();
        assertThat(cell(cells, "p999_us")).isEmpty();
    }

    @Test
    void p999RefusedBelowThousandSamples() throws IOException {
        var cells = emitAndReadCells(PerfReport.row("consumer")
                .latencyKind("per_fetch_rpc")
                .records(999)
                .bytes(999)
                .elapsedNanos(1_000_000_000L)
                .histogram(histogramWith(999)));
        assertThat(cell(cells, "samples")).isEqualTo("999");
        assertThat(cell(cells, "p99_us")).isNotEmpty();
        assertThat(cell(cells, "p999_us")).isEmpty();
    }

    @Test
    void p999EmittedAtExactlyThousandSamples() throws IOException {
        var cells = emitAndReadCells(PerfReport.row("consumer")
                .latencyKind("per_fetch_rpc")
                .records(1000)
                .bytes(1000)
                .elapsedNanos(1_000_000_000L)
                .histogram(histogramWith(1000)));
        assertThat(cell(cells, "samples")).isEqualTo("1000");
        assertThat(cell(cells, "p99_us")).isNotEmpty();
        assertThat(cell(cells, "p999_us")).isNotEmpty();
    }

    @Test
    void inapplicableFieldsAreEmptyNotZero() throws IOException {
        var cells = emitAndReadCells(PerfReport.row("producer")
                .latencyKind("per_rpc")
                .acks("1")
                .payloadSize(1024)
                .records(2000)
                .bytes(2_048_000)
                .elapsedNanos(2_000_000_000L)
                .histogram(histogramWith(2000)));
        assertThat(cell(cells, "linger_ms")).isEmpty();
        assertThat(cell(cells, "compression")).isEmpty();
        assertThat(cell(cells, "batch_size")).isEmpty();
        assertThat(cell(cells, "replication_factor")).isEmpty();
        assertThat(cell(cells, "min_insync_replicas")).isEmpty();
        assertThat(cell(cells, "acks")).isEqualTo("1");
        assertThat(cell(cells, "payload_size")).isEqualTo("1024");
    }

    @Test
    void rowWithoutHistogramHasZeroSamplesAndEmptyLatencies() throws IOException {
        var cells = emitAndReadCells(PerfReport.row("replication")
                .partitions(1)
                .replicationFactor(3)
                .payloadSize(512)
                .records(50_000)
                .bytes(25_600_000)
                .elapsedNanos(3_000_000_000L));
        assertThat(cell(cells, "samples")).isEqualTo("0");
        assertThat(cell(cells, "latency_kind")).isEmpty();
        assertThat(cell(cells, "p50_us")).isEmpty();
        assertThat(cell(cells, "p99_us")).isEmpty();
        assertThat(cell(cells, "p999_us")).isEmpty();
        assertThat(cell(cells, "max_us")).isEmpty();
        assertThat(cell(cells, "records_per_s")).isNotEmpty();
    }

    @Test
    void environmentAndTimestampStampedOnEveryRow() throws IOException {
        var cells = emitAndReadCells(PerfReport.row("producer")
                .records(1)
                .bytes(1)
                .elapsedNanos(1_000_000)
                .histogram(histogramWith(1)));
        assertThat(cell(cells, "timestamp")).isEqualTo("2026-01-02T03:04:05Z");
        assertThat(cell(cells, "git_sha")).isEqualTo("abc1234");
        assertThat(cell(cells, "hostname")).isEqualTo("test-host");
        assertThat(cell(cells, "os")).isEqualTo("test-os");
        assertThat(cell(cells, "cpu_model")).isEqualTo("test-cpu");
        assertThat(cell(cells, "jdk_version")).isEqualTo("21.0.1");
    }
}
