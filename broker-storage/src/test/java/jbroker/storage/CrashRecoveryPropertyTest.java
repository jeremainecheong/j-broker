package jbroker.storage;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.assertj.core.api.Assertions;

/**
 * Property test for crash recovery: append a random number of batches, then
 * simulate a torn write by truncating/corrupting a random byte range in the
 * active {@code .log} file. Reopening must always yield a consistent log —
 * either the full set of batches is readable, or a strict prefix is.
 */
class CrashRecoveryPropertyTest {

    private Path dir;

    @BeforeProperty
    void setUp() throws Exception {
        dir = Files.createTempDirectory("jbroker-crash-prop");
    }

    @AfterProperty
    void tearDown() throws Exception {
        // Best-effort cleanup.
        try (var stream = Files.walk(dir)) {
            stream.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                    /* best-effort */
                }
            });
        }
    }

    @Property(tries = 100)
    void tornTailIsRecoverableAsPrefix(
            @ForAll @IntRange(min = 1, max = 30) int numBatches, @ForAll @IntRange(min = 1, max = 64) int torture)
            throws Exception {
        // Fresh subdirectory per property instance.
        var myDir = Files.createTempDirectory(dir, "run-");
        var config = new Log.Config(1024 * 1024, 0, 4096);
        var written = new ArrayList<byte[]>();
        try (var log = Log.open(myDir, config)) {
            for (int i = 0; i < numBatches; i++) {
                byte[] payload = new byte[(i * 7) % 50 + 1];
                for (int j = 0; j < payload.length; j++) payload[j] = (byte) ((i * 31 + j) & 0xFF);
                written.add(payload);
                log.append(List.of(new Record(0, 0L, null, payload)), 1_000L + i);
            }
            log.force();
        }

        // Append torture bytes to the active .log to simulate torn tail.
        Path active;
        try (var stream = Files.list(myDir)) {
            active = stream.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .sorted((a, b) ->
                            b.getFileName().toString().compareTo(a.getFileName().toString()))
                    .findFirst()
                    .orElseThrow();
        }
        try (var ch = FileChannel.open(active, StandardOpenOption.APPEND)) {
            var junk = ByteBuffer.allocate(torture);
            for (int i = 0; i < torture; i++) junk.put((byte) (i * 0x9e));
            junk.flip();
            ch.write(junk);
        }

        // Reopen and verify we see a prefix of `written`.
        try (var reopened = Log.open(myDir, config)) {
            var batches = reopened.read(0L, 16 * 1024 * 1024);
            var recovered = new ArrayList<byte[]>();
            for (var b : batches) {
                for (var rec : b.records()) recovered.add(rec.value());
            }
            // recovered must equal a prefix of written
            Assertions.assertThat(recovered.size()).isLessThanOrEqualTo(written.size());
            for (int i = 0; i < recovered.size(); i++) {
                Assertions.assertThat(recovered.get(i)).containsExactly(written.get(i));
            }
        }
    }
}
