package jbroker.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Offline integrity checker for a broker's partition logs (the read-only
 * half of the recovery scan): walk every {@code <topic>-<partition>}
 * directory under a topics dir, decode every batch — CRC-32C re-verified
 * by {@link RecordBatch#decode} — and report what each partition holds
 * and where (if anywhere) it stops being valid. Never writes; a live
 * broker's recovery scan is what truncates, this tool only tells an
 * operator whether a backup is worth restoring.
 */
public final class LogVerifier {

    /** One partition directory's verdict. {@code problem} is null when every byte decodes. */
    public record PartitionReport(
            String partitionDir, int segments, long batches, long records, long nextOffset, String problem) {}

    private LogVerifier() {}

    /** Verify every partition directory under {@code topicsDir}, sorted by name. */
    public static List<PartitionReport> verify(Path topicsDir) throws IOException {
        if (!Files.isDirectory(topicsDir)) {
            throw new IOException("not a directory: " + topicsDir);
        }
        var partitionDirs = new ArrayList<Path>();
        try (var stream = Files.list(topicsDir)) {
            stream.filter(Files::isDirectory).forEach(partitionDirs::add);
        }
        partitionDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));
        var out = new ArrayList<PartitionReport>(partitionDirs.size());
        for (var dir : partitionDirs) {
            out.add(verifyPartition(dir));
        }
        return out;
    }

    private static PartitionReport verifyPartition(Path dir) throws IOException {
        var logFiles = new ArrayList<Path>();
        try (var stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".log")).forEach(logFiles::add);
        }
        logFiles.sort(Comparator.comparing(p -> p.getFileName().toString()));
        int segments = 0;
        long batches = 0;
        long records = 0;
        long nextOffset = 0;
        String problem = null;
        for (var file : logFiles) {
            segments++;
            try (var ch = FileChannel.open(file, StandardOpenOption.READ)) {
                long size = ch.size();
                long pos = 0;
                while (pos < size) {
                    int toMap = (int) Math.min(size - pos, Integer.MAX_VALUE);
                    var mapped = ch.map(FileChannel.MapMode.READ_ONLY, pos, toMap);
                    while (mapped.remaining() >= RecordBatch.BATCH_OVERHEAD) {
                        int mark = mapped.position();
                        try {
                            var parsed = RecordBatch.decode(mapped);
                            batches++;
                            records += parsed.records().size();
                            nextOffset = parsed.lastOffset() + 1;
                        } catch (IllegalArgumentException e) {
                            problem = file.getFileName() + " position " + (pos + mark) + ": " + e.getMessage();
                            break;
                        }
                    }
                    if (problem != null) break;
                    pos += mapped.position();
                    if (mapped.hasRemaining()) {
                        // Fewer bytes than a batch header at the tail.
                        problem = file.getFileName() + " position " + pos + ": partial trailing frame (" + (size - pos)
                                + " bytes)";
                        break;
                    }
                }
            }
            if (problem != null) break;
        }
        return new PartitionReport(dir.getFileName().toString(), segments, batches, records, nextOffset, problem);
    }

    /** True when every partition in {@code reports} decoded cleanly. */
    public static boolean allClean(List<PartitionReport> reports) {
        return reports.stream().allMatch(r -> r.problem() == null);
    }
}
