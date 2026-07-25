package jbroker.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * On-disk format marker for a broker's data directory. A single
 * {@code format.version} file at the {@link LogManager} root records the
 * format the directory was written in, so a binary that is too old to
 * understand the layout refuses to start instead of guessing — a rolling
 * downgrade onto newer data must fail loudly, not corrupt silently.
 *
 * <p>A directory-level marker, deliberately not a per-segment header:
 * prepending bytes to segment files would shift every offset→position
 * mapping (sparse index, {@code transferTo}) for no gain.
 *
 * <p>Semantics on open:
 *
 * <ul>
 *   <li>No marker — a fresh directory, or one written before markers
 *       existed. Stamp it with {@link #CURRENT} and proceed; the layout is
 *       unchanged, only the marker is new.
 *   <li>Marker equal to {@link #CURRENT} — proceed.
 *   <li>Marker below {@link #CURRENT} — proceed. Upward migrations are the
 *       newer binary's job; the migration step re-stamps only after the
 *       layout is actually upgraded, so the marker is left untouched here
 *       and a crash mid-migration still rolls back cleanly.
 *   <li>Marker above {@link #CURRENT} — refuse: the directory was written
 *       by a newer broker whose format this binary cannot decode.
 *   <li>Unreadable marker (empty, non-numeric, non-positive) — refuse. The
 *       file is operator data; never silently overwrite it.
 * </ul>
 *
 * <p>The stamp is crash-safe: written to a temp sibling with {@code SYNC},
 * then atomically moved into place. A crash mid-stamp leaves either no
 * marker (a stale temp file is removed on the next open) or the complete
 * one — the visible marker only ever holds a whole value.
 */
public final class FormatVersion {

    /**
     * The newest on-disk format this binary reads and writes.
     *
     * <p>Format 1 is the v2 record-batch layout, including batches whose
     * records section is zstd-compressed per the attributes codec bits —
     * compression shipped in the same release as this marker, so no
     * binary exists that understands the marker but not the codec.
     *
     * <p>Format 2 ({@link #TRANSACTIONS}) adds transaction control batches
     * (attributes bit 3, a marker record instead of application data) and
     * the transactional data bit (bit 4). A format-1 binary would hand a
     * control batch's marker record to applications as payload, so a
     * directory holding control batches must refuse a downgraded binary. A
     * fresh directory stamps 2 at open (as with format 1, the marker
     * records what this binary may write); a directory already marked 1
     * keeps its marker — and its downgrade freedom — until the first
     * control batch actually lands, at which point the write path
     * re-stamps via {@link #ensureAtLeast} before the marker bytes hit
     * disk. A future layout change that older readers would misread must
     * bump this.
     */
    public static final int CURRENT = 2;

    /** Format 2: transaction control batches + the transactional attribute bit. */
    public static final int TRANSACTIONS = 2;

    /** Marker file name at the data-dir root. */
    public static final String FILE_NAME = "format.version";

    private FormatVersion() {}

    /**
     * Validate {@code rootDir}'s marker against {@link #CURRENT}, stamping
     * an unmarked directory. Returns the version the marker records after
     * the call.
     *
     * @throws IOException if the marker names a newer format than this
     *     binary understands, or cannot be parsed
     */
    public static int check(Path rootDir) throws IOException {
        return check(rootDir, CURRENT);
    }

    /** Visible for tests: {@link #check(Path)} against an explicit current version. */
    static int check(Path rootDir, int current) throws IOException {
        var marker = rootDir.resolve(FILE_NAME);
        // A crash between the temp write and the atomic move can leave the
        // temp file behind; only the marker's presence decides, so drop it.
        Files.deleteIfExists(tmpFor(marker));
        if (!Files.exists(marker)) {
            stamp(marker, current);
            return current;
        }
        var content = Files.readString(marker).trim();
        int version;
        try {
            version = Integer.parseInt(content);
        } catch (NumberFormatException e) {
            throw new IOException("unreadable format marker " + marker + ": expected an integer, found \"" + content
                    + "\" — refusing to guess; inspect the file or restore from backup");
        }
        if (version < 1) {
            throw new IOException("unreadable format marker " + marker + ": no broker ever wrote version " + version
                    + " — refusing to guess; inspect the file or restore from backup");
        }
        if (version > current) {
            throw new IOException("data directory " + rootDir + " uses on-disk format " + version
                    + " but this broker only understands format " + current
                    + "; it was written by a newer broker — upgrade the binary or restore from backup");
        }
        return version;
    }

    /**
     * The migration re-stamp: raise {@code rootDir}'s marker to
     * {@code version} at the moment the directory's contents actually
     * become that format (the first control batch, for
     * {@link #TRANSACTIONS}). Runs {@link #check(Path)} first, so an
     * unmarked directory is stamped and a marker this binary cannot read
     * or exceed still refuses. Returns the marker's version after the
     * call. Callers must invoke this BEFORE writing the format-changing
     * bytes: a crash in between leaves an over-claiming marker (safe —
     * downgrade refuses unnecessarily), never an under-claiming one
     * (unsafe — downgrade would misread).
     */
    public static int ensureAtLeast(Path rootDir, int version) throws IOException {
        if (version > CURRENT) {
            throw new IllegalArgumentException(
                    "cannot stamp format " + version + ": this binary only writes up to " + CURRENT);
        }
        int existing = check(rootDir);
        if (existing >= version) return existing;
        stamp(rootDir.resolve(FILE_NAME), version);
        return version;
    }

    private static void stamp(Path marker, int version) throws IOException {
        var tmp = tmpFor(marker);
        Files.writeString(
                tmp,
                version + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC);
        Files.move(tmp, marker, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private static Path tmpFor(Path marker) {
        return marker.resolveSibling(marker.getFileName() + ".tmp");
    }
}
