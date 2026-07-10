package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LeaderEpochCheckpointTest {

    @Test
    void assignPersistsEntriesAndSurvivesReopen(@TempDir Path dir) throws Exception {
        var path = dir.resolve("leader-epoch-checkpoint");
        var cp = LeaderEpochCheckpoint.open(path);
        cp.assign(1, 0L);
        cp.assign(2, 100L);
        cp.assign(5, 500L);
        assertThat(cp.entries()).hasSize(3);

        var reopened = LeaderEpochCheckpoint.open(path);
        assertThat(reopened.entries()).hasSize(3);
        assertThat(reopened.epochFor(150L)).hasValueSatisfying(e -> {
            assertThat(e.epoch()).isEqualTo(2);
            assertThat(e.startOffset()).isEqualTo(100L);
        });
        assertThat(reopened.epochFor(0L))
                .hasValueSatisfying(e -> assertThat(e.epoch()).isEqualTo(1));
    }

    @Test
    void truncateFromDropsWipedEntriesAndAllowsCorrectedReassignment(@TempDir Path dir) throws Exception {
        var path = dir.resolve("leader-epoch-checkpoint");
        var cp = LeaderEpochCheckpoint.open(path);
        cp.assign(1, 0L);
        cp.assign(2, 100L);
        cp.assign(5, 500L);

        // Log truncated to 100: the epoch-2 and epoch-5 ranges are gone.
        cp.truncateFrom(100L);
        assertThat(cp.entries()).hasSize(1);
        assertThat(cp.epochFor(150L))
                .hasValueSatisfying(e -> assertThat(e.epoch()).isEqualTo(1));

        // Refetching the range under the corrected lineage must be
        // recordable — the monotonicity guard no longer blocks it.
        cp.assign(3, 100L);
        assertThat(cp.epochFor(150L))
                .hasValueSatisfying(e -> assertThat(e.epoch()).isEqualTo(3));

        // Survives reopen.
        var reopened = LeaderEpochCheckpoint.open(path);
        assertThat(reopened.entries()).hasSize(2);
    }

    @Test
    void nonMonotonicAssignmentsAreIgnored(@TempDir Path dir) throws Exception {
        var cp = LeaderEpochCheckpoint.open(dir.resolve("cp"));
        cp.assign(1, 10L);
        cp.assign(1, 20L); // same epoch — refuse
        cp.assign(0, 30L); // lower epoch — refuse
        assertThat(cp.entries()).hasSize(1);
    }
}
