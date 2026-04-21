package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

/**
 * {@link StateMachine#snapshot} / {@link StateMachine#restore} must round-trip
 * the full applied state so a fresh replica can be bootstrapped solely from a
 * snapshot without replaying the log.
 */
class StateMachineSnapshotTest {

    @Test
    void mapStateMachineSnapshotRestoreRoundtrips() throws Exception {
        var sm = new MapStateMachine();
        sm.apply(new LogEntry(1L, new Term(1), LogEntry.Type.NORMAL, MapStateMachine.encode("k1", "v1")));
        sm.apply(new LogEntry(2L, new Term(1), LogEntry.Type.NORMAL, MapStateMachine.encode("k2", "v2")));

        var out = new ByteArrayOutputStream();
        sm.snapshot(out);

        var restored = new MapStateMachine();
        restored.restore(new ByteArrayInputStream(out.toByteArray()));

        assertThat(restored.snapshot()).containsEntry("k1", "v1").containsEntry("k2", "v2");
    }
}
