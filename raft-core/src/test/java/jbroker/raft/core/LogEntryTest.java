package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class LogEntryTest {

    @Test
    void constructsNormalEntry() {
        var entry = new LogEntry(1L, new Term(2), LogEntry.Type.NORMAL, new byte[] {1, 2, 3});
        assertThat(entry.index()).isEqualTo(1L);
        assertThat(entry.term()).isEqualTo(new Term(2));
        assertThat(entry.type()).isEqualTo(LogEntry.Type.NORMAL);
        assertThat(entry.payload()).containsExactly(1, 2, 3);
    }

    @Test
    void rejectsNegativeIndex() {
        assertThatThrownBy(() -> new LogEntry(-1L, Term.ZERO, LogEntry.Type.NORMAL, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullPayload() {
        assertThatThrownBy(() -> new LogEntry(0L, Term.ZERO, LogEntry.Type.NORMAL, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void equalsAndHashCodeUseByteArrayContent() {
        var a = new LogEntry(1L, new Term(2), LogEntry.Type.NORMAL, new byte[] {1, 2, 3});
        var b = new LogEntry(1L, new Term(2), LogEntry.Type.NORMAL, new byte[] {1, 2, 3});
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void differentPayloadsAreNotEqual() {
        var a = new LogEntry(1L, new Term(2), LogEntry.Type.NORMAL, new byte[] {1, 2, 3});
        var b = new LogEntry(1L, new Term(2), LogEntry.Type.NORMAL, new byte[] {9, 9, 9});
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void payloadIsDefensivelyCopiedOnConstruction() {
        var original = new byte[] {1, 2, 3};
        var entry = new LogEntry(1L, new Term(2), LogEntry.Type.NORMAL, original);
        original[0] = 99;
        assertThat(entry.payload()).containsExactly(1, 2, 3);
    }

    @Test
    void payloadAccessorReturnsDefensiveCopy() {
        var entry = new LogEntry(1L, new Term(2), LogEntry.Type.NORMAL, new byte[] {1, 2, 3});
        byte[] first = entry.payload();
        first[0] = 99;
        assertThat(entry.payload()).containsExactly(1, 2, 3);
    }

    @Test
    void toStringShowsPayloadBytes() {
        var entry = new LogEntry(1L, new Term(2), LogEntry.Type.NORMAL, new byte[] {1, 2, 3});
        assertThat(entry.toString()).contains("[1, 2, 3]");
    }
}
