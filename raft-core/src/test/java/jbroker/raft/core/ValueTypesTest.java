package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ValueTypesTest {

    @Test
    void nodeIdRejectsNegative() {
        assertThatThrownBy(() -> new NodeId(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nodeIdAcceptsZero() {
        assertThat(new NodeId(0).value()).isZero();
    }

    @Test
    void termZeroIsValid() {
        assertThat(new Term(0).value()).isZero();
    }

    @Test
    void termRejectsNegative() {
        assertThatThrownBy(() -> new Term(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void termNextReturnsIncrementedTerm() {
        assertThat(new Term(5).next()).isEqualTo(new Term(6));
    }

    @Test
    void roleEnumHasExpectedValues() {
        assertThat(Role.values()).containsExactly(Role.FOLLOWER, Role.CANDIDATE, Role.LEADER);
    }
}
