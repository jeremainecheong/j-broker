package jbroker.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FilePersistentStateTest {

    @Test
    void defaultsToTermZeroAndNoVote(@TempDir Path dir) throws Exception {
        try (var state = FilePersistentState.open(dir.resolve("state.bin"))) {
            assertThat(state.currentTerm()).isEqualTo(Term.ZERO);
            assertThat(state.votedFor()).isEmpty();
        }
    }

    @Test
    void persistsAcrossReopen(@TempDir Path dir) throws Exception {
        var path = dir.resolve("state.bin");
        try (var state = FilePersistentState.open(path)) {
            state.update(new Term(7), Optional.of(new NodeId(2)));
        }
        try (var reopened = FilePersistentState.open(path)) {
            assertThat(reopened.currentTerm()).isEqualTo(new Term(7));
            assertThat(reopened.votedFor()).contains(new NodeId(2));
        }
    }

    @Test
    void clearsVoteOnTermBump(@TempDir Path dir) throws Exception {
        var path = dir.resolve("state.bin");
        try (var state = FilePersistentState.open(path)) {
            state.update(new Term(3), Optional.of(new NodeId(1)));
            state.update(new Term(4), Optional.empty());
            assertThat(state.votedFor()).isEmpty();
        }
    }
}
