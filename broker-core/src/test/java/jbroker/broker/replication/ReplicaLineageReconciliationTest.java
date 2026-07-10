package jbroker.broker.replication;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import jbroker.broker.OffsetsForLeaderEpochHandler;
import jbroker.broker.ReplicaFetchHandler;
import jbroker.broker.TopicManager;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;
import jbroker.storage.LogManager;
import jbroker.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end replay of the soak-v5 divergence: a follower whose tail was
 * written under a rejected leadership must truncate it and converge onto
 * the leader's lineage — not splice the leader's records on top of junk.
 *
 * <p>Wires a real {@link ReplicaFetcher} (follower log) against real
 * {@link ReplicaFetchHandler} + {@link OffsetsForLeaderEpochHandler}
 * (leader log) in-process, no cluster. Before lineage validation this
 * exact sequence "succeeded": the follower fetched from its LEO (4), the
 * leader served offset-4 records, and offsets 1–3 stayed divergent
 * forever while the follower reported a healthy LEO.
 */
class ReplicaLineageReconciliationTest {

    private static final int LEADER = 1;
    private static final int FOLLOWER = 2;

    @Test
    void divergedFollowerTruncatesAndConvergesOntoLeaderLineage(@TempDir Path leaderDir, @TempDir Path followerDir)
            throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        // Current metadata: leader epoch 2 — BOTH sides agree, which is
        // exactly why the old epoch check could not catch this.
        tm.onPartitionChange("t", 0, LEADER, List.of(LEADER, FOLLOWER), List.of(LEADER, FOLLOWER), 2, 0);

        try (var leaderLm = lm(leaderDir);
                var followerLm = lm(followerDir)) {
            // Leader (true lineage): [shared@e1, real-1@e2, real-2@e2]
            var leaderLog = leaderLm.logFor("t", 0);
            leaderLog.append(records("shared"), 1_000L, -1L, (short) -1, -1, 1);
            leaderLog.append(records("real-1"), 1_000L, -1L, (short) -1, -1, 2);
            leaderLog.append(records("real-2"), 1_000L, -1L, (short) -1, -1, 2);
            leaderLm.leaderEpochCheckpoint("t", 0).assign(1, 0L);
            leaderLm.leaderEpochCheckpoint("t", 0).assign(2, 1L);

            // Follower (junk tail): [shared@e1, junk@e1, junk@e1, junk@e1]
            // — the soak's repeated speculative appends under epoch 1.
            var followerLog = followerLm.logFor("t", 0);
            followerLog.append(records("shared"), 1_000L, -1L, (short) -1, -1, 1);
            followerLog.append(records("junk"), 1_000L, -1L, (short) -1, -1, 1);
            followerLog.append(records("junk"), 1_000L, -1L, (short) -1, -1, 1);
            followerLog.append(records("junk"), 1_000L, -1L, (short) -1, -1, 1);
            followerLm.leaderEpochCheckpoint("t", 0).assign(1, 0L);

            // Leader-side handlers, called directly (no gRPC).
            var fetchHandler =
                    new ReplicaFetchHandler(leaderLm, tm, LEADER, new FollowerStateTracker(), () -> 1_700_000_000_000L);
            var offsetsHandler = new OffsetsForLeaderEpochHandler(leaderLm, tm, LEADER);
            ReplicaFetcher.Peer peer = new ReplicaFetcher.Peer() {
                @Override
                public ReplicaFetchResponse fetch(ReplicaFetchRequest req) {
                    return fetchHandler.handle(req);
                }

                @Override
                public long offsetsForLeaderEpoch(String topic, int partition, int leaderEpoch) {
                    return offsetsHandler
                            .handle(jbroker.proto.broker.OffsetsForLeaderEpochRequest.newBuilder()
                                    .setTopic(topic)
                                    .setPartition(partition)
                                    .setLeaderEpoch(leaderEpoch)
                                    .build())
                            .getEndOffset();
                }
            };
            var fetcher = new ReplicaFetcher(followerLm, "t", 0, FOLLOWER, peer);

            // Poll 1: fenced on lineage (follower last-batch epoch 1 vs
            // leader lineage epoch 2 at offset 3) → reconciled → junk
            // truncated to the epoch-1 intersection (offset 1).
            assertThat(fetcher.pollOnce(2)).isEqualTo(ReplicaFetcher.PollResult.RECONCILED);
            assertThat(followerLog.nextOffset()).isEqualTo(1L);

            // Poll 2: lineage now matches; the true epoch-2 records land.
            assertThat(fetcher.pollOnce(2)).isEqualTo(ReplicaFetcher.PollResult.ADVANCED);
            assertThat(followerLog.nextOffset()).isEqualTo(3L);

            // Converged: payloads identical, and the follower's checkpoint
            // recorded the epoch-2 lineage from the replicated batch headers.
            assertThat(payloads(followerLm)).containsExactly("shared", "real-1", "real-2");
            assertThat(followerLm.leaderEpochCheckpoint("t", 0).epochFor(2L)).hasValueSatisfying(e -> {
                assertThat(e.epoch()).isEqualTo(2);
                assertThat(e.startOffset()).isEqualTo(1L);
            });

            // Poll 3: steady state.
            assertThat(fetcher.pollOnce(2)).isEqualTo(ReplicaFetcher.PollResult.EMPTY);
        }
    }

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        jbroker.storage.LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        java.util.concurrent.TimeUnit.MINUTES.toMillis(5)));
    }

    private static List<Record> records(String payload) {
        return List.of(new Record(0, 0L, null, payload.getBytes(StandardCharsets.UTF_8)));
    }

    private static List<String> payloads(LogManager lm) throws Exception {
        var out = new java.util.ArrayList<String>();
        for (var b : lm.logFor("t", 0).read(0L, 1 << 20)) {
            for (var r : b.records()) {
                out.add(new String(r.value(), StandardCharsets.UTF_8));
            }
        }
        return out;
    }
}
