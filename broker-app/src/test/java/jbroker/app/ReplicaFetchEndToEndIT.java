package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.replication.ReplicaFetcher;
import jbroker.broker.replication.ReplicaPeerClient;
import jbroker.raft.core.NodeId;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the data-plane goal: a follower pulling batches from a
 * leader via ReplicaFetch. Uses one real {@link Broker} (playing the
 * leader) and a second bare {@link LogManager} + {@link ReplicaFetcher}
 * (playing the follower). The full 3-broker metadata-Raft cluster is
 * covered elsewhere — this IT scopes strictly to the replication RPC.
 */
class ReplicaFetchEndToEndIT {

    @Test
    void followerPullsAllProducedRecordsFromLeader(@TempDir Path leaderDir, @TempDir Path followerDir)
            throws Exception {
        int leaderId = 1;
        var node = TestBrokers.start((rp, bp) -> new Broker.Config(new NodeId(leaderId), leaderDir, rp, bp));
        var leader = node.broker();
        int brokerPort = node.brokerPort();

        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            client.createTopic("replicated", 1, 3);

            int n = 200;
            for (int i = 0; i < n; i++) {
                client.produce("replicated", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }

            try (var followerLm = new LogManager(
                            followerDir,
                            new LogManager.Config(
                                    128L * 1024 * 1024,
                                    Long.MAX_VALUE,
                                    LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                                    TimeUnit.MINUTES.toMillis(5)));
                    var peer = new ReplicaPeerClient("127.0.0.1", brokerPort)) {

                // Use broker-id 1 so the ISR check on the leader accepts us —
                // in single-broker mode the only member of the ISR is broker 1.
                // This IT exercises the replication RPC end-to-end; the full
                // multi-broker ISR story is covered elsewhere, where a real
                // controller proposes PartitionChangeRecords adding follower
                // broker ids.
                var fetcher = new ReplicaFetcher(
                        followerLm,
                        "replicated",
                        0, /* followerBrokerId */
                        1,
                        new ReplicaFetcher.Peer() {
                            @Override
                            public jbroker.proto.broker.ReplicaFetchResponse fetch(
                                    jbroker.proto.broker.ReplicaFetchRequest req) {
                                return peer.fetch(req, 5_000L);
                            }

                            @Override
                            public long offsetsForLeaderEpoch(String topic, int partition, int leaderEpoch) {
                                return peer.offsetsForLeaderEpoch(topic, partition, leaderEpoch, 5_000L);
                            }
                        });

                long deadline = System.currentTimeMillis() + 30_000;
                while (System.currentTimeMillis() < deadline
                        && followerLm.logFor("replicated", 0).nextOffset() < n) {
                    fetcher.pollOnce(/* expectedEpoch */ 0);
                    Thread.sleep(10);
                }

                var followerLog = followerLm.logFor("replicated", 0);
                assertThat(followerLog.nextOffset()).isEqualTo(n);
                assertThat(fetcher.highWatermark()).isEqualTo(n);
            }
        } finally {
            leader.close();
        }
    }
}
