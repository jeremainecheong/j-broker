package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.auth.AuthMode;
import jbroker.broker.client.BrokerClient;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.raft.core.NodeId;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R3.1 acceptance: with {@code auth.mode=mtls}, a client without a
 * client certificate is rejected with {@code UNAUTHENTICATED} on every
 * service — produce, fetch, admin — while a certificate-bearing client
 * works end to end.
 *
 * <p>The broker's TLS listener deliberately runs with client-auth
 * <em>optional</em>: certless clients complete the TLS handshake, so the
 * rejection observed here is the auth interceptor's per-RPC gate, not a
 * handshake failure (that path is covered by {@link MtlsClusterIT}).
 *
 * <p>Skipped when {@code openssl} is unavailable on PATH.
 */
class AnonymousRejectedIT {

    private static Path tlsDir;

    @BeforeAll
    static void bootstrapCerts() throws Exception {
        Assumptions.assumeTrue(isOpensslOnPath(), "openssl not on PATH — skipping mTLS IT");

        Path projectRoot = Paths.get(System.getProperty("user.dir")).getParent();
        Path script = projectRoot.resolve("scripts/tls/bootstrap-ca.sh");
        Assumptions.assumeTrue(Files.isExecutable(script), "bootstrap-ca.sh not found or not executable: " + script);

        tlsDir = Files.createTempDirectory("jbroker-anon-it-");
        var pb = new ProcessBuilder(script.toString(), tlsDir.toString()).redirectErrorStream(true);
        Process p = pb.start();
        byte[] out = p.getInputStream().readAllBytes();
        int rc = p.waitFor();
        Assumptions.assumeTrue(rc == 0, "bootstrap-ca.sh failed rc=" + rc + ", output=\n" + new String(out));
    }

    @Test
    void certlessClientsAreRejectedOnEveryServiceWhileCertifiedClientsWork(@TempDir Path dataDir) throws Exception {
        // Client-auth optional: the handshake admits certless peers so the
        // per-RPC UNAUTHENTICATED gate is what this test observes.
        var serverTls = new TlsConfig(
                true, tlsDir.resolve("broker1.crt"), tlsDir.resolve("broker1.key"), tlsDir.resolve("ca.crt"), false);

        try (var node = TestBrokers.start((rp, bp) -> {
            var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", rp, bp));
            return new Broker.Config(new NodeId(1), dataDir, rp, bp, voters, 1)
                    .withTls(serverTls)
                    .withAuthMode(AuthMode.MTLS);
        })) {
            var broker = node.broker();
            int brokerPort = node.brokerPort();
            awaitBrokerReady(broker);

            // --- Certified client (CN=admin): full round trip works.
            var adminTls = TlsConfig.mtlsClient(
                    tlsDir.resolve("admin.crt"), tlsDir.resolve("admin.key"), tlsDir.resolve("ca.crt"));
            try (var client = new BrokerClient("localhost", brokerPort, adminTls)) {
                client.createTopic("secured", 1, 1);
                client.produce("secured", 0, "hello".getBytes());
                assertThat(client.fetchAll("secured", 0, 1 << 20)).hasSize(1);
            }

            // --- Certless TLS client: every service rejects before any
            // handler runs.
            var anonTls = new TlsConfig(true, null, null, tlsDir.resolve("ca.crt"), false);
            ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", brokerPort)
                    .sslContext(TlsContexts.clientContext(anonTls))
                    .build();
            try {
                var producer = ProducerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                assertUnauthenticated(() -> producer.produce(ProduceRequest.newBuilder()
                        .setTopic("secured")
                        .setPartition(0)
                        .build()));

                var consumer = ConsumerGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                assertUnauthenticated(() -> consumer.fetch(FetchRequest.newBuilder()
                        .setTopic("secured")
                        .setPartition(0)
                        .build()));

                var admin = AdminGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                assertUnauthenticated(
                        () -> admin.listTopics(ListTopicsRequest.newBuilder().build()));
            } finally {
                channel.shutdownNow();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

    private static void assertUnauthenticated(Runnable rpc) {
        assertThatThrownBy(rpc::run).isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(
                        e.getStatus().getCode())
                .isEqualTo(Status.Code.UNAUTHENTICATED));
    }

    private static void awaitBrokerReady(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.brokerRegistry().addressFor(1).isPresent()) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker 1 did not register within 10s");
    }

    private static boolean isOpensslOnPath() {
        try {
            Process p = new ProcessBuilder("openssl", "version")
                    .redirectErrorStream(true)
                    .start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
