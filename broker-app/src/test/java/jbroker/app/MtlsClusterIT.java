package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.broker.client.BrokerClient;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.raft.core.NodeId;
import jbroker.tls.TlsConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P15.2 — mTLS cluster integration test. Generates a dev CA + broker server
 * cert + admin client cert via {@code scripts/tls/bootstrap-ca.sh}, stands up
 * a single-broker cluster with TLS enabled + client-auth=REQUIRE, and asserts
 * two invariants:
 *
 * <ul>
 *   <li>A client presenting a valid cert chain completes the TLS handshake
 *       and successfully issues an Admin RPC.
 *   <li>A plaintext client against the same broker fails — the TLS server
 *       closes the connection before any gRPC frames travel, which surfaces
 *       as UNAVAILABLE / INTERNAL to the client.
 * </ul>
 *
 * <p>Skipped when {@code openssl} is unavailable on PATH — CI environments
 * that don't provide openssl fall back to the plaintext-only test matrix.
 */
class MtlsClusterIT {

    private static Path tlsDir;

    @BeforeAll
    static void bootstrapCerts() throws Exception {
        Assumptions.assumeTrue(isOpensslOnPath(), "openssl not on PATH — skipping mTLS IT");

        // broker-app module's cwd when gradle runs tests is the module dir;
        // the bootstrap script lives two levels up.
        Path projectRoot = Paths.get(System.getProperty("user.dir")).getParent();
        Path script = projectRoot.resolve("scripts/tls/bootstrap-ca.sh");
        Assumptions.assumeTrue(Files.isExecutable(script), "bootstrap-ca.sh not found or not executable: " + script);

        tlsDir = Files.createTempDirectory("jbroker-mtls-it-");
        var pb = new ProcessBuilder(script.toString(), tlsDir.toString()).redirectErrorStream(true);
        Process p = pb.start();
        byte[] out = p.getInputStream().readAllBytes();
        int rc = p.waitFor();
        Assumptions.assumeTrue(rc == 0, "bootstrap-ca.sh failed rc=" + rc + ", output=\n" + new String(out));
    }

    @Test
    void tlsClientReachesTlsBroker(@TempDir Path dataDir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();

        var serverTls = TlsConfig.mtlsServer(
                tlsDir.resolve("broker1.crt"), tlsDir.resolve("broker1.key"), tlsDir.resolve("ca.crt"));

        var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));
        var cfg = new Broker.Config(new NodeId(1), dataDir, raftPort, brokerPort, voters, 1).withTls(serverTls);

        try (var broker = Broker.start(cfg)) {
            awaitBrokerReady(broker);

            // --- TLS client with valid client cert: handshake + Admin RPC succeed.
            var clientTls = TlsConfig.mtlsClient(
                    tlsDir.resolve("admin.crt"), tlsDir.resolve("admin.key"), tlsDir.resolve("ca.crt"));
            try (var client = new BrokerClient("localhost", brokerPort, clientTls)) {
                client.createTopic("mtls-topic", 1, 1);
                var topics = client.listTopics();
                assertThat(topics).anySatisfy(t -> assertThat(t.getTopic()).isEqualTo("mtls-topic"));
            }
        }
    }

    @Test
    void plaintextClientIsRejectedByTlsBroker(@TempDir Path dataDir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();

        var serverTls = TlsConfig.mtlsServer(
                tlsDir.resolve("broker1.crt"), tlsDir.resolve("broker1.key"), tlsDir.resolve("ca.crt"));
        var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));
        var cfg = new Broker.Config(new NodeId(1), dataDir, raftPort, brokerPort, voters, 1).withTls(serverTls);

        try (var broker = Broker.start(cfg)) {
            awaitBrokerReady(broker);

            // Plaintext channel against a TLS server — the TLS handshake
            // never completes, so any RPC surfaces a gRPC status failure.
            var channel = NettyChannelBuilder.forAddress("localhost", brokerPort)
                    .usePlaintext()
                    .build();
            try {
                var stub = AdminGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
                assertThatThrownBy(() ->
                                stub.listTopics(ListTopicsRequest.newBuilder().build()))
                        .isInstanceOf(StatusRuntimeException.class);
            } finally {
                channel.shutdownNow();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
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

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
