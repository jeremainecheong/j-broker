package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.auth.AuthMode;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.tls.TlsConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Acceptance: the allow/deny grid enforced end to end over mTLS.
 * One broker, {@code auth.mode=mtls}, {@code super.users=admin,broker1}.
 * The admin principal seeds topics and ACLs; alice holds produce+consume
 * on {@code orders} only; bob holds nothing. Every check asserts on the
 * {@code UNAUTHORIZED} refusal from the handler, not a transport error —
 * all three principals authenticate successfully.
 *
 * <p>Skipped when {@code openssl} is unavailable on PATH.
 */
class AclMatrixIT {

    private static Path tlsDir;

    @BeforeAll
    static void bootstrapCerts() throws Exception {
        Assumptions.assumeTrue(isOpensslOnPath(), "openssl not on PATH — skipping mTLS IT");

        Path projectRoot = Paths.get(System.getProperty("user.dir")).getParent();
        Path script = projectRoot.resolve("scripts/tls/bootstrap-ca.sh");
        Assumptions.assumeTrue(Files.isExecutable(script), "bootstrap-ca.sh not found or not executable: " + script);

        tlsDir = Files.createTempDirectory("jbroker-aclmatrix-it-");
        var pb = new ProcessBuilder(script.toString(), tlsDir.toString()).redirectErrorStream(true);
        Process p = pb.start();
        byte[] out = p.getInputStream().readAllBytes();
        int rc = p.waitFor();
        Assumptions.assumeTrue(rc == 0, "bootstrap-ca.sh failed rc=" + rc + ", output=\n" + new String(out));
    }

    @Test
    void aclGridIsEnforcedPerPrincipalAndOperation(@TempDir Path dataDir) throws Exception {
        var serverTls = TlsConfig.mtlsServer(
                tlsDir.resolve("broker1.crt"), tlsDir.resolve("broker1.key"), tlsDir.resolve("ca.crt"));

        try (var node = TestBrokers.start((rp, bp) -> {
            var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", rp, bp));
            return new Broker.Config(new NodeId(1), dataDir, rp, bp, voters, 1)
                    .withTls(serverTls)
                    .withAuthMode(AuthMode.MTLS)
                    .withSuperUsers(Set.of("admin", "broker1"));
        })) {
            int brokerPort = node.brokerPort();
            awaitBrokerReady(node.broker());

            // --- Seed as the super-user: topics + alice's grants.
            try (var admin = client("admin", brokerPort)) {
                admin.createTopic("orders", 1, 1);
                admin.createTopic("payments", 1, 1);
                admin.createAcl("alice", "topic", "orders", false, "produce", true);
                admin.createAcl("alice", "topic", "orders", false, "consume", true);
                admin.createAcl("alice", "group", "g1", false, "consume", true);
                assertThat(admin.listAcls()).hasSize(3);
            }

            try (var alice = client("alice", brokerPort)) {
                // Granted: produce + consume on orders, group g1.
                alice.produce("orders", 0, "a1".getBytes());
                assertThat(alice.fetchAll("orders", 0, 1 << 20)).hasSize(1);

                // Not granted: the payments topic, any admin surface.
                assertUnauthorized(() -> alice.produce("payments", 0, "a2".getBytes()));
                assertUnauthorized(() -> alice.fetchAll("payments", 0, 1 << 20));
                assertUnauthorized(() -> alice.createTopic("alice-topic", 1, 1));
                assertUnauthorized(() -> alice.deleteTopic("orders"));
                assertUnauthorized(() -> alice.createAcl("alice", "topic", "*", false, "*", true));
                assertUnauthorized(alice::listAcls);
            }

            try (var bob = client("bob", brokerPort)) {
                // No grants at all: default-deny on every path.
                assertUnauthorized(() -> bob.produce("orders", 0, "b1".getBytes()));
                assertUnauthorized(() -> bob.fetchAll("orders", 0, 1 << 20));
                assertUnauthorized(() -> bob.deleteTopic("orders"));
            }

            // Revocation propagates: alice loses produce, keeps consume.
            try (var admin = client("admin", brokerPort)) {
                admin.deleteAcl("alice", "topic", "orders", false, "produce");
            }
            try (var alice = client("alice", brokerPort)) {
                assertUnauthorized(() -> alice.produce("orders", 0, "a3".getBytes()));
                assertThat(alice.fetchAll("orders", 0, 1 << 20)).hasSize(1);
            }
        }
    }

    private static void assertUnauthorized(ThrowingCallable rpc) {
        assertThatThrownBy(rpc::call).hasMessageContaining("not authorized");
    }

    @FunctionalInterface
    private interface ThrowingCallable {
        void call() throws Exception;
    }

    private static BrokerClient client(String principal, int brokerPort) {
        var tls = TlsConfig.mtlsClient(
                tlsDir.resolve(principal + ".crt"), tlsDir.resolve(principal + ".key"), tlsDir.resolve("ca.crt"));
        return new BrokerClient("localhost", brokerPort, tls);
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
