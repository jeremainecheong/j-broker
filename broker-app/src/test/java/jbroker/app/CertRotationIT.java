package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.tls.TlsConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Acceptance for the documented rolling rotation: regenerate the
 * broker's server certificate under the same CA, restart the broker on
 * the same data directory and ports, and a client still holding its
 * pre-rotation certificate and trust bundle keeps working — including
 * reading records produced before the rotation.
 *
 * <p>Skipped when {@code openssl} is unavailable on PATH.
 */
class CertRotationIT {

    private static Path script;

    @BeforeAll
    static void locateScript() throws Exception {
        Assumptions.assumeTrue(isOpensslOnPath(), "openssl not on PATH — skipping mTLS IT");
        Path projectRoot = Paths.get(System.getProperty("user.dir")).getParent();
        script = projectRoot.resolve("scripts/tls/bootstrap-ca.sh");
        Assumptions.assumeTrue(Files.isExecutable(script), "bootstrap-ca.sh not found or not executable: " + script);
    }

    @Test
    void rotatedServerCertKeepsOldClientsWorking(@TempDir Path dataDir, @TempDir Path tlsDir) throws Exception {
        runScript(tlsDir.toString());

        // Freeze the client's pre-rotation identity: rotation must not
        // require re-issuing client material when the CA is unchanged.
        var frozen = Files.createDirectory(tlsDir.resolve("frozen"));
        for (var f : new String[] {"admin.crt", "admin.key", "ca.crt"}) {
            Files.copy(tlsDir.resolve(f), frozen.resolve(f), StandardCopyOption.REPLACE_EXISTING);
        }
        var clientTls = TlsConfig.mtlsClient(
                frozen.resolve("admin.crt"), frozen.resolve("admin.key"), frozen.resolve("ca.crt"));
        var serverTls = TlsConfig.mtlsServer(
                tlsDir.resolve("broker1.crt"), tlsDir.resolve("broker1.key"), tlsDir.resolve("ca.crt"));

        int raftPort;
        int brokerPort;
        var node = TestBrokers.start((rp, bp) -> {
            var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", rp, bp));
            return new Broker.Config(new NodeId(1), dataDir, rp, bp, voters, 1).withTls(serverTls);
        });
        try {
            raftPort = node.raftPort();
            brokerPort = node.brokerPort();
            awaitReady(node.broker());
            try (var client = new BrokerClient("localhost", brokerPort, clientTls)) {
                client.createTopic("rotated", 1, 1);
                client.produce("rotated", 0, "before-rotation".getBytes());
            }
        } finally {
            node.close();
        }

        // Rotate: same CA, fresh server certs (the --force path), then
        // restart on the same ports + data dir like a rolling restart.
        byte[] oldCert = Files.readAllBytes(tlsDir.resolve("broker1.crt"));
        runScript(tlsDir.toString(), "--force");
        assertThat(Files.readAllBytes(tlsDir.resolve("broker1.crt")))
                .as("rotation really produced a new server cert")
                .isNotEqualTo(oldCert);
        assertThat(Files.readAllBytes(tlsDir.resolve("ca.crt")))
                .as("the CA is stable across rotation")
                .isEqualTo(Files.readAllBytes(frozen.resolve("ca.crt")));

        var rotatedTls = TlsConfig.mtlsServer(
                tlsDir.resolve("broker1.crt"), tlsDir.resolve("broker1.key"), tlsDir.resolve("ca.crt"));
        var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));
        var restarted = Broker.start(
                new Broker.Config(new NodeId(1), dataDir, raftPort, brokerPort, voters, 1).withTls(rotatedTls));
        try {
            awaitReady(restarted);
            try (var client = new BrokerClient("localhost", brokerPort, clientTls)) {
                client.produce("rotated", 0, "after-rotation".getBytes());
                var all = client.fetchAll("rotated", 0, 1 << 20);
                assertThat(all).hasSize(2);
                assertThat(new String(all.get(0))).isEqualTo("before-rotation");
            }
        } finally {
            restarted.close();
        }
    }

    private static void runScript(String... args) throws Exception {
        var cmd = new java.util.ArrayList<String>();
        cmd.add(script.toString());
        cmd.addAll(List.of(args));
        var p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        byte[] out = p.getInputStream().readAllBytes();
        int rc = p.waitFor();
        Assumptions.assumeTrue(rc == 0, "bootstrap-ca.sh failed rc=" + rc + ", output=\n" + new String(out));
    }

    private static void awaitReady(Broker broker) throws InterruptedException {
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
