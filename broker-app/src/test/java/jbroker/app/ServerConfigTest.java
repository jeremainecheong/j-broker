package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Layering and validation of the server configuration: defaults ← file ←
 * env ← flags, with unknown file keys rejected (typos must not silently
 * fall back to defaults) and every validation problem reported at once.
 */
class ServerConfigTest {

    private static final String[] NO_FLAGS = new String[0];

    @Test
    void defaultsApplyWithoutAnySource() throws Exception {
        var config = ServerConfig.resolve(null, Map.of(), NO_FLAGS);

        assertThat(config.intValue("node.id")).isEqualTo(1);
        assertThat(config.intValue("broker.port")).isEqualTo(9092);
        assertThat(config.longValue("storage.headroom.bytes")).isEqualTo(1024L * 1024 * 1024);
        assertThat(config.boolValue("tls.enabled")).isFalse();
        assertThat(config.validate()).isEmpty();
    }

    @Test
    void fileOverridesDefaultsEnvOverridesFileFlagsOverrideEnv(@TempDir Path dir) throws Exception {
        var file = dir.resolve("j-broker.yaml");
        Files.writeString(
                file,
                """
                broker.port: 7000
                raft.port: 7100
                node.id: 4
                """);

        var config = ServerConfig.resolve(
                file, Map.of("JBROKER_RAFT_PORT", "7200", "JBROKER_NODE_ID", "5"), new String[] {"--id", "6"});

        assertThat(config.intValue("broker.port")).as("file beats default").isEqualTo(7000);
        assertThat(config.intValue("raft.port")).as("env beats file").isEqualTo(7200);
        assertThat(config.intValue("node.id")).as("flag beats env").isEqualTo(6);
    }

    @Test
    void unknownFileKeysAreStartupErrors(@TempDir Path dir) throws Exception {
        var file = dir.resolve("j-broker.yaml");
        Files.writeString(file, "brokre.port: 7000\n");

        assertThatThrownBy(() -> ServerConfig.resolve(file, Map.of(), NO_FLAGS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown key")
                .hasMessageContaining("brokre.port");
    }

    @Test
    void nestedYamlIsRejected(@TempDir Path dir) throws Exception {
        var file = dir.resolve("j-broker.yaml");
        Files.writeString(file, """
                log:
                  cleaner: 5
                """);

        assertThatThrownBy(() -> ServerConfig.resolve(file, Map.of(), NO_FLAGS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scalar");
    }

    @Test
    void missingConfigFileIsAnError(@TempDir Path dir) {
        assertThatThrownBy(() -> ServerConfig.resolve(dir.resolve("absent.yaml"), Map.of(), NO_FLAGS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void validateCollectsEveryProblemAtOnce(@TempDir Path dir) throws Exception {
        var file = dir.resolve("j-broker.yaml");
        Files.writeString(
                file,
                """
                broker.port: 99999
                min.insync.replicas: zero
                voters: 2@host:1:2
                tls.enabled: true
                """);

        var config = ServerConfig.resolve(file, Map.of(), NO_FLAGS);
        var problems = config.validate();

        assertThat(problems)
                .anySatisfy(p -> assertThat(p).contains("broker.port"))
                .anySatisfy(p -> assertThat(p).contains("min.insync.replicas"))
                .anySatisfy(p -> assertThat(p).contains("voters must include node.id"))
                .anySatisfy(p -> assertThat(p).contains("tls.cert is required"));
    }

    @Test
    void tlsFilesMustExistWhenEnabled(@TempDir Path dir) throws Exception {
        var pem = Files.writeString(dir.resolve("a.pem"), "x");
        var file = dir.resolve("j-broker.yaml");
        Files.writeString(
                file,
                "tls.enabled: true\n"
                        + "tls.cert: " + pem + "\n"
                        + "tls.key: " + pem + "\n"
                        + "tls.trust: " + dir.resolve("missing.pem") + "\n");

        var config = ServerConfig.resolve(file, Map.of(), NO_FLAGS);

        assertThat(config.validate()).hasSize(1).first().asString().contains("tls.trust does not exist");
    }

    @Test
    void referenceCoversEveryDeclaredKey() {
        var reference = ServerConfig.renderReference();
        for (var key : ServerConfig.KEYS) {
            assertThat(reference).contains("`" + key.name() + "`");
        }
    }
}
