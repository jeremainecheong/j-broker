package jbroker.admin.ui;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import jbroker.admin.AdminApp;
import jbroker.app.Broker;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.client.consumer.Consumer;
import jbroker.broker.client.consumer.ConsumerConfig;
import jbroker.broker.client.consumer.OffsetAndMetadata;
import jbroker.broker.client.consumer.RebalanceListener;
import jbroker.broker.client.consumer.StringDeserializer;
import jbroker.proto.common.TopicPartition;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * IT for the /groups/{id} admin actions — the reset-offsets modal and
 * delete-group danger button wired into {@link GroupsViewController}. Two
 * assertions are load-bearing for other tests:
 *
 * <ol>
 *   <li>The reset modal carries {@code x-cloak} — {@link AlpineCloakIT}
 *       only checks topics.html, so this test pins the invariant on the
 *       group-detail page explicitly.</li>
 *   <li>The reset form-post actually flows through to the broker — we
 *       commit offsets, POST the reset form, and assert the committed
 *       offset is reset back to zero via the REST describe endpoint.</li>
 * </ol>
 */
@SpringBootTest(
        classes = AdminApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "jbroker.redis.url=")
class GroupActionsViewIT {

    private static Broker broker;
    private static Path dataDir;
    private static int brokerPort;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startBroker() throws IOException, InterruptedException {
        brokerPort = freePort();
        int raftPort = freePort();
        dataDir = Files.createTempDirectory("p15-group-actions");
        broker = Broker.start(new Broker.Config(new NodeId(1), dataDir, raftPort, brokerPort));
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && broker.role() != Role.LEADER) {
            Thread.sleep(25);
        }
        waitForCoordinatorTopic(broker);
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) broker.close();
        if (dataDir != null) deleteQuietly(dataDir);
    }

    @DynamicPropertySource
    static void brokers(DynamicPropertyRegistry registry) {
        registry.add("jbroker.admin.brokers", () -> "localhost:" + brokerPort);
    }

    @Test
    void groupDetailPageRendersDeleteButtonAndResetModal() throws Exception {
        String groupId = "actions-render-group";
        seedCommittedGroup(groupId, "actions-render-topic", 5L);

        var resp = rest.getForEntity("http://localhost:" + port + "/groups/" + groupId, String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        String body = resp.getBody();
        assertThat(body).isNotNull();
        // Alpine x-data scope wraps the reset modal.
        assertThat(body).contains("x-data=\"{ showReset");
        // x-cloak is load-bearing — without it the modal flashes on first paint.
        assertThat(body).contains("x-cloak");
        // Delete form posts to the view-controller endpoint and guards with a JS confirm.
        assertThat(body).contains("/ui/groups/" + groupId + "/delete");
        assertThat(body).contains("confirm('Delete group?')");
        // Reset modal form posts to the view-controller endpoint with the expected fields.
        assertThat(body).contains("/ui/groups/" + groupId + "/reset-offsets");
        assertThat(body).contains("name=\"topic\"");
        assertThat(body).contains("name=\"partition\"");
        assertThat(body).contains("name=\"offset\"");
    }

    @Test
    void resetOffsetsFormPostResetsCommittedOffset() throws Exception {
        String groupId = "actions-reset-group";
        String topic = "actions-reset-topic";
        seedCommittedGroup(groupId, topic, 5L);

        // Sanity-check: the committed offset is 5 before we reset.
        assertCommittedOffset(groupId, topic, 0, 5L);

        // POST the reset form the same way the modal does.
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("topic", topic);
        form.add("partition", "0");
        form.add("offset", "0");
        form.add("leaderEpoch", "0");
        var postResp = rest.exchange(
                "http://localhost:" + port + "/ui/groups/" + groupId + "/reset-offsets",
                HttpMethod.POST,
                new HttpEntity<>(form, headers),
                String.class);
        // View controllers redirect back to /groups/{id}; TestRestTemplate follows by default.
        assertThat(postResp.getStatusCode().is2xxSuccessful()
                        || postResp.getStatusCode().is3xxRedirection())
                .isTrue();

        // Post-reset: committed offset must drop back to 0 (with a short
        // bounded poll to absorb the Raft commit round-trip — no sleeps).
        long deadline = System.currentTimeMillis() + 5_000;
        long committed = -1;
        while (System.currentTimeMillis() < deadline) {
            committed = readCommittedOffset(groupId, topic, 0);
            if (committed == 0L) return;
            Thread.sleep(25);
        }
        assertThat(committed).as("committed offset should reset to 0 within 5s").isEqualTo(0L);
    }

    /** Produces one record, commits a non-zero offset for partition 0, then closes the consumer. */
    private void seedCommittedGroup(String groupId, String topic, long commitOffset) throws Exception {
        try (var producer = new BrokerClient("localhost", brokerPort)) {
            producer.createTopic(topic, 1, 1);
            for (int i = 0; i < commitOffset + 5; i++) {
                producer.produce(topic, 0, ("v" + i).getBytes());
            }
        }
        var cfg = ConsumerConfig.builder(groupId, "localhost", brokerPort)
                .pollFetchDeadline(Duration.ofSeconds(5))
                .build();
        try (var consumer = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic), RebalanceListener.NO_OP);
            long deadline = System.currentTimeMillis() + 10_000;
            while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200));
            }
            var commits = new HashMap<TopicPartition, OffsetAndMetadata>();
            commits.put(tp(topic, 0), new OffsetAndMetadata(commitOffset));
            consumer.commitSync(commits);
        }
    }

    private void assertCommittedOffset(String groupId, String topic, int partition, long expected) {
        long actual = readCommittedOffset(groupId, topic, partition);
        assertThat(actual).isEqualTo(expected);
    }

    private long readCommittedOffset(String groupId, String topic, int partition) {
        // The REST describe endpoint surfaces committed offsets as a flat list.
        String body =
                rest.getForObject("http://localhost:" + port + "/api/v1/consumer-groups/" + groupId, String.class);
        if (body == null) return -1;
        // Bare-bones parse: look for the partition's committed_offset entry.
        // The /api/v1 envelope is snake_case (application.yml), and jackson
        // preserves record declaration order, so the needle below is stable.
        String needle = "\"topic\":\"" + topic + "\",\"partition\":" + partition + ",\"committed_offset\":";
        int idx = body.indexOf(needle);
        if (idx < 0) return -1;
        int start = idx + needle.length();
        int end = start;
        while (end < body.length() && (Character.isDigit(body.charAt(end)) || body.charAt(end) == '-')) end++;
        return Long.parseLong(body.substring(start, end));
    }

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }

    private static void waitForCoordinatorTopic(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.topics()
                            .describe(jbroker.broker.ConsumerOffsetsTopic.NAME)
                            .isPresent()
                    && broker.brokerRegistry().addressFor(1).isPresent()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("__consumer_offsets did not auto-create within 10s");
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void deleteQuietly(Path root) {
        try (var s = Files.walk(root)) {
            s.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort
                }
            });
        } catch (IOException ignored) {
            // best-effort
        }
    }
}
