package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.DeleteTopicRequest;
import jbroker.proto.broker.UpdateTopicConfigRequest;
import jbroker.proto.raft.MetadataRecord;
import org.junit.jupiter.api.Test;

class AdminHandlerDeleteConfigTest {

    @Test
    void deleteUnknownTopicReturnsUnknownTopic() throws Exception {
        var tm = new TopicManager();
        var handler = new AdminHandler(
                tm,
                (payload, timeout) -> {
                    /* never proposes */
                },
                1,
                () -> Set.of(1),
                Optional::empty,
                new BrokerRegistry());

        var resp = handler.deleteTopic(
                DeleteTopicRequest.newBuilder().setTopic("nope").build());

        assertThat(resp.hasError()).isTrue();
        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.UNKNOWN_TOPIC);
    }

    @Test
    void deleteTopicProposesDeleteTopicRecord() throws Exception {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 3, 1, 0);
        var seenPayload = new AtomicReference<byte[]>();
        AdminHandler.MetadataProposer proposer = (payload, timeoutMillis) -> {
            seenPayload.set(payload);
            tm.onTopicDeleted("orders");
        };
        var handler = new AdminHandler(tm, proposer, 1, () -> Set.of(1), Optional::empty, new BrokerRegistry());

        var resp = handler.deleteTopic(
                DeleteTopicRequest.newBuilder().setTopic("orders").build());

        assertThat(resp.hasError()).isFalse();
        assertThat(seenPayload.get()).isNotNull();
        var record = MetadataRecord.parseFrom(seenPayload.get());
        assertThat(record.hasDeleteTopic()).isTrue();
        assertThat(record.getDeleteTopic().getTopic()).isEqualTo("orders");
    }

    @Test
    void deleteTopicPopulatesNotLeaderHintsWhenLeaderKnown() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 3, 1, 0);
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(2, "leader-host", 9999);
        var handler = new AdminHandler(
                tm,
                (p, ms) -> {
                    throw new IllegalStateException("not the leader");
                },
                1,
                () -> Set.of(1, 2),
                () -> Optional.of(2),
                reg);

        var resp = handler.deleteTopic(
                DeleteTopicRequest.newBuilder().setTopic("orders").build());

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        assertThat(resp.getError().getHintMap()).containsEntry("suggested_leader_id", "2");
        assertThat(resp.getError().getHintMap()).containsEntry("suggested_leader_host", "leader-host");
        assertThat(resp.getError().getHintMap()).containsEntry("suggested_leader_port", "9999");
    }

    @Test
    void updateTopicConfigUnknownTopicReturnsUnknownTopic() {
        var tm = new TopicManager();
        var handler = new AdminHandler(tm, (p, ms) -> {}, 1, () -> Set.of(1), Optional::empty, new BrokerRegistry());

        var resp = handler.updateTopicConfig(UpdateTopicConfigRequest.newBuilder()
                .setTopic("nope")
                .putConfig("retention.ms", "1000")
                .build());

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.UNKNOWN_TOPIC);
    }

    @Test
    void updateTopicConfigProposesAndEchoesMergedConfig() throws InvalidProtocolBufferException {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L, false, false, Map.of("retention.ms", "86400000"));
        var seen = new AtomicReference<byte[]>();
        AdminHandler.MetadataProposer proposer = (payload, timeoutMillis) -> {
            seen.set(payload);
            tm.onTopicConfigUpdated("orders", Map.of("compression.type", "zstd"));
        };
        var handler = new AdminHandler(tm, proposer, 1, () -> Set.of(1), Optional::empty, new BrokerRegistry());

        var resp = handler.updateTopicConfig(UpdateTopicConfigRequest.newBuilder()
                .setTopic("orders")
                .putConfig("compression.type", "zstd")
                .build());

        assertThat(resp.hasError()).isFalse();
        var merged = resp.getConfigMap();
        assertThat(merged).containsEntry("retention.ms", "86400000");
        assertThat(merged).containsEntry("compression.type", "zstd");

        var record = MetadataRecord.parseFrom(seen.get());
        assertThat(record.hasUpdateTopicConfig()).isTrue();
        assertThat(record.getUpdateTopicConfig().getConfigMap()).containsEntry("compression.type", "zstd");
    }

    @Test
    void createTopicRoundTripsConfigMap() throws InvalidProtocolBufferException {
        var tm = new TopicManager();
        var seen = new AtomicReference<byte[]>();
        AdminHandler.MetadataProposer proposer = (payload, timeoutMillis) -> seen.set(payload);
        var handler = new AdminHandler(tm, proposer, 1, () -> Set.of(1), Optional::empty, new BrokerRegistry());

        handler.createTopic(CreateTopicRequest.newBuilder()
                .setTopic("orders")
                .setPartitions(1)
                .setReplicationFactor(1)
                .putConfig("retention.ms", "60000")
                .build());

        var record = MetadataRecord.parseFrom(seen.get());
        assertThat(record.hasCreateTopic()).isTrue();
        assertThat(record.getCreateTopic().getTopic().getConfigMap()).containsEntry("retention.ms", "60000");
    }
}
