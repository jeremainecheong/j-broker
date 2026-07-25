package jbroker.broker.txn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import jbroker.proto.common.TopicPartition;
import org.junit.jupiter.api.Test;

class TxnStateTopicCodecTest {

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }

    @Test
    void keyRoundTrips() {
        byte[] key = TxnStateTopic.keyForTxn("payments-svc-7");
        assertThat(TxnStateTopic.decodeTxnKey(key)).contains("payments-svc-7");
    }

    @Test
    void foreignRecordTypeKeyIsSkipped() {
        // First byte is the record-type tag; an unknown tag must decode to
        // empty (forward compat), not throw.
        byte[] key = TxnStateTopic.keyForTxn("t1");
        key[0] = 0x7F;
        assertThat(TxnStateTopic.decodeTxnKey(key)).isEmpty();
        assertThat(TxnStateTopic.decodeTxnKey(null)).isEmpty();
        assertThat(TxnStateTopic.decodeTxnKey(new byte[0])).isEmpty();
    }

    @Test
    void valueRoundTripsEveryState() {
        for (var state : TxnState.values()) {
            var rec = new TxnStateRecord(
                    "t1", 42L, 7, state, List.of(tp("orders", 0), tp("payments", 3)), 60_000, 1_234_567L);
            var decoded = TxnStateTopic.decodeTxnValue("t1", TxnStateTopic.valueForTxnState(rec));
            assertThat(decoded).isEqualTo(rec);
        }
    }

    @Test
    void valueRoundTripsEmptyPartitionSet() {
        var rec = new TxnStateRecord("t1", 1L, 0, TxnState.EMPTY, List.of(), 30_000, 0L);
        assertThat(TxnStateTopic.decodeTxnValue("t1", TxnStateTopic.valueForTxnState(rec)))
                .isEqualTo(rec);
    }

    @Test
    void truncatedValueThrows() {
        byte[] value = TxnStateTopic.valueForTxnState(
                new TxnStateRecord("t1", 1L, 0, TxnState.ONGOING, List.of(tp("orders", 0)), 30_000, 9L));
        byte[] truncated = new byte[value.length - 3];
        System.arraycopy(value, 0, truncated, 0, truncated.length);
        assertThatThrownBy(() -> TxnStateTopic.decodeTxnValue("t1", truncated))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void wireCodesAreStable() {
        // Persisted bytes depend on these exact values — never renumber.
        assertThat(TxnState.EMPTY.wireCode()).isEqualTo((byte) 0);
        assertThat(TxnState.ONGOING.wireCode()).isEqualTo((byte) 1);
        assertThat(TxnState.PREPARE_COMMIT.wireCode()).isEqualTo((byte) 2);
        assertThat(TxnState.PREPARE_ABORT.wireCode()).isEqualTo((byte) 3);
        assertThat(TxnState.COMPLETE_COMMIT.wireCode()).isEqualTo((byte) 4);
        assertThat(TxnState.COMPLETE_ABORT.wireCode()).isEqualTo((byte) 5);
        for (var s : TxnState.values()) {
            assertThat(TxnState.fromWireCode(s.wireCode())).isEqualTo(s);
        }
    }

    @Test
    void partitionRoutingIsStableAndInRange() {
        int p = TxnStateTopic.partitionFor("payments-svc-7", TxnStateTopic.PARTITION_COUNT);
        assertThat(p).isEqualTo(TxnStateTopic.partitionFor("payments-svc-7", TxnStateTopic.PARTITION_COUNT));
        assertThat(p).isBetween(0, TxnStateTopic.PARTITION_COUNT - 1);
        // Negative hashCode must still land in range.
        assertThat(TxnStateTopic.partitionFor("polygenelubricants", TxnStateTopic.PARTITION_COUNT))
                .isBetween(0, TxnStateTopic.PARTITION_COUNT - 1);
    }
}
