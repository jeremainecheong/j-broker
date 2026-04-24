package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import jbroker.proto.raft.BrokerRegistrationRecord;
import jbroker.proto.raft.MetadataRecord;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

class MetadataStateBrokerRegistrationTest {

    private record Reg(int id, String host, int port) {}

    @Test
    void brokerRegistrationListenerFiresOnApply() {
        var seen = new ArrayList<Reg>();
        MetadataStateMachine.BrokerRegistrationListener listener =
                (id, host, port, advHost, advPort) -> seen.add(new Reg(id, host, port));
        var sm = new MetadataStateMachine(
                new TopicManager(), new ProducerIdRegistry(), (t, p, e, l) -> {}, listener, (t, p, s) -> {});

        sm.apply(wrap(reg(1, "h1", 9001), 1));
        sm.apply(wrap(reg(2, "h2", 9002), 2));
        sm.apply(wrap(reg(1, "h1-new", 9001), 3));

        assertThat(seen).containsExactly(new Reg(1, "h1", 9001), new Reg(2, "h2", 9002), new Reg(1, "h1-new", 9001));
    }

    private static BrokerRegistrationRecord reg(int id, String host, int port) {
        return BrokerRegistrationRecord.newBuilder()
                .setBrokerId(id)
                .setHost(host)
                .setPort(port)
                .build();
    }

    private static LogEntry wrap(BrokerRegistrationRecord r, long index) {
        byte[] bytes = MetadataRecord.newBuilder().setBroker(r).build().toByteArray();
        return new LogEntry(index, new Term(1), LogEntry.Type.NORMAL, bytes);
    }
}
