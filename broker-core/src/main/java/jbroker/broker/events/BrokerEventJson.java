package jbroker.broker.events;

/**
 * Hand-rolled JSON encoder for {@link BrokerEvent}. broker-core avoids a
 * Jackson dependency so every module that imports broker-core stays
 * classpath-small; the admin-app uses Jackson on its own REST surface.
 *
 * <p>Output matches theschema per event type. String values are
 * emitted with {@link #escape} which handles quotes, backslashes, and
 * control chars — sufficient for broker-produced payloads (topic names,
 * group ids, host strings).
 */
public final class BrokerEventJson {

    private BrokerEventJson() {}

    public static String encode(BrokerEvent e) {
        return switch (e) {
            case BrokerEvent.LeaderChanged l -> String.format(
                    "{\"topic\":\"%s\",\"partition\":%d,\"oldLeader\":%d,\"newLeader\":%d,\"leaderEpoch\":%d}",
                    escape(l.topic()), l.partition(), l.oldLeader(), l.newLeader(), l.leaderEpoch());
            case BrokerEvent.IsrChanged i -> {
                var sb = new StringBuilder(
                        "{\"topic\":\"" + escape(i.topic()) + "\",\"partition\":" + i.partition() + ",\"isr\":[");
                for (int x = 0; x < i.isr().size(); x++) {
                    if (x > 0) sb.append(',');
                    sb.append(i.isr().get(x));
                }
                sb.append("]}");
                yield sb.toString();
            }
            case BrokerEvent.RaftTermChanged r -> String.format(
                    "{\"oldTerm\":%d,\"newTerm\":%d,\"newLeader\":%d}", r.oldTerm(), r.newTerm(), r.newLeader());
            case BrokerEvent.BrokerRegistered b -> String.format(
                    "{\"brokerId\":%d,\"host\":\"%s\",\"port\":%d}", b.brokerId(), escape(b.host()), b.port());
            case BrokerEvent.BrokerFenced b -> String.format("{\"brokerId\":%d}", b.brokerId());
            case BrokerEvent.ConsumerGroupRebalance c -> String.format(
                    "{\"groupId\":\"%s\",\"generation\":%d,\"memberCount\":%d}",
                    escape(c.groupId()), c.generation(), c.memberCount());
            case BrokerEvent.ChaosAction ca -> {
                var sb = new StringBuilder(96);
                sb.append("{\"action\":\"")
                        .append(escape(ca.action()))
                        .append("\",\"brokerId\":")
                        .append(ca.brokerId());
                if (ca.peerId() != null) sb.append(",\"peerId\":").append(ca.peerId());
                if (ca.millis() != null) sb.append(",\"millis\":").append(ca.millis());
                sb.append('}');
                yield sb.toString();
            }
        };
    }

    private static String escape(String s) {
        if (s == null) return "";
        var sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
