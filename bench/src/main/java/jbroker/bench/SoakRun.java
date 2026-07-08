package jbroker.bench;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import jbroker.broker.client.BrokerClient;

/**
 * The soak loss/duplication invariant's idempotent-producer load driver — the produce and
 * verify halves of {@code scripts/chaos/scenario-chaos-with-load.sh}.
 *
 * <p><b>soak-produce</b> sends sequenced payloads ({@code p<partition>-<seq>})
 * with {@code acks=all} + idempotent producer state for the whole duration,
 * retrying each (partition, sequence) across the broker list until acked —
 * leader failovers show up as rotations, never as gaps. Every acked payload
 * is appended to {@code --acked-file}; that file is the ground truth the
 * verify half replays.
 *
 * <p><b>soak-verify</b> re-reads every partition from offset 0 and holds the
 * run to the invariant: every acked record consumed exactly once, no record
 * consumed twice ({@link SoakLedger}). Unacked records may appear at most
 * once (an ack the client never saw can still have committed).
 */
public final class SoakRun {

    private SoakRun() {}

    public static void produce(String[] args) throws Exception {
        var brokers = parseBrokers(BenchArgs.get(args, "--brokers", "127.0.0.1:9092"));
        String topic = BenchArgs.get(args, "--topic", null);
        int partitions = Math.max(1, BenchArgs.getInt(args, "--partitions", 3));
        int durationSeconds = BenchArgs.getInt(args, "--duration-seconds", 600);
        int rate = Math.max(1, BenchArgs.getInt(args, "--rate", 200));
        String ackedFileStr = BenchArgs.get(args, "--acked-file", null);
        if (topic == null || ackedFileStr == null) {
            System.err.println("--topic and --acked-file required");
            System.exit(2);
        }
        Path ackedFile = Path.of(ackedFileStr);

        var clients = openClients(brokers);
        long pid = withAnyBroker(clients, BrokerClient::initProducerId);
        System.out.printf(
                "soak-produce: pid=%d topic=%s partitions=%d duration=%ds rate=%d/s%n",
                pid, topic, partitions, durationSeconds, rate);

        long[] nextSeq = new long[partitions];
        int[] preferredBroker = new int[partitions];
        long acked = 0;
        long rotations = 0;
        long deadlineNanos = System.nanoTime() + durationSeconds * 1_000_000_000L;
        long sleepNanosPerRecord = 1_000_000_000L / rate;
        long lastReport = System.nanoTime();

        try (var out = Files.newBufferedWriter(ackedFile, StandardCharsets.UTF_8)) {
            int p = 0;
            while (System.nanoTime() < deadlineNanos) {
                long seq = nextSeq[p];
                String payload = "p" + p + "-" + seq;
                if (produceOneUntilAcked(clients, preferredBroker, p, topic, payload, pid, seq, deadlineNanos)) {
                    nextSeq[p]++;
                    acked++;
                    out.write(payload);
                    out.newLine();
                } else {
                    rotations++; // deadline hit mid-record; it stays unacked
                }
                p = (p + 1) % partitions;
                java.util.concurrent.locks.LockSupport.parkNanos(sleepNanosPerRecord);
                if (System.nanoTime() - lastReport > 5_000_000_000L) {
                    System.out.printf("soak-produce: acked=%d%n", acked);
                    lastReport = System.nanoTime();
                }
            }
            out.flush();
        } finally {
            closeAll(clients);
        }
        System.out.printf("soak-produce: done, acked=%d records (%d abandoned at deadline)%n", acked, rotations);
    }

    /**
     * Retry the same (partition, sequence) across the broker list until it
     * is acked or the soak deadline passes. OUT_OF_ORDER_SEQUENCE on a
     * retry means an earlier attempt already committed (the broker's dedup
     * window has advanced past this sequence) — that is an ack.
     */
    private static boolean produceOneUntilAcked(
            List<ClientSlot> clients,
            int[] preferredBroker,
            int partition,
            String topic,
            String payload,
            long pid,
            long seq,
            long deadlineNanos)
            throws InterruptedException {
        int attempt = 0;
        while (System.nanoTime() < deadlineNanos + 5_000_000_000L) { // grace: finish in-flight record
            int idx = (preferredBroker[partition] + attempt) % clients.size();
            var slot = clients.get(idx);
            try {
                slot.ensureOpen();
                slot.client.idempotentProduceAcksAll(
                        topic, partition, payload.getBytes(StandardCharsets.UTF_8), pid, /*epoch*/ 0, (int) seq);
                preferredBroker[partition] = idx;
                return true;
            } catch (RuntimeException e) {
                if (String.valueOf(e.getMessage()).contains("OUT_OF_ORDER_SEQUENCE")) {
                    preferredBroker[partition] = idx;
                    return true;
                }
                slot.closeQuietly();
                attempt++;
                if (attempt % clients.size() == 0) Thread.sleep(250); // full rotation failed; back off a beat
            }
        }
        return false;
    }

    public static void verify(String[] args) throws Exception {
        var brokers = parseBrokers(BenchArgs.get(args, "--brokers", "127.0.0.1:9092"));
        String topic = BenchArgs.get(args, "--topic", null);
        int partitions = Math.max(1, BenchArgs.getInt(args, "--partitions", 3));
        int settleSeconds = BenchArgs.getInt(args, "--settle-seconds", 90);
        String ackedFileStr = BenchArgs.get(args, "--acked-file", null);
        if (topic == null || ackedFileStr == null) {
            System.err.println("--topic and --acked-file required");
            System.exit(2);
        }
        var acked = Files.readAllLines(Path.of(ackedFileStr), StandardCharsets.UTF_8).stream()
                .filter(s -> !s.isBlank())
                .toList();

        // Post-chaos, replicas may still be catching up and HWM gating can
        // briefly hide committed records. Re-read until clean or settle
        // budget exhausted; loss is only declared at the end.
        long settleDeadline = System.nanoTime() + settleSeconds * 1_000_000_000L;
        SoakLedger.Result result = null;
        while (true) {
            var clients = openClients(brokers);
            try {
                var consumed = new ArrayList<String>();
                for (int p = 0; p < partitions; p++) {
                    consumed.addAll(readPartitionFully(clients, topic, p));
                }
                result = SoakLedger.compare(acked, consumed);
            } finally {
                closeAll(clients);
            }
            if (result.clean() || System.nanoTime() > settleDeadline) break;
            System.out.printf("soak-verify: not settled yet (%s); retrying%n", result.summary());
            Thread.sleep(5_000);
        }

        System.out.println("soak-verify: " + result.summary());
        if (!result.clean()) {
            result.missing().stream().limit(20).forEach(m -> System.out.println("  MISSING   " + m));
            result.duplicated().stream().limit(20).forEach(d -> System.out.println("  DUPLICATE " + d));
            System.exit(1);
        }
        System.out.println("soak-verify: PASS — no lost records, no duplicated records");
    }

    private static List<String> readPartitionFully(List<ClientSlot> clients, String topic, int partition)
            throws InterruptedException {
        var out = new ArrayList<String>();
        long offset = 0;
        int emptyReads = 0;
        int broker = 0;
        long deadline = System.nanoTime() + 120_000_000_000L;
        while (emptyReads < 3 && System.nanoTime() < deadline) {
            var slot = clients.get(broker % clients.size());
            List<byte[]> batch;
            try {
                slot.ensureOpen();
                batch = slot.client.fetch(topic, partition, offset, 1 << 20);
            } catch (RuntimeException e) {
                slot.closeQuietly();
                broker++;
                Thread.sleep(200);
                continue;
            }
            if (batch.isEmpty()) {
                emptyReads++;
                Thread.sleep(300);
                continue;
            }
            emptyReads = 0;
            for (var v : batch) out.add(new String(v, StandardCharsets.UTF_8));
            offset += batch.size();
        }
        return out;
    }

    // --- broker connection plumbing ---

    private record HostPort(String host, int port) {}

    /** Lazily (re)opened client per broker address; failures close the slot so the next use reconnects. */
    private static final class ClientSlot {
        final HostPort addr;
        BrokerClient client;

        ClientSlot(HostPort addr) {
            this.addr = addr;
        }

        void ensureOpen() {
            if (client == null) client = new BrokerClient(addr.host(), addr.port());
        }

        void closeQuietly() {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception ignored) {
                    // reconnect on next use
                }
                client = null;
            }
        }
    }

    private static List<HostPort> parseBrokers(String csv) {
        var out = new ArrayList<HostPort>();
        for (var part : csv.split(",")) {
            var bits = part.trim().split(":");
            out.add(new HostPort(bits[0], Integer.parseInt(bits[1])));
        }
        return out;
    }

    private static List<ClientSlot> openClients(List<HostPort> brokers) {
        var out = new ArrayList<ClientSlot>();
        for (var b : brokers) out.add(new ClientSlot(b));
        return out;
    }

    private static void closeAll(List<ClientSlot> clients) {
        for (var c : clients) c.closeQuietly();
    }

    private interface ClientCall<T> {
        T apply(BrokerClient client);
    }

    private static <T> T withAnyBroker(List<ClientSlot> clients, ClientCall<T> call) throws InterruptedException {
        RuntimeException last = null;
        for (int round = 0; round < 20; round++) {
            for (var slot : clients) {
                try {
                    slot.ensureOpen();
                    return call.apply(slot.client);
                } catch (RuntimeException e) {
                    last = e;
                    slot.closeQuietly();
                }
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("no broker reachable after 20 rounds", last);
    }
}
