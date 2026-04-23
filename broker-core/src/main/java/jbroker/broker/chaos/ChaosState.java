package jbroker.broker.chaos;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * P9.3 — in-process chaos injection state. One instance per broker,
 * read by the gRPC interceptors that gate inbound / outbound Raft and
 * replication traffic, and written by the {@code /debug/chaos/*} HTTP
 * endpoints exposed on the broker.
 *
 * <p>Cooperative model: brokers fault themselves in response to admin
 * requests rather than having an external actor send SIGSTOP or set
 * iptables rules. This is portable across dev / CI / container
 * deployments, and matches the simulator's in-memory chaos style.
 */
public final class ChaosState {

    private final AtomicBoolean paused = new AtomicBoolean(false);
    /** Peer broker ids currently blocked (both inbound + outbound). */
    private final ConcurrentHashMap<Integer, Boolean> blockedPeers = new ConcurrentHashMap<>();
    /** Global latency injection in millis (applied on every intercepted RPC). */
    private volatile long latencyMs = 0L;

    public boolean isPaused() {
        return paused.get();
    }

    public void pause() {
        paused.set(true);
    }

    public void resume() {
        paused.set(false);
    }

    public boolean isBlocked(int peerId) {
        return blockedPeers.containsKey(peerId);
    }

    public void blockPeer(int peerId) {
        blockedPeers.put(peerId, Boolean.TRUE);
    }

    public void unblockPeer(int peerId) {
        blockedPeers.remove(peerId);
    }

    public void clearBlockedPeers() {
        blockedPeers.clear();
    }

    public Map<Integer, Boolean> blockedPeers() {
        return Map.copyOf(blockedPeers);
    }

    public long latencyMs() {
        return latencyMs;
    }

    public void setLatencyMs(long millis) {
        this.latencyMs = Math.max(0L, millis);
    }

    /** Helper for interceptors: sleep if latency is non-zero, swallowing interruption. */
    public void maybeSleep() {
        long ms = latencyMs;
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
