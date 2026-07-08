package jbroker.broker.chaos;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-process chaos injection state. One instance per broker,
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
    /**
     * Peer broker ids whose OUTBOUND (self → peer) RPCs are blocked. Populated
     * by {@link #blockPeer} (symmetric partition — default) and
     * {@link #blockOutboundToPeer} (asymmetric — caller only blocks outbound).
     * Consulted by {@link ChaosClientInterceptor} per RPC.
     */
    private final ConcurrentHashMap<Integer, Boolean> outboundBlocked = new ConcurrentHashMap<>();
    /**
     * Peer broker ids whose INBOUND (peer → self) RPCs are rejected at the
     * server. Populated symmetrically by {@link #blockPeer} and asymmetrically
     * by {@link #blockInboundFromPeer}. Consulted by
     * {@link ChaosServerInterceptor} per RPC.
     *
     * <p>Audit-finding #3 — before this split, both sides consulted the same
     * set, so partitions were always symmetric. Asymmetric partitions
     * (A→B works, B→A doesn't) are the shape Raft elections actually need
     * to defend against (a stale leader still hearing heartbeats but unable
     * to broadcast one).
     */
    private final ConcurrentHashMap<Integer, Boolean> inboundBlocked = new ConcurrentHashMap<>();
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

    /** Symmetric: true if outbound to peer is blocked. Kept for back-compat with existing call sites. */
    public boolean isBlocked(int peerId) {
        return isOutboundBlocked(peerId);
    }

    /** Used by {@link ChaosClientInterceptor} to short-circuit outbound RPCs to a partitioned peer. */
    public boolean isOutboundBlocked(int peerId) {
        return outboundBlocked.containsKey(peerId);
    }

    /** Used by {@link ChaosServerInterceptor} to reject inbound RPCs from a partitioned peer. */
    public boolean isInboundBlocked(int peerId) {
        return inboundBlocked.containsKey(peerId);
    }

    /**
     * Symmetric block — both outbound to and inbound from the peer. Matches
     * the original symmetric-only partition behavior so existing partition
     * scripts + tests are unaffected.
     */
    public void blockPeer(int peerId) {
        outboundBlocked.put(peerId, Boolean.TRUE);
        inboundBlocked.put(peerId, Boolean.TRUE);
    }

    /** Directional partition control, asymmetric: block only RPCs from self to peer; peer→self continues to work. */
    public void blockOutboundToPeer(int peerId) {
        outboundBlocked.put(peerId, Boolean.TRUE);
    }

    /** Directional partition control, asymmetric: block only RPCs from peer to self; self→peer continues to work. */
    public void blockInboundFromPeer(int peerId) {
        inboundBlocked.put(peerId, Boolean.TRUE);
    }

    /** Symmetric unblock — clears both directions. */
    public void unblockPeer(int peerId) {
        outboundBlocked.remove(peerId);
        inboundBlocked.remove(peerId);
    }

    public void unblockOutboundToPeer(int peerId) {
        outboundBlocked.remove(peerId);
    }

    public void unblockInboundFromPeer(int peerId) {
        inboundBlocked.remove(peerId);
    }

    public void clearBlockedPeers() {
        outboundBlocked.clear();
        inboundBlocked.clear();
    }

    /**
     * Observability: peer ids whose outbound RPCs are blocked. Kept for
     * back-compat with callers written against the original symmetric-only
     * partition API; new callers that need full visibility can also read
     * {@link #inboundBlockedPeers}.
     */
    public Map<Integer, Boolean> blockedPeers() {
        return Map.copyOf(outboundBlocked);
    }

    public Map<Integer, Boolean> outboundBlockedPeers() {
        return Map.copyOf(outboundBlocked);
    }

    public Map<Integer, Boolean> inboundBlockedPeers() {
        return Map.copyOf(inboundBlocked);
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
