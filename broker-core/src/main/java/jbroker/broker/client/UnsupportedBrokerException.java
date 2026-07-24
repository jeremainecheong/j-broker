package jbroker.broker.client;

/**
 * A broker's advertised protocol range does not overlap the range this
 * client speaks — or the broker predates version discovery entirely and
 * has no {@code Metadata.ApiVersions} RPC. Raised on the first use of a
 * broker connection, before any real RPC, and never retried: a version
 * mismatch cannot heal on its own — upgrade whichever side is older so
 * the ranges overlap.
 */
public final class UnsupportedBrokerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String endpoint;
    private final int brokerMin;
    private final int brokerMax;
    private final int clientMin;
    private final int clientMax;

    private UnsupportedBrokerException(
            String message, String endpoint, int brokerMin, int brokerMax, int clientMin, int clientMax) {
        super(message);
        this.endpoint = endpoint;
        this.brokerMin = brokerMin;
        this.brokerMax = brokerMax;
        this.clientMin = clientMin;
        this.clientMax = clientMax;
    }

    /** The broker answered ApiVersions with a range disjoint from the client's. */
    static UnsupportedBrokerException incompatibleRange(
            String endpoint, int brokerMin, int brokerMax, int clientMin, int clientMax) {
        return new UnsupportedBrokerException(
                "broker at " + endpoint + " speaks protocol versions [" + brokerMin + ", " + brokerMax
                        + "] but this client supports [" + clientMin + ", " + clientMax
                        + "]; upgrade whichever side is older so the ranges overlap",
                endpoint,
                brokerMin,
                brokerMax,
                clientMin,
                clientMax);
    }

    /** The broker has no ApiVersions RPC at all (gRPC UNIMPLEMENTED). */
    static UnsupportedBrokerException missingHandshake(String endpoint, int clientMin, int clientMax) {
        return new UnsupportedBrokerException(
                "broker at " + endpoint + " predates protocol version discovery (no ApiVersions RPC); this client"
                        + " supports protocol versions [" + clientMin + ", " + clientMax
                        + "] and requires a broker that advertises its range — upgrade the broker",
                endpoint,
                -1,
                -1,
                clientMin,
                clientMax);
    }

    /** {@code host:port} of the incompatible broker. */
    public String endpoint() {
        return endpoint;
    }

    /** Broker's advertised minimum, or -1 when the broker predates the handshake. */
    public int brokerMin() {
        return brokerMin;
    }

    /** Broker's advertised maximum, or -1 when the broker predates the handshake. */
    public int brokerMax() {
        return brokerMax;
    }

    /** Oldest protocol version this client speaks. */
    public int clientMin() {
        return clientMin;
    }

    /** Newest protocol version this client speaks. */
    public int clientMax() {
        return clientMax;
    }
}
