package jbroker.broker;

/**
 * The broker's supported protocol range, exchanged with clients via
 * {@code Metadata.ApiVersions} at connect time and piggybacked on broker
 * heartbeats so every peer's range is visible from one
 * {@code DescribeCluster} call.
 *
 * <p>Version 1 covers the entire wire surface as of the release that
 * introduced version discovery. Bump {@link #CURRENT} when an RPC changes
 * shape incompatibly; raise {@link #MIN_SUPPORTED} only when dropping
 * support for clients older than a released version.
 */
public final class ProtocolVersion {

    /** Oldest protocol version this broker still accepts. */
    public static final int MIN_SUPPORTED = 1;

    /** Newest protocol version this broker speaks. */
    public static final int CURRENT = 1;

    /**
     * Semantic version of the broker build. Mirrors the Gradle project
     * version (root build.gradle.kts) by hand — the build stamps neither
     * a manifest Implementation-Version nor a resource, so this constant
     * is the only version string visible at runtime.
     */
    public static final String BROKER_VERSION = "0.1.0-SNAPSHOT";

    private ProtocolVersion() {}
}
