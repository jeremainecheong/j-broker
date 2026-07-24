package jbroker.broker;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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

    /** Classpath location of the properties file the build stamps into the jar. */
    static final String VERSION_RESOURCE = "/jbroker/broker/broker-version.properties";

    /** Used only when the stamped resource is absent, e.g. IDE runs against raw class dirs. */
    static final String FALLBACK_VERSION = "2.0.0-SNAPSHOT";

    /**
     * Semantic version of the broker build. Gradle stamps the project
     * version into {@value #VERSION_RESOURCE} at build time, so this
     * always mirrors the version the jar was actually built as.
     */
    public static final String BROKER_VERSION = load("version", FALLBACK_VERSION);

    /** Commit id the broker was built from, or {@code "unknown"} outside a git checkout. */
    public static final String BUILD_COMMIT = load("commit", "unknown");

    private static String load(String key, String fallback) {
        try (InputStream in = ProtocolVersion.class.getResourceAsStream(VERSION_RESOURCE)) {
            if (in == null) {
                return fallback;
            }
            var props = new Properties();
            props.load(in);
            String value = props.getProperty(key, "").trim();
            return value.isEmpty() ? fallback : value;
        } catch (IOException e) {
            return fallback;
        }
    }

    private ProtocolVersion() {}
}
