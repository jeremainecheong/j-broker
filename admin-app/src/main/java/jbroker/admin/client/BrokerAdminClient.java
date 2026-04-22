package jbroker.admin.client;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.broker.MetadataGrpc;

/**
 * Thin gRPC wrapper the admin-app uses to talk to a single broker. One
 * instance per configured broker; callers that need cluster-wide fan-out
 * iterate a {@link BrokerAdminClientPool}.
 *
 * <p>Scope for P8.2: {@code DescribeCluster} only. Later Phase 8 slices add
 * {@code Admin} + {@code Consumer} stubs (topic CRUD, consumer-group
 * describe) on the same channel.
 */
public final class BrokerAdminClient implements AutoCloseable {

    private final String address;
    private final ManagedChannel channel;
    private final MetadataGrpc.MetadataBlockingStub metadata;

    public BrokerAdminClient(String host, int port) {
        this.address = host + ":" + port;
        this.channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.metadata = MetadataGrpc.newBlockingStub(channel);
    }

    public static BrokerAdminClient parse(String hostPort) {
        int colon = hostPort.indexOf(':');
        if (colon < 0) throw new IllegalArgumentException("broker address needs host:port, got " + hostPort);
        return new BrokerAdminClient(hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
    }

    public String address() {
        return address;
    }

    /**
     * Issues a {@code DescribeCluster} RPC with a short deadline. gRPC status
     * errors (unreachable broker, deadline) bubble up as
     * {@link StatusRuntimeException}; callers that fan out across brokers are
     * expected to swallow them and move on to the next endpoint.
     */
    public DescribeClusterResponse describeCluster() {
        return metadata.withDeadlineAfter(3, TimeUnit.SECONDS)
                .describeCluster(DescribeClusterRequest.newBuilder().build());
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
