package jbroker.admin.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import jbroker.broker.ErrorCodes;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.ReassignPartitionRequest;
import jbroker.proto.broker.ReassignPartitionResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link BrokerAdminClientPool#controllerRouted} routing contract, against
 * scripted in-process gRPC brokers:
 *
 * <ol>
 *   <li>The suggested-leader hint is actually followed — even to a broker
 *       the pool was never configured with, which plain iteration can
 *       never reach.</li>
 *   <li>The hint retry is bounded: a useless hint costs exactly one extra
 *       call, then the answer propagates with the hint intact.</li>
 *   <li>Without a hint the pool falls back to plain iteration, like
 *       {@code firstNonNotLeader}.</li>
 * </ol>
 */
class BrokerAdminClientPoolHintFollowIT {

    /** Scripted Admin service: counts calls, answers from the supplier. */
    private static final class FakeBroker extends AdminGrpc.AdminImplBase {
        final AtomicInteger calls = new AtomicInteger();
        final Supplier<ReassignPartitionResponse> answer;

        FakeBroker(Supplier<ReassignPartitionResponse> answer) {
            this.answer = answer;
        }

        @Override
        public void reassignPartition(ReassignPartitionRequest req, StreamObserver<ReassignPartitionResponse> obs) {
            calls.incrementAndGet();
            obs.onNext(answer.get());
            obs.onCompleted();
        }
    }

    private final List<Server> servers = new java.util.ArrayList<>();
    private BrokerAdminClientPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) pool.shutdown();
        for (var s : servers) s.shutdownNow();
    }

    private int start(FakeBroker broker) throws Exception {
        var server = NettyServerBuilder.forPort(0).addService(broker).build().start();
        servers.add(server);
        return server.getPort();
    }

    private static ReassignPartitionResponse ok() {
        return ReassignPartitionResponse.getDefaultInstance();
    }

    private static ReassignPartitionResponse notLeader(String hintHost, int hintPort) {
        var err = jbroker.proto.broker.Error.newBuilder()
                .setCode(ErrorCodes.NOT_LEADER)
                .setMessage("not the controller");
        if (hintHost != null) {
            err.putHint("suggested_leader_host", hintHost).putHint("suggested_leader_port", Integer.toString(hintPort));
        }
        return ReassignPartitionResponse.newBuilder().setError(err).build();
    }

    private ReassignPartitionResponse route() {
        return pool.controllerRouted(
                c -> c.reassignPartition("orders", 0, List.of(1, 2)), ReassignPartitionResponse::getError);
    }

    @Test
    void hintReachesALeaderThePoolWasNeverConfiguredWith() throws Exception {
        var leader = new FakeBroker(BrokerAdminClientPoolHintFollowIT::ok);
        int leaderPort = start(leader);
        var follower = new FakeBroker(() -> notLeader("127.0.0.1", leaderPort));
        int followerPort = start(follower);

        // Only the follower is configured — blind iteration could never
        // find the leader; only the hint can.
        pool = new BrokerAdminClientPool("127.0.0.1:" + followerPort, false, "", "", "");

        var resp = route();

        assertThat(resp.getError().getCode()).isZero();
        assertThat(follower.calls.get()).isEqualTo(1);
        assertThat(leader.calls.get()).isEqualTo(1);
    }

    @Test
    void uselessHintCostsExactlyOneRetryAndPropagatesTheRefusal() throws Exception {
        // The follower stubbornly hints at itself, so following the hint
        // can never converge — the retry must stay bounded.
        var selfPort = new AtomicInteger();
        var follower = new FakeBroker(() -> notLeader("127.0.0.1", selfPort.get()));
        selfPort.set(start(follower));

        pool = new BrokerAdminClientPool("127.0.0.1:" + selfPort.get(), false, "", "", "");

        var resp = route();

        assertThat(resp.getError().getCode()).isEqualTo(ErrorCodes.NOT_LEADER);
        assertThat(resp.getError().getHintMap()).containsKey("suggested_leader_host");
        assertThat(follower.calls.get()).isEqualTo(2); // initial + one hint hop, nothing more
    }

    @Test
    void withoutAHintThePoolFallsBackToIteration() throws Exception {
        var follower = new FakeBroker(() -> notLeader(null, 0));
        int followerPort = start(follower);
        var leader = new FakeBroker(BrokerAdminClientPoolHintFollowIT::ok);
        int leaderPort = start(leader);

        pool = new BrokerAdminClientPool("127.0.0.1:" + followerPort + ",127.0.0.1:" + leaderPort, false, "", "", "");

        var resp = route();

        assertThat(resp.getError().getCode()).isZero();
        assertThat(follower.calls.get()).isEqualTo(1);
        assertThat(leader.calls.get()).isEqualTo(1);
    }
}
