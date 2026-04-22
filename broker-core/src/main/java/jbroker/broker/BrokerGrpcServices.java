package jbroker.broker;

import io.grpc.stub.StreamObserver;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.BrokerHeartbeatRequest;
import jbroker.proto.broker.BrokerHeartbeatResponse;
import jbroker.proto.broker.ClusterGrpc;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.CommitOffsetsResponse;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatResponse;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.CreateTopicResponse;
import jbroker.proto.broker.DescribeTopicRequest;
import jbroker.proto.broker.DescribeTopicResponse;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.FindCoordinatorResponse;
import jbroker.proto.broker.InitProducerIdRequest;
import jbroker.proto.broker.InitProducerIdResponse;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ListTopicsResponse;
import jbroker.proto.broker.OffsetsForLeaderEpochRequest;
import jbroker.proto.broker.OffsetsForLeaderEpochResponse;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.proto.broker.ReplicaConsumerGrpc;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;

/**
 * Thin wrappers that turn {@link ProduceHandler} / {@link FetchHandler} /
 * {@link AdminHandler} into the generated {@code RaftImplBase}-style gRPC
 * service classes. Kept together so the broker-app wiring can pull in one
 * file.
 */
public final class BrokerGrpcServices {

    private BrokerGrpcServices() {}

    public static ProducerGrpc.ProducerImplBase producer(ProduceHandler handler, InitProducerIdHandler initProducerId) {
        return new ProducerGrpc.ProducerImplBase() {
            @Override
            public void produce(ProduceRequest req, StreamObserver<ProduceResponse> out) {
                out.onNext(handler.handle(req));
                out.onCompleted();
            }

            @Override
            public void initProducerId(InitProducerIdRequest req, StreamObserver<InitProducerIdResponse> out) {
                out.onNext(initProducerId.initProducerId(req));
                out.onCompleted();
            }
        };
    }

    /**
     * Back-compat overload used by Milestone 5/6 tests that don't yet wire the
     * consumer-group RPCs. Equivalent to the dual-handler form below with a
     * placeholder {@link ConsumerHandler} whose group RPCs always return
     * {@code COORDINATOR_NOT_AVAILABLE}.
     */
    public static ConsumerGrpc.ConsumerImplBase consumer(FetchHandler handler) {
        return new ConsumerGrpc.ConsumerImplBase() {
            @Override
            public void fetch(FetchRequest req, StreamObserver<FetchResponse> out) {
                out.onNext(handler.handle(req));
                out.onCompleted();
            }
        };
    }

    public static ConsumerGrpc.ConsumerImplBase consumer(FetchHandler fetch, ConsumerHandler consumer) {
        return new ConsumerGrpc.ConsumerImplBase() {
            @Override
            public void fetch(FetchRequest req, StreamObserver<FetchResponse> out) {
                out.onNext(fetch.handle(req));
                out.onCompleted();
            }

            @Override
            public void listOffsets(ListOffsetsRequest req, StreamObserver<ListOffsetsResponse> out) {
                out.onNext(consumer.listOffsets(req));
                out.onCompleted();
            }

            @Override
            public void findCoordinator(FindCoordinatorRequest req, StreamObserver<FindCoordinatorResponse> out) {
                out.onNext(consumer.findCoordinator(req));
                out.onCompleted();
            }

            @Override
            public void consumerGroupHeartbeat(
                    ConsumerGroupHeartbeatRequest req, StreamObserver<ConsumerGroupHeartbeatResponse> out) {
                out.onNext(consumer.consumerGroupHeartbeat(req));
                out.onCompleted();
            }

            @Override
            public void commitOffsets(CommitOffsetsRequest req, StreamObserver<CommitOffsetsResponse> out) {
                out.onNext(consumer.commitOffsets(req));
                out.onCompleted();
            }

            @Override
            public void fetchOffsets(FetchOffsetsRequest req, StreamObserver<FetchOffsetsResponse> out) {
                out.onNext(consumer.fetchOffsets(req));
                out.onCompleted();
            }
        };
    }

    public static ReplicaConsumerGrpc.ReplicaConsumerImplBase replicaConsumer(
            ReplicaFetchHandler fetchHandler, OffsetsForLeaderEpochHandler offsetsHandler) {
        return new ReplicaConsumerGrpc.ReplicaConsumerImplBase() {
            @Override
            public void replicaFetch(ReplicaFetchRequest req, StreamObserver<ReplicaFetchResponse> out) {
                out.onNext(fetchHandler.handle(req));
                out.onCompleted();
            }

            @Override
            public void offsetsForLeaderEpoch(
                    OffsetsForLeaderEpochRequest req, StreamObserver<OffsetsForLeaderEpochResponse> out) {
                out.onNext(offsetsHandler.handle(req));
                out.onCompleted();
            }
        };
    }

    public static ClusterGrpc.ClusterImplBase cluster(BrokerHeartbeatHandler handler) {
        return new ClusterGrpc.ClusterImplBase() {
            @Override
            public void brokerHeartbeat(BrokerHeartbeatRequest req, StreamObserver<BrokerHeartbeatResponse> out) {
                out.onNext(handler.handle(req));
                out.onCompleted();
            }
        };
    }

    public static AdminGrpc.AdminImplBase admin(AdminHandler handler) {
        return new AdminGrpc.AdminImplBase() {
            @Override
            public void createTopic(CreateTopicRequest req, StreamObserver<CreateTopicResponse> out) {
                out.onNext(handler.createTopic(req));
                out.onCompleted();
            }

            @Override
            public void listTopics(ListTopicsRequest req, StreamObserver<ListTopicsResponse> out) {
                out.onNext(handler.listTopics(req));
                out.onCompleted();
            }

            @Override
            public void describeTopic(DescribeTopicRequest req, StreamObserver<DescribeTopicResponse> out) {
                out.onNext(handler.describeTopic(req));
                out.onCompleted();
            }
        };
    }
}
