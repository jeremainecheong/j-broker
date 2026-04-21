package jbroker.broker;

import io.grpc.stub.StreamObserver;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.CreateTopicResponse;
import jbroker.proto.broker.DescribeTopicRequest;
import jbroker.proto.broker.DescribeTopicResponse;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ListTopicsResponse;
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

    public static ProducerGrpc.ProducerImplBase producer(ProduceHandler handler) {
        return new ProducerGrpc.ProducerImplBase() {
            @Override
            public void produce(ProduceRequest req, StreamObserver<ProduceResponse> out) {
                out.onNext(handler.handle(req));
                out.onCompleted();
            }
        };
    }

    public static ConsumerGrpc.ConsumerImplBase consumer(FetchHandler handler) {
        return new ConsumerGrpc.ConsumerImplBase() {
            @Override
            public void fetch(FetchRequest req, StreamObserver<FetchResponse> out) {
                out.onNext(handler.handle(req));
                out.onCompleted();
            }
        };
    }

    public static ReplicaConsumerGrpc.ReplicaConsumerImplBase replicaConsumer(ReplicaFetchHandler handler) {
        return new ReplicaConsumerGrpc.ReplicaConsumerImplBase() {
            @Override
            public void replicaFetch(ReplicaFetchRequest req, StreamObserver<ReplicaFetchResponse> out) {
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
