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
import jbroker.proto.broker.DeleteTopicRequest;
import jbroker.proto.broker.DeleteTopicResponse;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.broker.DescribeConsumerGroupRequest;
import jbroker.proto.broker.DescribeConsumerGroupResponse;
import jbroker.proto.broker.DescribeMetricsRequest;
import jbroker.proto.broker.DescribeMetricsResponse;
import jbroker.proto.broker.DescribeRaftRequest;
import jbroker.proto.broker.DescribeRaftResponse;
import jbroker.proto.broker.DescribeTopicPartitionsRequest;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.DescribeTopicRequest;
import jbroker.proto.broker.DescribeTopicResponse;
import jbroker.proto.broker.EventMessage;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.FindCoordinatorResponse;
import jbroker.proto.broker.InitProducerIdRequest;
import jbroker.proto.broker.InitProducerIdResponse;
import jbroker.proto.broker.ListConsumerGroupsRequest;
import jbroker.proto.broker.ListConsumerGroupsResponse;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ListTopicsResponse;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.broker.OffsetsForLeaderEpochRequest;
import jbroker.proto.broker.OffsetsForLeaderEpochResponse;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.proto.broker.ReplicaConsumerGrpc;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;
import jbroker.proto.broker.SubscribeEventsRequest;
import jbroker.proto.broker.UpdateTopicConfigRequest;
import jbroker.proto.broker.UpdateTopicConfigResponse;

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
     * Back-compat overload used by older tests that don't yet wire the
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

    public static MetadataGrpc.MetadataImplBase metadata(MetadataServiceHandler handler) {
        return new MetadataGrpc.MetadataImplBase() {
            @Override
            public void describeCluster(DescribeClusterRequest req, StreamObserver<DescribeClusterResponse> out) {
                out.onNext(handler.describeCluster(req));
                out.onCompleted();
            }

            @Override
            public void describeTopicPartitions(
                    DescribeTopicPartitionsRequest req, StreamObserver<DescribeTopicPartitionsResponse> out) {
                out.onNext(handler.describeTopicPartitions(req));
                out.onCompleted();
            }

            @Override
            public void listConsumerGroups(
                    ListConsumerGroupsRequest req, StreamObserver<ListConsumerGroupsResponse> out) {
                out.onNext(handler.listConsumerGroups(req));
                out.onCompleted();
            }

            @Override
            public void describeConsumerGroup(
                    DescribeConsumerGroupRequest req, StreamObserver<DescribeConsumerGroupResponse> out) {
                out.onNext(handler.describeConsumerGroup(req));
                out.onCompleted();
            }

            @Override
            public void describeRaft(DescribeRaftRequest req, StreamObserver<DescribeRaftResponse> out) {
                out.onNext(handler.describeRaft(req));
                out.onCompleted();
            }

            @Override
            public void describeMetrics(DescribeMetricsRequest req, StreamObserver<DescribeMetricsResponse> out) {
                out.onNext(handler.describeMetrics(req));
                out.onCompleted();
            }

            @Override
            public void subscribeEvents(SubscribeEventsRequest req, StreamObserver<EventMessage> out) {
                var publisher = handler.eventPublisher();
                if (publisher == null) {
                    out.onCompleted();
                    return;
                }
                // Subscribe FIRST so nothing published between the replay
                // walk and the pump thread's first take() slips through. Then
                // replay the ring tail under after_id to cover events that
                // landed while the subscriber was setting up.
                var sub = publisher.subscribe();
                try {
                    for (var e : publisher.replayAfter(req.getAfterId())) {
                        out.onNext(toMessage(e));
                    }
                } catch (Exception replayErr) {
                    sub.close();
                    out.onError(replayErr);
                    return;
                }
                Thread.ofVirtual().name("broker-event-stream").start(() -> {
                    try {
                        while (true) {
                            var event = sub.take();
                            if (event == null) break;
                            synchronized (out) {
                                out.onNext(toMessage(event));
                            }
                        }
                        synchronized (out) {
                            out.onCompleted();
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        synchronized (out) {
                            out.onError(e);
                        }
                    } finally {
                        sub.close();
                    }
                });
            }
        };
    }

    private static EventMessage toMessage(jbroker.broker.events.BrokerEvent e) {
        return EventMessage.newBuilder()
                .setId(e.id())
                .setType(e.typeName())
                .setDataJson(jbroker.broker.events.BrokerEventJson.encode(e))
                .build();
    }

    public static AdminGrpc.AdminImplBase admin(AdminHandler handler) {
        return admin(handler, null);
    }

    /**
     * Admin service with optional consumer-group mutation support.
     * The {@code consumer} parameter may be null for single-topic test
     * harnesses; in production Broker.start always passes a wired
     * ConsumerHandler so the new delete/reset RPCs route correctly.
     */
    public static AdminGrpc.AdminImplBase admin(AdminHandler handler, ConsumerHandler consumer) {
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

            @Override
            public void deleteTopic(DeleteTopicRequest req, StreamObserver<DeleteTopicResponse> out) {
                out.onNext(handler.deleteTopic(req));
                out.onCompleted();
            }

            @Override
            public void updateTopicConfig(UpdateTopicConfigRequest req, StreamObserver<UpdateTopicConfigResponse> out) {
                out.onNext(handler.updateTopicConfig(req));
                out.onCompleted();
            }

            @Override
            public void createAcl(
                    jbroker.proto.broker.CreateAclRequest req,
                    StreamObserver<jbroker.proto.broker.CreateAclResponse> out) {
                out.onNext(handler.createAcl(req));
                out.onCompleted();
            }

            @Override
            public void deleteAcl(
                    jbroker.proto.broker.DeleteAclRequest req,
                    StreamObserver<jbroker.proto.broker.DeleteAclResponse> out) {
                out.onNext(handler.deleteAcl(req));
                out.onCompleted();
            }

            @Override
            public void listAcls(
                    jbroker.proto.broker.ListAclsRequest req,
                    StreamObserver<jbroker.proto.broker.ListAclsResponse> out) {
                out.onNext(handler.listAcls(req));
                out.onCompleted();
            }

            @Override
            public void forceCompactPartition(
                    jbroker.proto.broker.ForceCompactPartitionRequest req,
                    StreamObserver<jbroker.proto.broker.ForceCompactPartitionResponse> out) {
                out.onNext(handler.forceCompactPartition(req));
                out.onCompleted();
            }

            @Override
            public void deleteConsumerGroup(
                    jbroker.proto.broker.DeleteConsumerGroupRequest req,
                    StreamObserver<jbroker.proto.broker.DeleteConsumerGroupResponse> out) {
                var b = jbroker.proto.broker.DeleteConsumerGroupResponse.newBuilder();
                if (consumer == null) {
                    b.setError(jbroker.proto.common.ErrorCode.UNIMPLEMENTED);
                } else {
                    b.setError(consumer.deleteConsumerGroupAdmin(req.getGroupId()));
                }
                out.onNext(b.build());
                out.onCompleted();
            }

            @Override
            public void resetConsumerGroupOffsets(
                    jbroker.proto.broker.ResetConsumerGroupOffsetsRequest req,
                    StreamObserver<jbroker.proto.broker.ResetConsumerGroupOffsetsResponse> out) {
                var b = jbroker.proto.broker.ResetConsumerGroupOffsetsResponse.newBuilder();
                if (consumer == null) {
                    b.setError(jbroker.proto.common.ErrorCode.UNIMPLEMENTED);
                    for (var r : req.getResetsList()) {
                        b.addResults(jbroker.proto.broker.OffsetResetResult.newBuilder()
                                .setTp(r.getTp())
                                .setError(jbroker.proto.common.ErrorCode.UNIMPLEMENTED)
                                .build());
                    }
                    out.onNext(b.build());
                    out.onCompleted();
                    return;
                }
                var topLevel = new jbroker.proto.common.ErrorCode[1];
                topLevel[0] = jbroker.proto.common.ErrorCode.OK;
                java.util.List<jbroker.proto.common.ErrorCode> results;
                try {
                    results = consumer.resetConsumerGroupOffsetsAdmin(req.getGroupId(), req.getResetsList(), topLevel);
                } catch (java.io.IOException ioe) {
                    results =
                            java.util.Collections.nCopies(req.getResetsCount(), jbroker.proto.common.ErrorCode.UNKNOWN);
                }
                b.setError(topLevel[0]);
                for (int i = 0; i < req.getResetsCount(); i++) {
                    b.addResults(jbroker.proto.broker.OffsetResetResult.newBuilder()
                            .setTp(req.getResets(i).getTp())
                            .setError(results.get(i))
                            .build());
                }
                out.onNext(b.build());
                out.onCompleted();
            }
        };
    }
}
