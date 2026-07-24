plugins {
    id("jbroker.java-conventions")
    id("jbroker.publishing-conventions")
}

description = "gRPC transport + TLS wiring for j-broker Raft peers"

dependencies {
    api(project(":raft-core"))
    api(project(":proto"))
    api("io.grpc:grpc-netty-shaded:1.65.1")
    api("io.grpc:grpc-stub:1.65.1")
    api("io.grpc:grpc-protobuf:1.65.1")
}
