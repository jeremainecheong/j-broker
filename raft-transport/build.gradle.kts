plugins {
    id("jbroker.java-conventions")
}

dependencies {
    api(project(":raft-core"))
    api(project(":proto"))
    api("io.grpc:grpc-netty-shaded:1.65.1")
    api("io.grpc:grpc-stub:1.65.1")
    api("io.grpc:grpc-protobuf:1.65.1")
}
