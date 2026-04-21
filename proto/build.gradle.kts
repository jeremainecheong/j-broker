import com.google.protobuf.gradle.id

plugins {
    id("jbroker.java-conventions")
    id("com.google.protobuf") version "0.9.4"
}

dependencies {
    api("com.google.protobuf:protobuf-java:3.25.5")
    api("io.grpc:grpc-stub:1.65.1")
    api("io.grpc:grpc-protobuf:1.65.1")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
}

// Strip -Werror from compileJava tasks because protoc-generated Java emits
// lint warnings that the java-conventions plugin's -Xlint:all -Werror setup
// would otherwise escalate to errors. The convention plugin still applies
// -Werror to hand-written code in raft-core, raft-transport, etc.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.removeIf { it == "-Werror" }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.5"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.65.1"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc") {}
            }
        }
    }
}
