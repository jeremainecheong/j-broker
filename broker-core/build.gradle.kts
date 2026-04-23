plugins {
    id("jbroker.java-conventions")
}

dependencies {
    api(project(":proto"))
    api(project(":broker-storage"))
    api(project(":raft-core"))
    implementation(project(":raft-transport"))

    // Testcontainers backs the Redis quota IT. Scoped to tests
    // so broker-core stays classpath-slim at runtime.
    testImplementation("org.testcontainers:testcontainers:1.20.2")
    testImplementation("org.testcontainers:junit-jupiter:1.20.2")
}
