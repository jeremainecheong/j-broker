plugins {
    id("jbroker.java-conventions")
}

dependencies {
    api(project(":proto"))
    api(project(":broker-storage"))
    api(project(":raft-core"))
    // raft-transport now exports jbroker.tls.TlsConfig which
    // broker-core re-exports in its public client surfaces. Promote from
    // implementation to api so downstream modules (broker-app, admin-app)
    // see the TLS types transitively.
    api(project(":raft-transport"))

    // Testcontainers backs the Redis quota IT. Scoped to tests
    // so broker-core stays classpath-slim at runtime.
    testImplementation("org.testcontainers:testcontainers:1.20.2")
    testImplementation("org.testcontainers:junit-jupiter:1.20.2")
}
