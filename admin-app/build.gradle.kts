plugins {
    id("jbroker.java-conventions")
    id("org.springframework.boot") version "3.3.5"
    id("io.spring.dependency-management") version "1.1.6"
    application
}

dependencies {
    implementation(project(":broker-core"))
    implementation(project(":proto"))
    // P15.2 — jbroker.tls.TlsConfig / TlsContexts live in raft-transport;
    // admin-app now builds gRPC channels with mTLS support, so pull it in
    // directly (broker-core's dep is implementation-scoped).
    implementation(project(":raft-transport"))
    // gRPC shaded netty channel builder. Not exported transitively by
    // broker-core (only raft-transport apis it), so bring it in directly
    // here so admin-app client code can dial brokers.
    implementation("io.grpc:grpc-netty-shaded:1.65.1")

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-thymeleaf")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    // P9.1 — Prometheus scrape endpoint at /actuator/prometheus. Auto-wires
    // a PrometheusMeterRegistry bean; PrometheusMetricsBinder reads from
    // MetricsScraper and emits jbroker_* meters.
    implementation("io.micrometer:micrometer-registry-prometheus")

    // Broker-app is only used by tests: ITs boot an in-process Broker to
    // point the admin-app at. Production deployments run admin-app + broker
    // as separate JVMs, so this stays test-scoped deliberately.
    testImplementation(project(":broker-app"))
    // P13.7 — Testcontainers backs the Redis pub/sub fanout IT. Scoped
    // to tests so admin-app stays classpath-slim at runtime.
    testImplementation("org.testcontainers:testcontainers:1.20.2")
    testImplementation("org.testcontainers:junit-jupiter:1.20.2")
    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        // Spring Boot's starter-test pulls spring-boot-starter-logging (Logback
        // + Log4j bridges). The convention plugin already contributes Logback
        // to testRuntime, so dedup rather than fight the BOMs.
    }
}

// Spring Boot's starter classpath exposes legacy @Deprecated types and raw
// generics that -Xlint:all + -Werror (applied by the convention plugin)
// would escalate to compile errors as soon as controllers reference them.
// Strip -Werror here — the convention plugin's other lints still run, and
// hand-written code remains warning-free. Mirrors proto/build.gradle.kts.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.removeIf { it == "-Werror" }
}

application {
    mainClass.set("jbroker.admin.AdminApp")
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveFileName.set("admin-app.jar")
}
