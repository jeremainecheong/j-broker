plugins {
    id("jbroker.java-conventions")
}

dependencies {
    testImplementation(project(":raft-core"))
    testImplementation(project(":raft-transport"))
    testImplementation(project(":proto"))
    // cross-slice E2E suite and chaos harness need Broker + client.
    testImplementation(project(":broker-app"))
    // jbroker.app.testkit — shared bind-retry cluster starters.
    testImplementation(testFixtures(project(":broker-app")))
    testImplementation(project(":broker-core"))
    testImplementation(project(":broker-storage"))
}

tasks.test {
    useJUnitPlatform {
        excludeTags("stress", "chaos")
    }
}

tasks.register<Test>("stressTest") {
    useJUnitPlatform {
        includeTags("stress")
    }
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
}

// chaos suite — excluded from the default test task so the normal CI
// build stays fast. Invoke locally via `./gradlew :integration-tests:chaosTest`
// (or via scripts/chaos/group-churn.sh which sets a longer duration env var).
tasks.register<Test>("chaosTest") {
    useJUnitPlatform {
        includeTags("chaos")
    }
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    // Long-running chaos ITs can legitimately exceed the default 120s JUnit
    // timeout; extend to 15 minutes here (headroom over the 10-min group-churn).
    systemProperty("junit.jupiter.execution.timeout.default", "900s")
}
