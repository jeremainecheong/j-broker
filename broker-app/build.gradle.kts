plugins {
    id("jbroker.java-conventions")
    `java-test-fixtures`
    application
}

dependencies {
    implementation(project(":broker-core"))
    implementation(project(":broker-storage"))
    implementation(project(":raft-core"))
    implementation(project(":raft-transport"))
    implementation(project(":proto"))
    // Real slf4j backend for the shipped distribution. Without it the
    // broker container runs on slf4j's NOP logger and every fencing /
    // ISR / replication log line is silently dropped — which is exactly
    // what crippled the chaos forensics in #115.
    //
    // Version must stay in lockstep with the conventions plugin's
    // testRuntimeOnly logback (currently 1.5.18): admin-app consumes this
    // project in its Spring Boot tests, and a second logback version in
    // the graph produces a classic/core skew that breaks pattern parsing
    // ("no conversion class registered for [d]").
    //
    // The config is deliberately named jbroker-logback.xml, NOT
    // logback.xml: a classpath-root logback.xml is auto-discovered by
    // every consumer of this project (Spring Boot admin-app tests
    // included) and hijacks their logging. The start scripts activate it
    // via applicationDefaultJvmArgs below; tests and consumers never
    // load it.
    runtimeOnly("ch.qos.logback:logback-classic:1.5.18")
    // jbroker.app.testkit — bind-retry cluster starters shared with
    // integration-tests (freePort()->bind TOCTOU hardening, see #97).
    testFixturesApi(project(":raft-core"))
}

application {
    mainClass.set("jbroker.app.BrokerApp")
    // Activates the distribution's logging config (a classpath resource in
    // this project's jar). Baked into the generated start scripts, so the
    // Docker image and bare-dist users get it with no extra wiring, while
    // gradle test JVMs (which don't go through the start scripts) are
    // untouched.
    applicationDefaultJvmArgs = listOf("-Dlogback.configurationFile=jbroker-logback.xml")
}
