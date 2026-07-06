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
    // jbroker.app.testkit — bind-retry cluster starters shared with
    // integration-tests (freePort()->bind TOCTOU hardening, see #97).
    testFixturesApi(project(":raft-core"))
}

application {
    mainClass.set("jbroker.app.BrokerApp")
}
