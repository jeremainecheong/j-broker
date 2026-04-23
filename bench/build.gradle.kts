plugins {
    id("jbroker.java-conventions")
    application
}

dependencies {
    implementation(project(":broker-core"))
    implementation(project(":broker-storage"))
    implementation(project(":raft-core"))
    implementation(project(":raft-transport"))
    implementation(project(":broker-app"))
    implementation(project(":proto"))

    // HdrHistogram for microsecond-precision latency percentiles
    // (p50/p99/p999) across the produce + fetch paths. The admin metrics
    // endpoint exposes a cruder 3-bucket snapshot via Micrometer; this
    // module is for deliberate one-off perf runs where the operator wants
    // the full distribution.
    implementation("org.hdrhistogram:HdrHistogram:2.2.2")
}

application {
    // Single entry point — dispatches on the first argument
    // (producer/consumer). See PerfMain for supported flags.
    mainClass.set("jbroker.bench.PerfMain")
}
