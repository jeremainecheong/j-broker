plugins {
    `java-library`
    id("com.diffplug.spotless")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
        vendor.set(JvmVendorSpec.ADOPTIUM)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror", "-parameters"))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform {
        // Hardware-dependent throughput micro-benchmarks are tagged
        // `perf` and excluded from default `./gradlew test`. They still
        // run under `-PincludeTags=perf` or env JBROKER_RUN_PERF=1.
        val runPerf = project.findProperty("includeTags")?.toString()?.contains("perf") == true
                || System.getenv("JBROKER_RUN_PERF") == "1"
        if (!runPerf) {
            excludeTags("perf")
        }
        // /Testcontainers-backed ITs + 1M-record scale tests
        // are tagged `slow`. Default CI excludes them; opt-in via
        // `-PincludeTags=slow` or env JBROKER_RUN_SLOW_TESTS=1.
        val runSlow = project.findProperty("includeTags")?.toString()?.contains("slow") == true
                || System.getenv("JBROKER_RUN_SLOW_TESTS") == "1"
        if (!runSlow) {
            excludeTags("slow")
        }
    }
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }
}

dependencies {
    "implementation"("org.slf4j:slf4j-api:2.0.13")

    "testImplementation"(platform("org.junit:junit-bom:5.10.3"))
    "testImplementation"("org.junit.jupiter:junit-jupiter")
    "testImplementation"("org.assertj:assertj-core:3.26.3")
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    "testRuntimeOnly"("ch.qos.logback:logback-classic:1.5.18")
}

spotless {
    java {
        palantirJavaFormat("2.50.0")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
        target("src/**/*.java")
    }
}
