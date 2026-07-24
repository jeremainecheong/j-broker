plugins {
    id("jbroker.java-conventions")
    id("jbroker.publishing-conventions")
}

description = "Segmented log storage engine for j-broker"

dependencies {
    implementation("com.github.luben:zstd-jni:1.5.7-4")
    testImplementation("net.jqwik:jqwik:1.9.0")
}

tasks.named<Test>("test") {
    useJUnitPlatform {
        includeEngines("junit-jupiter", "jqwik")
    }
}
