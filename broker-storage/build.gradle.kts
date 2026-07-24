plugins {
    id("jbroker.java-conventions")
}

dependencies {
    implementation("com.github.luben:zstd-jni:1.5.7-4")
    testImplementation("net.jqwik:jqwik:1.9.0")
}

tasks.named<Test>("test") {
    useJUnitPlatform {
        includeEngines("junit-jupiter", "jqwik")
    }
}
