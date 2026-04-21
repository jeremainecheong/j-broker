plugins {
    id("jbroker.java-conventions")
}

dependencies {
    testImplementation("net.jqwik:jqwik:1.9.0")
}

tasks.named<Test>("test") {
    useJUnitPlatform {
        includeEngines("junit-jupiter", "jqwik")
    }
}
