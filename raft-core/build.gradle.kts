plugins {
    id("jbroker.java-conventions")
    id("jbroker.publishing-conventions")
}

description = "Raft consensus core used for j-broker metadata"

dependencies {
    api(project(":proto"))

    testImplementation("com.tngtech.archunit:archunit-junit5:1.3.0")
}
