plugins {
    id("jbroker.java-conventions")
}

dependencies {
    testImplementation(project(":raft-core"))
    testImplementation(project(":raft-transport"))
    testImplementation(project(":proto"))
}

// Integration tests run slower; isolate in their own task later if needed.
