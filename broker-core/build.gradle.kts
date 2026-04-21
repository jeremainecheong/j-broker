plugins {
    id("jbroker.java-conventions")
}

dependencies {
    api(project(":proto"))
    api(project(":broker-storage"))
    api(project(":raft-core"))
    implementation(project(":raft-transport"))
}
