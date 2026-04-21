plugins {
    id("jbroker.java-conventions")
    application
}

dependencies {
    implementation(project(":broker-core"))
    implementation(project(":broker-storage"))
    implementation(project(":raft-core"))
    implementation(project(":raft-transport"))
    implementation(project(":proto"))
}

application {
    mainClass.set("jbroker.app.BrokerApp")
}
