plugins {
    id("jbroker.java-conventions")
}

dependencies {
    testImplementation(project(":raft-core"))
    testImplementation(project(":raft-transport"))
    testImplementation(project(":proto"))
}

tasks.test {
    useJUnitPlatform {
        excludeTags("stress")
    }
}

tasks.register<Test>("stressTest") {
    useJUnitPlatform {
        includeTags("stress")
    }
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
}
