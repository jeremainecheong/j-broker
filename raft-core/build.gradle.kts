plugins {
    id("jbroker.java-conventions")
}

dependencies {
    api(project(":proto"))

    testImplementation("com.tngtech.archunit:archunit-junit5:1.3.0")
}
