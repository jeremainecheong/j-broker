plugins {
    id("jbroker.java-conventions")
    id("org.springframework.boot") version "3.3.5"
    id("io.spring.dependency-management") version "1.1.6"
    application
}

dependencies {
    implementation(project(":broker-core"))
    implementation(project(":proto"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-thymeleaf")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")

    // Broker-app is only used by tests: ITs boot an in-process Broker to
    // point the admin-app at. Production deployments run admin-app + broker
    // as separate JVMs, so this stays test-scoped deliberately.
    testImplementation(project(":broker-app"))
    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        // Spring Boot's starter-test pulls spring-boot-starter-logging (Logback
        // + Log4j bridges). The convention plugin already contributes Logback
        // to testRuntime, so dedup rather than fight the BOMs.
    }
}

application {
    mainClass.set("jbroker.admin.AdminApp")
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveFileName.set("admin-app.jar")
}
