plugins {
    `java-library`
    `maven-publish`
}

// Coordinates for the published client surface follow the GitHub Packages
// convention for this repository's owner. Unpublished modules keep the
// internal `jbroker` group from the root build script.
group = "io.github.jeremainecheong"

java {
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set(project.name)
                // Lazy: module build scripts set `description` after this
                // convention plugin has been applied.
                description.set(project.provider { project.description ?: project.name })
                url.set("https://github.com/jeremainecheong/j-broker")
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                scm {
                    url.set("https://github.com/jeremainecheong/j-broker")
                    connection.set("scm:git:https://github.com/jeremainecheong/j-broker.git")
                }
            }
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/jeremainecheong/j-broker")
            credentials {
                // Standard Actions environment; absent on dev machines,
                // which is fine — publishToMavenLocal needs no credentials.
                username = providers.environmentVariable("GITHUB_ACTOR").orNull
                password = providers.environmentVariable("GITHUB_TOKEN").orNull
            }
        }
    }
}
