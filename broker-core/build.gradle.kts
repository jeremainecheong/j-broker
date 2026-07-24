import java.io.ByteArrayOutputStream
import javax.inject.Inject
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ValueSource
import org.gradle.api.provider.ValueSourceParameters
import org.gradle.process.ExecOperations

plugins {
    id("jbroker.java-conventions")
}

dependencies {
    api(project(":proto"))
    api(project(":broker-storage"))
    api(project(":raft-core"))
    // Raft-transport now exports jbroker.tls.TlsConfig which
    // broker-core re-exports in its public client surfaces. Promote from
    // implementation to api so downstream modules (broker-app, admin-app)
    // see the TLS types transitively.
    api(project(":raft-transport"))

    // Testcontainers backs the Redis quota IT. Scoped to tests
    // so broker-core stays classpath-slim at runtime.
    testImplementation("org.testcontainers:testcontainers:1.20.2")
    testImplementation("org.testcontainers:junit-jupiter:1.20.2")
}

/**
 * Commit id of the checkout being built, or "unknown" when git is
 * unavailable (exported source trees, containers without a .git dir).
 * A ValueSource keeps the external `git` call configuration-cache
 * safe: it is re-evaluated each build and only re-runs the tasks that
 * consume it, instead of invalidating the whole cached configuration.
 */
abstract class GitCommitValueSource : ValueSource<String, GitCommitValueSource.Params> {
    interface Params : ValueSourceParameters {
        val repoDir: DirectoryProperty
    }

    @get:Inject
    abstract val execOperations: ExecOperations

    override fun obtain(): String {
        val stdout = ByteArrayOutputStream()
        return try {
            execOperations.exec {
                workingDir(parameters.repoDir.get().asFile)
                commandLine("git", "rev-parse", "HEAD")
                standardOutput = stdout
                errorOutput = ByteArrayOutputStream()
                isIgnoreExitValue = true
            }
            stdout.toString(Charsets.UTF_8).trim().ifEmpty { "unknown" }
        } catch (e: Exception) {
            "unknown"
        }
    }
}

val gitCommit = providers.of(GitCommitValueSource::class.java) {
    parameters.repoDir.set(rootDir)
}

// Stamps the project version (and commit) into a classpath resource so
// ProtocolVersion.BROKER_VERSION always mirrors the Gradle version.
// Content is deterministic — no timestamps — so the jar is reproducible
// and the task stays up to date until version or commit change.
val generateVersionResource = tasks.register("generateVersionResource") {
    val outputDir = layout.buildDirectory.dir("generated/resources/version")
    val brokerVersion = version.toString()
    // Local copy: referencing the script-level property from doLast would
    // capture the script object, which the configuration cache rejects.
    val commit = gitCommit
    inputs.property("brokerVersion", brokerVersion)
    inputs.property("gitCommit", commit)
    outputs.dir(outputDir)
    doLast {
        val file = outputDir.get().file("jbroker/broker/broker-version.properties").asFile
        file.parentFile.mkdirs()
        file.writeText("version=$brokerVersion\ncommit=${commit.get()}\n")
    }
}

sourceSets {
    named("main") {
        resources.srcDir(generateVersionResource)
    }
}
