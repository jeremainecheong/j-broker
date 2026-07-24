allprojects {
    group = "jbroker"
    // Everyday builds carry the rolling snapshot. Release builds pass
    // -PreleaseVersion=<x.y.z>, derived from the git tag by
    // .github/workflows/release.yml, so jars, images, and the stamped
    // broker-version resource all pick up the tag's version. A dedicated
    // property (rather than -Pversion) because this assignment would
    // silently clobber a plain -Pversion override.
    version = providers.gradleProperty("releaseVersion").getOrElse("2.0.0-SNAPSHOT")

    repositories {
        mavenCentral()
    }
}
