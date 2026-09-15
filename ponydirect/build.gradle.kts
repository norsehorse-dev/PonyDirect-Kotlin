// PonyDirect (Kotlin) - the sealed P2P transport. A pure Kotlin/JVM library: no
// Android, no Context. It builds and tests on a plain JDK, which is the point -
// the wire crypto and traversal can be read and run without the app, and an app
// that ships it can still go through F-Droid's build server.
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("org.jetbrains.kotlin.jvm")
}

// Coordinates so a consuming app can depend on "com.ponydirect:ponydirect" and, in a
// local composite build (includeBuild), Gradle substitutes this project for it.
group = "com.ponydirect"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

// No kotlin { jvmToolchain(17) } on purpose: F-Droid's buildserver runs with
// Gradle toolchain auto-provisioning disabled, and a jvmToolchain() request that
// doesn't match an installed JDK fails hard there. sourceCompatibility /
// targetCompatibility plus jvmTarget below emit JVM 17 bytecode with no toolchain
// discovery step.
kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

dependencies {
    testImplementation("junit:junit:4.13.2")
}

tasks.test {
    useJUnit()
    testLogging { events("passed", "skipped", "failed") }
}
