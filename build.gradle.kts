// Root build for the standalone PonyDirect-Kotlin repo. The :ponydirect module
// applies kotlin("jvm"); this makes the plugin available and points every project
// at Maven Central. No Android plugin here - the transport is pure JVM (java.net
// UDP sockets), and the app supplies the mDNS discovery (NsdManager) behind an
// interface, so this builds and tests on a plain JDK.
plugins {
    id("org.jetbrains.kotlin.jvm") version "2.1.0" apply false
}
allprojects {
    repositories {
        mavenCentral()
    }
}
