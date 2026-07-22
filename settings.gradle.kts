pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
    plugins {
        id("io.quarkus") version "3.12.0"
    }
}

rootProject.name = "kafka-scheduler"
