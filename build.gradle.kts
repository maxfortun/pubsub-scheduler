plugins {
    java
    id("io.quarkus") version "3.12.0"
}

repositories {
    mavenCentral()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:quarkus-camel-bom:${quarkusPlatformVersion}"))

    // Quarkus core
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-scheduler")

    // Camel core
    implementation("org.apache.camel.quarkus:camel-quarkus-core")
    implementation("org.apache.camel.quarkus:camel-quarkus-xml-io-dsl")
    implementation("org.apache.camel.quarkus:camel-quarkus-direct")
    implementation("org.apache.camel.quarkus:camel-quarkus-bean")
    implementation("org.apache.camel.quarkus:camel-quarkus-log")

    // Camel messaging (include what you need)
    implementation("org.apache.camel.quarkus:camel-quarkus-kafka")
    implementation("org.apache.camel.quarkus:camel-quarkus-activemq")

    // Database
    implementation("io.quarkus:quarkus-jdbc-postgresql")
    implementation("io.quarkus:quarkus-agroal")

    // Secret providers (optional - include what you need)
    implementation("io.quarkiverse.amazonservices:quarkus-amazon-secretsmanager:2.12.0")
    implementation("io.quarkiverse.amazonservices:quarkus-amazon-ssm:2.12.0")
    implementation("software.amazon.awssdk:url-connection-client:2.25.0")
    implementation("io.quarkiverse.vault:quarkus-vault:4.0.0")

    // REST API
    implementation("io.quarkus:quarkus-rest")
    implementation("io.quarkus:quarkus-rest-jackson")

    // Health & metrics
    implementation("io.quarkus:quarkus-smallrye-health")
    implementation("io.quarkus:quarkus-micrometer-registry-prometheus")

    // OpenAPI / Swagger
    implementation("io.quarkus:quarkus-smallrye-openapi")

    // Testing
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.quarkus:quarkus-junit5-mockito")
    testImplementation("org.apache.camel.quarkus:camel-quarkus-junit5")
    testImplementation("io.rest-assured:rest-assured")
    testImplementation("io.quarkus:quarkus-jdbc-h2")
    testImplementation("io.quarkus:quarkus-jdbc-mysql")
}

group = "net.maxf"
version = "1.0.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    maxHeapSize = "2g"
    jvmArgs("-XX:+UseG1GC")
}

tasks.test {
    useJUnitPlatform {
        excludeTags("docker", "database")
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs Docker integration tests"
    group = "verification"
    useJUnitPlatform {
        includeTags("docker")
    }
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}

tasks.register<Test>("databaseTest") {
    description = "Runs database-specific tests (requires running DB containers)"
    group = "verification"
    useJUnitPlatform {
        includeTags("database")
    }
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    // Database tests spawn multiple Quarkus instances - need more memory and fresh JVMs
    maxHeapSize = "3g"
    forkEvery = 1
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}
