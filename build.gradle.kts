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
    implementation("io.quarkus:quarkus-jdbc-mysql")
    implementation("io.quarkus:quarkus-agroal")

    // Secret providers (optional - include what you need)
    implementation("io.quarkiverse.amazonservices:quarkus-amazon-secretsmanager:2.12.0")
    implementation("io.quarkiverse.amazonservices:quarkus-amazon-ssm:2.12.0")
    implementation("software.amazon.awssdk:url-connection-client:2.25.0")
    implementation("io.quarkiverse.vault:quarkus-vault:4.0.0")

    // REST API
    implementation("io.quarkus:quarkus-rest")
    implementation("io.quarkus:quarkus-rest-jackson")

    // Cron expression parsing
    implementation("com.cronutils:cron-utils:9.2.1")

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
    useJUnitPlatform()
}

tasks.register<Exec>("cleanTestDatabases") {
    description = "Cleans all test databases and Kafka topics"
    group = "verification"
    workingDir = projectDir
    commandLine("bash", "scripts/clean-test-databases.sh")
    isIgnoreExitValue = true
}

tasks.register<Exec>("integrationTest") {
    description = "Runs containerized integration tests (requires ./scripts/start-test-infra.sh first)"
    group = "verification"
    workingDir = projectDir
    commandLine("bash", "scripts/run-integration-tests.sh")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}
