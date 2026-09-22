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
    maxHeapSize = "3g"
    forkEvery = 1
}

tasks.register<Exec>("cleanTestDatabases") {
    description = "Cleans all test databases before running integration tests"
    group = "verification"
    workingDir = projectDir
    commandLine("bash", "scripts/clean-test-databases.sh")
    isIgnoreExitValue = true
}

tasks.register<Exec>("stopSchedulerContainers") {
    description = "Stops scheduler containers that might interfere with tests"
    group = "verification"
    commandLine("bash", "-c", "docker stop scheduler-postgres scheduler-mysql scheduler-cockroach 2>/dev/null || true")
    isIgnoreExitValue = true
}

tasks.register<Test>("postgresTest") {
    description = "Runs PostgreSQL integration tests"
    group = "verification"
    dependsOn("stopSchedulerContainers", "cleanTestDatabases")
    useJUnitPlatform {
        includeTags("postgres")
    }
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    maxHeapSize = "2g"
    forkEvery = 1
    reports.html.outputLocation.set(layout.buildDirectory.dir("reports/tests/postgres"))
    reports.junitXml.outputLocation.set(layout.buildDirectory.dir("test-results/postgres"))
    binaryResultsDirectory.set(layout.buildDirectory.dir("test-results/postgres/binary"))
}

tasks.register<Test>("mysqlTest") {
    description = "Runs MySQL integration tests"
    group = "verification"
    dependsOn("stopSchedulerContainers", "cleanTestDatabases")
    useJUnitPlatform {
        includeTags("mysql")
    }
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    maxHeapSize = "2g"
    forkEvery = 1
    reports.html.outputLocation.set(layout.buildDirectory.dir("reports/tests/mysql"))
    reports.junitXml.outputLocation.set(layout.buildDirectory.dir("test-results/mysql"))
    binaryResultsDirectory.set(layout.buildDirectory.dir("test-results/mysql/binary"))
}

tasks.register<Test>("cockroachTest") {
    description = "Runs CockroachDB integration tests"
    group = "verification"
    dependsOn("stopSchedulerContainers", "cleanTestDatabases")
    useJUnitPlatform {
        includeTags("cockroachdb")
    }
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    maxHeapSize = "2g"
    forkEvery = 1
    reports.html.outputLocation.set(layout.buildDirectory.dir("reports/tests/cockroachdb"))
    reports.junitXml.outputLocation.set(layout.buildDirectory.dir("test-results/cockroachdb"))
    binaryResultsDirectory.set(layout.buildDirectory.dir("test-results/cockroachdb/binary"))
}

tasks.register("parallelDatabaseTest") {
    description = "Runs all database tests in parallel (use: ./gradlew parallelDatabaseTest --parallel)"
    group = "verification"
    dependsOn("postgresTest", "mysqlTest", "cockroachTest")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}
