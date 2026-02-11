plugins {
    java
    application
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    mainClass.set("com.example.segmentation.CustomerSegmentationApp")
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

val kafkaVersion: String by project
val jacksonVersion: String by project
val junitVersion: String by project
val avroVersion: String by project
val confluentVersion: String by project
val testcontainersVersion: String by project

avro {
    stringType.set("String")
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("org.apache.avro:avro:$avroVersion")
    implementation("io.confluent:kafka-streams-avro-serde:$confluentVersion")
    implementation("org.slf4j:slf4j-api:2.0.13")
    implementation("io.github.cdimascio:dotenv-java:3.2.0")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.29")

    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.7")
    testImplementation("org.testcontainers:kafka:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    testImplementation("org.awaitility:awaitility:4.2.2")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<JavaExec>("runKafkaProducer") {
    description = "Run the sample Order producer"
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.example.segmentation.SampleOrderProducer")
}
