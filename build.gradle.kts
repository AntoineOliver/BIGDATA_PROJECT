plugins {
    scala
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.12.18")
    compileOnly("org.apache.spark:spark-core_2.12:3.5.1")
    compileOnly("org.apache.spark:spark-sql_2.12:3.5.1")
    compileOnly("org.apache.hadoop:hadoop-aws:3.3.4")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.3")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

application {
    mainClass.set("job.SecondAnalysis")
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    archiveBaseName.set("BIGDATA_PROJECT")
    archiveVersion.set("")
}
