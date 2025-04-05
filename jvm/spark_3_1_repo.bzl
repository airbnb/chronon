load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repo", "versioned_artifacts")

spark_3_1_repo = repo(name = "spark_3_1", provided = True, artifacts = [
    "org.apache.avro:avro:1.10.2",
    "org.apache.curator:apache-curator:2.12.0",
    "org.apache.datasketches:datasketches-java:2.0.0",
    "org.apache.datasketches:datasketches-memory:1.3.0",
    "org.apache.hive:hive-exec:2.3.7",
    "org.apache.kafka:kafka_2.12:2.6.3",
    versioned_artifacts("3.1.1", [
        "org.apache.spark:spark-streaming_2.12",
        "org.apache.spark:spark-core_2.12:jar:tests",
        "org.apache.spark:spark-hive_2.12",
        "org.apache.spark:spark-sql-kafka-0-10_2.12",
        "org.apache.spark:spark-sql_2.12",
        "org.apache.spark:spark-streaming-kafka-0-10_2.12",
    ]),
    versioned_artifacts("3.7.0-M5", [
        "org.json4s:json4s-ast_2.12",
        "org.json4s:json4s-core_2.12",
        "org.json4s:json4s-jackson_2.12",
    ]),
    versioned_artifacts("2.10.5", [
        "com.fasterxml.jackson.core:jackson-core",
        "com.fasterxml.jackson.core:jackson-annotations",
        "com.fasterxml.jackson.core:jackson-databind",
        "com.fasterxml.jackson.module:jackson-module-scala_2.12",
        "com.fasterxml.jackson.module:jackson-module-scala_2.13",
    ]),
    "org.apache.hive:hive-metastore:2.3.7",
], excluded_artifacts = [
    "org.slf4j:slf4j-log4j12",
    "org.pentaho:pentaho-aggdesigner-algorithm",
])
