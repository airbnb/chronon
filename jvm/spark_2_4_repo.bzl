load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repo", "versioned_artifacts")

spark_2_4_repo = repo(name = "spark_2_4", provided = True, artifacts = [
    "com.esotericsoftware:kryo:5.1.1",
    "com.esotericsoftware.kryo:kryo:2.21",
    versioned_artifacts("2.6.7", [
        "com.fasterxml.jackson.core:jackson-annotations",
        "com.fasterxml.jackson.core:jackson-core",
        "com.fasterxml.jackson.core:jackson-databind",
    ]),
    "com.google.guava:guava:14.0.1",
    versioned_artifacts("0.12.0", [
        "com.yahoo.datasketches:memory",
        "com.yahoo.datasketches:sketches-core",
    ]),
    "commons-codec:commons-codec:1.10",
    "io.netty:netty-all:4.1.74.Final",
    "org.apache.avro:avro:1.8.2",
    # Hive
    "org.apache.curator:apache-curator:2.6.0",
    "org.apache.datasketches:datasketches-java:2.0.0",
    "org.apache.datasketches:datasketches-memory:1.3.0",
    "org.apache.kafka:kafka_2.11:0.8.2.2",
    versioned_artifacts("2.17.1", [
        "org.apache.logging.log4j:log4j-api",
        "org.apache.logging.log4j:log4j-core",
        "org.apache.logging.log4j:log4j-slf4j-impl",
        "org.apache.logging.log4j:log4j-web",
    ]),
    "org.apache.logging.log4j:log4j-1.2-api:2.12.4",
    versioned_artifacts("2.4.0", [
        "org.apache.spark:spark-avro_2.11",
        "org.apache.spark:spark-catalyst_2.11",
        "org.apache.spark:spark-core_2.11",
        "org.apache.spark:spark-hive_2.11",
        "org.apache.spark:spark-mllib-local_2.11",
        "org.apache.spark:spark-mllib_2.11",
        "org.apache.spark:spark-sql-kafka-0-10_2.11",
        "org.apache.spark:spark-sql_2.11",
        "org.apache.spark:spark-streaming_2.11",
        "org.apache.spark:spark-tags_2.11",
        "org.apache.spark:spark-unsafe_2.11",
        "org.apache.spark:spark-streaming-kafka-0-10_2.11",
    ]),
    versioned_artifacts("3.5.3", [
        "org.json4s:json4s-ast_2.11",
        "org.json4s:json4s-core_2.11",
        "org.json4s:json4s-jackson_2.11",
    ]),
    "org.apache.hive:hive-metastore:2.3.9",
    "org.apache.hive:hive-exec:2.3.9",
], excluded_artifacts = ["org.pentaho:pentaho-aggdesigner-algorithm"])
