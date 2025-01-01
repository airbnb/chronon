load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repo", "versioned_artifacts")

spark_3_1_repo = repo(name = "spark_3_1", provided = True, artifacts = [
    "org.apache.avro:avro:1.10.2",
    "org.apache.curator:apache-curator:2.12.0",
    "org.apache.datasketches:datasketches-java:2.0.0",
    "org.apache.datasketches:datasketches-memory:1.3.0",
    "org.apache.hive:hive-exec:3.1.2",
    "org.apache.kafka:kafka_2.12:2.6.3",
    versioned_artifacts("3.1.3", [
        "org.apache.spark:spark-core_2.12:jar:tests",
        "org.apache.spark:spark-hive_2.12",
        "org.apache.spark:spark-mllib_2.12",
        "org.apache.spark:spark-sql-kafka-0-10_2.12",
        "org.apache.spark:spark-sql_2.12",
        "org.apache.spark:spark-streaming-kafka-0-10_2.12",
    ]),
    "org.elasticsearch:elasticsearch-hadoop:6.4.2",
    versioned_artifacts("3.7.0-M5", [
        "org.json4s:json4s-ast_2.12",
        "org.json4s:json4s-core_2.12",
        "org.json4s:json4s-jackson_2.12",
    ]),
    "org.apache.hive:hive-metastore:2.3.9",
], excluded_artifacts = ["org.slf4j:slf4j-log4j12"])
