load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repo", "versioned_artifacts")

spark_2_4_repo = repo(name = "spark_2_4", provided = True, artifacts = [
    "org.apache.avro:avro:1.8.2",
    "org.apache.curator:apache-curator:2.11.0",
    "org.apache.datasketches:datasketches-java:2.0.0",
    "org.apache.datasketches:datasketches-memory:1.3.0",
    "org.apache.hive:hive-exec:1.2.1",
    versioned_artifacts("2.4.0", [
        "org.apache.spark:spark-streaming_2.11",
        "org.apache.spark:spark-core_2.11:jar:tests",
        "org.apache.spark:spark-hive_2.11",
        "org.apache.spark:spark-sql-kafka-0-10_2.11",
        "org.apache.spark:spark-sql_2.11",
        "org.apache.spark:spark-streaming-kafka-0-10_2.11",
    ]),
    versioned_artifacts("3.7.0-M5", [
        "org.json4s:json4s-ast_2.11",
        "org.json4s:json4s-core_2.11",
        "org.json4s:json4s-jackson_2.11",
    ]),
    "org.apache.hive:hive-metastore:2.3.9",
], excluded_artifacts = ["org.slf4j:slf4j-log4j12", "org.pentaho:pentaho-aggdesigner-algorithm"])
