load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repo", "versioned_artifacts")

spark_2_4_repo = repo(name = "spark_2_4", artifacts = [
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
    versioned_artifacts("3.5.2", [
        "org.json4s:json4s-ast_2.11",
        "org.json4s:json4s-core_2.11",
        "org.json4s:json4s-jackson_2.11",
    ]),
    "org.apache.hive:hive-metastore:2.3.9",
], excluded_artifacts = ["org.slf4j:slf4j-log4j12", "org.pentaho:pentaho-aggdesigner-algorithm"])

spark_3_1_repo = repo(name = "spark_3_1", artifacts = [
    "org.apache.avro:avro:1.10.2",
    "org.apache.curator:apache-curator:2.12.0",
    "org.apache.datasketches:datasketches-java:2.0.0",
    "org.apache.datasketches:datasketches-memory:1.3.0",
    "org.apache.hive:hive-exec:3.1.2",
    "org.apache.kafka:kafka_2.12:2.6.3",
    versioned_artifacts("3.1.1", [
        "org.apache.spark:spark-streaming_2.12",
        "org.apache.spark:spark-core_2.12:jar:tests",
        "org.apache.spark:spark-hive_2.12",
        "org.apache.spark:spark-sql-kafka-0-10_2.12",
        "org.apache.spark:spark-sql_2.12",
        "org.apache.spark:spark-streaming-kafka-0-10_2.12",
    ]),
    versioned_artifacts("3.5.2", [
        "org.json4s:json4s-ast_2.12",
        "org.json4s:json4s-core_2.12",
        "org.json4s:json4s-jackson_2.12",
    ]),
    "org.apache.hive:hive-metastore:2.3.9",
    "io.delta:delta-core_2.12:2.0.2",
], excluded_artifacts = ["org.slf4j:slf4j-log4j12"])

spark_3_2_repo = repo(
    name = "spark_3_2",
    vars = {
        "spark_version": "3.2.1",
        "hadoop_version": "3.3.6",
    },
    artifacts = [
        # Spark artifacts - only Scala 2.12 since that's our target
        "org.apache.spark:spark-sql_2.12:{spark_version}",
        "org.apache.spark:spark-hive_2.12:{spark_version}",
        "org.apache.spark:spark-streaming_2.12:{spark_version}",

        # Spark artifacts for Scala 2.13
        "org.apache.spark:spark-sql_2.13:{spark_version}",
        "org.apache.spark:spark-hive_2.13:{spark_version}",
        "org.apache.spark:spark-streaming_2.13:{spark_version}",

        # Other dependencies
        "org.apache.curator:apache-curator:2.12.0",
        "com.esotericsoftware:kryo:5.1.1",
        "com.yahoo.datasketches:sketches-core:0.13.4",
        "com.yahoo.datasketches:memory:0.12.2",
        "com.yahoo.datasketches:sketches-hive:0.13.0",
        "org.apache.datasketches:datasketches-java:2.0.0",
        "org.apache.datasketches:datasketches-memory:1.3.0",

        # Kafka dependencies - only Scala 2.12
        "org.apache.kafka:kafka_2.12:2.6.3",

        # Avro dependencies
        "org.apache.avro:avro:1.8.2",
        "org.apache.avro:avro-mapred:1.8.2",
        "org.apache.hive:hive-metastore:2.3.9",
        "org.apache.hive:hive-exec:3.1.2",

        # Monitoring
        "io.prometheus.jmx:jmx_prometheus_javaagent:0.20.0",
        "io.delta:delta-core_2.12:2.0.2",
    ],
    excluded_artifacts = [
        "org.pentaho:pentaho-aggdesigner-algorithm",
    ],
)

spark_3_5_repo = repo(
    name = "spark_3_5",
    vars = {
        "spark_version": "3.5.4",
        "hadoop_version": "3.3.6",
    },
    artifacts = [
        # Spark artifacts - for scala 2.12
        "org.apache.spark:spark-sql_2.12:{spark_version}",
        "org.apache.spark:spark-hive_2.12:{spark_version}",
        "org.apache.spark:spark-streaming_2.12:{spark_version}",

        # Spark artifacts for Scala 2.13
        "org.apache.spark:spark-sql_2.13:{spark_version}",
        "org.apache.spark:spark-hive_2.13:{spark_version}",
        "org.apache.spark:spark-streaming_2.13:{spark_version}",

        # Other dependencies
        "org.apache.curator:apache-curator:2.12.0",
        "com.esotericsoftware:kryo:5.1.1",
        "com.yahoo.datasketches:sketches-core:0.13.4",
        "com.yahoo.datasketches:memory:0.12.2",
        "com.yahoo.datasketches:sketches-hive:0.13.0",
        "org.apache.datasketches:datasketches-java:2.0.0",
        "org.apache.datasketches:datasketches-memory:1.3.0",

        # Kafka dependencies - only Scala 2.12
        "org.apache.kafka:kafka_2.12:2.6.3",

        # Avro dependencies
        "org.apache.avro:avro:1.8.2",
        "org.apache.avro:avro-mapred:1.8.2",
        "org.apache.hive:hive-metastore:2.3.9",
        "org.apache.hive:hive-exec:3.1.2",

        # Monitoring
        "io.prometheus.jmx:jmx_prometheus_javaagent:0.20.0",
        "io.delta:delta-core_2.12:2.0.2",
        "io.delta:delta-core_2.13:2.0.2",
    ],
    excluded_artifacts = [
        "org.pentaho:pentaho-aggdesigner-algorithm",
    ],
)
