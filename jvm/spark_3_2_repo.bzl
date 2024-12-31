load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repo")

spark_3_2_repo = repo(
    name = "spark_3_2",
    provided = True,
    vars = {
        "spark_version": "3.2.1",
        "hadoop_version": "3.3.6",
    },
    excluded_artifacts = [
        "org.pentaho:pentaho-aggdesigner-algorithm",
    ],
    artifacts = [
        # Spark artifacts - only Scala 2.12 since that's our target
        "org.apache.spark:spark-sql_2.12:{spark_version}",
        "org.apache.spark:spark-mllib_2.12:{spark_version}",
        "org.apache.spark:spark-hive_2.12:{spark_version}",

        # Hive
        "org.apache.hive:hive-exec:2.3.9",

        # Other dependencies
        "org.apache.curator:apache-curator:2.12.0",
        "com.esotericsoftware:kryo:5.1.1",
        "com.yahoo.datasketches:sketches-core:0.13.4",
        "com.yahoo.datasketches:memory:0.12.2",
        "com.yahoo.datasketches:sketches-hive:0.13.0",
        "org.apache.datasketches:datasketches-java:2.0.0",
        "org.apache.datasketches:datasketches-memory:1.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.6",
        "com.amazonaws:aws-java-sdk-s3:1.12.367",

        # Kafka dependencies - only Scala 2.12
        "org.apache.kafka:kafka_2.12:2.6.3",

        # Avro dependencies
        "org.apache.avro:avro:1.8.2",
        "org.apache.avro:avro-mapred:1.8.2",

        # Monitoring
        "io.prometheus.jmx:jmx_prometheus_javaagent:0.20.0",
    ],
)
