package(default_visibility = ["//visibility:public"])

load("@rules_jvm_external//:defs.bzl", "artifact")

jar_library(
    name = "spark-exec",
    jars = [
        scala_jar(
            name = "spark-core",
            org = "org.apache.spark",
        ),
        scala_jar(
            name = "spark-catalyst",
            org = "org.apache.spark",
        ),
        scala_jar(
            name = "spark-sql",
            org = "org.apache.spark",
        ),
        scala_jar(
            name = "spark-hive",
            org = "org.apache.spark",
        ),
        scala_jar(
            name = "spark-sketch",
            org = "org.apache.spark",
        ),
        scala_jar(
            name = "spark-streaming",
            org = "org.apache.spark",
        ),
        scala_jar(
            name = "spark-tags",
            org = "org.apache.spark",
        ),
        jar(
            name = "scala-library",
            org = "org.scala-lang",
        ),
        scala_jar(
            name = "spark-unsafe",
            org = "org.apache.spark",
        ),
        jar(
            name = "hive-exec",
            org = "org.apache.hive",
        ),
        jar(
            name = "hadoop-common",
            org = "org.apache.hadoop",
        ),
        jar(
            name = "jackson-core",
            org = "com.fasterxml.jackson.core",
        ),
        jar(
            name = "jackson-annotations",
            org = "com.fasterxml.jackson.core",
        ),
        jar(
            name = "jackson-databind",
            org = "com.fasterxml.jackson.core",
        ),
        jar(
            name = "kryo_shaded",
            org = "com.esotericsoftware",
        ),
        scala_jar(
            name = "json4s-jackson",
            org = "org.json4s",
        ),
        jar(
            name = "protobuf-java",
            org = "com.google.protobuf",
        ),
        jar(
            name = "hive-metastore",
            org = "org.apache.hive",
        ),
    ],
)

_SPARK_DEPS = [
    "//third_party/java/spark:spark-exec",
    maven_artifact("org.apache.commons:commons-lang3"),
]

java_binary(
    name = "spark",
    main_class = "None",  #hack
    runtime_deps = _SPARK_DEPS,
)
