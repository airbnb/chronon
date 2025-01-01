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
        #        scala_jar(
        #            name = "spark-common-utils",
        #            org = "org.apache.spark",
        #        ),
        #        scala_jar(
        #            name = "spark-sql-api",
        #            org = "org.apache.spark",
        #        ),
        jar(
            name = "avro",
            org = "org.apache.avro",
        ),
        jar(
            name = "hive-metastore",
            org = "org.apache.hive",
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
            name = "commons-lang3",
            org = "org.apache.commons",
        ),
    ],
)

java_binary(
    name = "spark",
    main_class = "None",  #hack
    runtime_deps = [
        "//third_party/java/spark:spark-exec",
    ],
)
