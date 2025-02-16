load("@rules_jvm_external//:specs.bzl", "maven")
load(":defs.bzl", "repo", "versioned_artifacts")

flink_1_16_repo = repo(
    name = "flink_1_16",
    artifacts = [
        versioned_artifacts(
            "1.16.0",
            [
                "org.apache.flink:flink-runtime",
                "org.apache.flink:flink-clients",
                "org.apache.flink:flink-connector-files",
                "org.apache.flink:flink-connector-hive_2.12",
                "org.apache.flink:flink-csv",
                "org.apache.flink:flink-json",
                "org.apache.flink:flink-metrics-core",
                "org.apache.flink:flink-metrics-prometheus:jar",
                "org.apache.flink:flink-orc",
                "org.apache.flink:flink-parquet",
                "org.apache.flink:flink-protobuf",
                "org.apache.flink:flink-scala_2.12",
                "org.apache.flink:flink-sql-gateway-api",
                "org.apache.flink:flink-streaming-java",
                "org.apache.flink:flink-streaming-scala_2.12",
                "org.apache.flink:flink-table-api-java",
                "org.apache.flink:flink-test-utils",
                "org.apache.flink:flink-table-planner_2.12",
                "org.apache.flink:flink-streaming-java:jar:tests",
                "org.apache.flink:flink-metrics-dropwizard",
            ],
        ),
    ],
)
