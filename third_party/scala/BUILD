load("@io_bazel_rules_scala_config//:config.bzl", "SCALA_MAJOR_VERSION", "SCALA_VERSION")

package(default_visibility = ["//visibility:public"])

alias(
    name = "combinators",
    actual = scala_artifact("org.scala-lang.modules:scala-parser-combinators"),
)

alias(
    name = "reflect",
    actual = maven_artifact(
        "org.scala-lang:scala-reflect",
        repository_name = "scala_" + SCALA_MAJOR_VERSION,
    ),
)

alias(
    name = "scala-compiler",
    actual = maven_artifact(
        "org.scala-lang:scala-compiler",
        repository_name = "scala_" + SCALA_MAJOR_VERSION,
    ),
)

alias(
    name = "scalactic",
    actual = scala_artifact("org.scalactic:scalactic"),
)

alias(
    name = "scalatest",
    actual = scala_artifact("org.scalatest:scalatest"),
)

alias(
    name = "scala-library",
    actual = maven_artifact(
        "org.scala-lang:scala-library",
        repository_name = "scala_" + SCALA_MAJOR_VERSION,
    ),
)
