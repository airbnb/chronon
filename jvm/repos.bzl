load("@rules_jvm_external//:defs.bzl", "DEFAULT_REPOSITORY_NAME")
load("@rules_jvm_external//:specs.bzl", "json", "maven", "parse")
load("//tools/build_rules:common.bzl", "jar")
load(":defs.bzl", "repo", "versioned_artifacts")

# repos with artifacts defined in external files
load(":maven_repo.bzl", "maven_repo")
load(":spark_repos.bzl", "spark_2_4_repo", "spark_3_1_repo", "spark_3_2_repo", "spark_3_5_repo")
load(":flink_repos.bzl", "flink_1_16_repo")

repos = [
    # The main repos are defined in individual files, which are loaded above and referenced here
    maven_repo,  # defined in maven_repo.bzl
    spark_2_4_repo,
    spark_3_1_repo,
    spark_3_2_repo,
    spark_3_5_repo,
    flink_1_16_repo,
    repo(name = "scala_2.11", artifacts = [
        versioned_artifacts("2.11.12", [
            "org.scala-lang:scala-library",
            "org.scala-lang:scala-compiler",
            "org.scala-lang:scala-reflect",
        ]),
    ]),
    repo(name = "scala_2.12", artifacts = [
        versioned_artifacts("2.12.18", [
            "org.scala-lang:scala-library",
            "org.scala-lang:scala-compiler",
            "org.scala-lang:scala-reflect",
        ]),
    ]),
    repo(name = "scala_2.13", artifacts = [
        versioned_artifacts("2.13.12", [
            "org.scala-lang:scala-library",
            "org.scala-lang:scala-compiler",
            "org.scala-lang:scala-reflect",
        ]),
    ]),
]

def get_repo(repo_name):
    for repo in repos:
        if repo.name == repo_name:
            return repo
    return None

def get_override_targets(repo_name = DEFAULT_REPOSITORY_NAME):
    if repo_name.startswith("scala_"):
        return {}
    overrides = {
        "log4j:log4j": "@maven//:ch_qos_reload4j_reload4j",
        "org.scala-lang:scala-library": "@//third_party/scala:scala-library",
        "org.scala-lang:scala-reflect": "@//third_party/scala:reflect",
        "org.scala-lang:scala-compiler": "@//third_party/scala:scala-compiler",
    }
    repo = get_repo(repo_name) or fail("Repo {} does not exist".format(repo_name))
    return overrides
