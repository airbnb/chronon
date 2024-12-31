load("@bazel_skylib//lib:dicts.bzl", "dicts")
load("@rules_jvm_external//:defs.bzl", "artifact")
load("//jvm:repos.bzl", "repos")

DEFAULT_PROVIDED_REPO = "maven"  # For backwards compatability

def jar_library(name, jars = [], overrides = {}, visibility = ["//visibility:public"], **kwargs):
    def _get_jars(repo_name):
        return [artifact(jar, repository_name = repo_name) for jar in jars]

    repo_name = DEFAULT_PROVIDED_REPO
    configured_jars = select({
        "//tools/flags/spark:spark_2_4": _get_jars("spark_2_4"),
        "//tools/flags/spark:spark_3_1": _get_jars("spark_3_1"),
        "//tools/flags/spark:spark_3_2": _get_jars("spark_3_2"),
        "//conditions:default": _get_jars("maven"),
    })

    native.java_library(
        name = name,
        exports = configured_jars,
        visibility = visibility,
        **kwargs
    )
