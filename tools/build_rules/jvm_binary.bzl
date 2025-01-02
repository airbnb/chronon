load("@rules_java//java:defs.bzl", "java_binary", "java_library")
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_binary", "scala_library")

def jvm_binary(
        name,
        srcs = [],
        deps = [],
        runtime_deps = [],
        services = {},
        tags = None,
        main_class = None,
        visibility = None,
        create_executable = True,
        testonly = None,
        # All other flags are passed to java_binary
        **kwargs):
    has_scala_srcs = False
    has_java_srcs = False
    for src in srcs:
        if src.endswith(".scala"):
            has_scala_srcs = True
        if src.endswith(".java"):
            has_java_srcs = True
    if has_scala_srcs and has_java_srcs:
        fail("Cannot have scala and java sources in same jvm_binary")

    lib_name = name + "_lib"
    if has_scala_srcs:
        scala_library(
            name = lib_name,
            srcs = srcs,
            deps = deps,
            runtime_deps = runtime_deps,
            tags = tags,
        )
    else:
        java_library(
            name = lib_name,
            srcs = srcs,
            deps = deps,
            runtime_deps = runtime_deps,
            tags = tags,
            testonly = testonly,
        )

    java_binary(
        name = name,
        runtime_deps = [lib_name],
        tags = tags,
        main_class = main_class,
        create_executable = create_executable,
        testonly = testonly,
        **kwargs
    )
