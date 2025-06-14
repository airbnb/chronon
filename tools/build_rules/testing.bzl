load("@io_bazel_rules_scala//scala:scala.bzl", "scala_junit_test", "scala_library")

def is_junit_test(path):
    return path.endswith("Test.scala") or path.endswith("Test.java")

def make_short_name(path, strip_prefix):
    if path.startswith(strip_prefix):
        short_name = path[len(strip_prefix):]
    else:
        short_name = path
    if short_name.startswith("/"):
        short_name = short_name[1:]
    return short_name.replace("/", "_").replace(".scala", "").replace(".java", "")

def scala_junit_test_suite(name, srcs, strip_prefix, **kwargs):
    test_deps = kwargs.pop("deps", [])
    jvm_flags = kwargs.pop("jvm_flags", [])
    timeout = kwargs.pop("timeout", "moderate")

    util_srcs = [src for src in srcs if not is_junit_test(src)]
    if len(util_srcs) > 0:
        test_utils = "{}_utils".format(name)
        scala_library(
            name = test_utils,
            srcs = util_srcs,
            deps = test_deps,
            **kwargs
        )
        test_deps.append(":{}".format(test_utils))

    tests = []
    for src in srcs:
        if is_junit_test(src):
            test_name = "{}_{}".format(name, make_short_name(src, strip_prefix))
            tests.append(test_name)
            scala_junit_test(
                name = test_name,
                srcs = [src],
                suffixes = ["Test"],
                timeout = timeout,
                deps = test_deps,
                jvm_flags = jvm_flags,
                **kwargs
            )

    native.test_suite(
        name = name,
        tests = tests,
    )
