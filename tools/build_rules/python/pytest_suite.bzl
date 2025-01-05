load("@rules_python//python:defs.bzl", "py_test")
load("@pypi//:requirements.bzl", "requirement")

def pytest_suite(name, srcs, deps = [], args = [], data = [], **kwargs):
    """
        Call pytest_suite
    """
    py_test(
        name = name,
        srcs = [
            "//tools/build_rules/python:pytest_wrapper.py",
        ] + srcs,
        main = "//tools/build_rules/python:pytest_wrapper.py",
        args = ["--capture=no"] + ["$(location :%s)" % x for x in srcs] + args,
        python_version = "PY3",
        srcs_version = "PY3",
        deps = deps + [
            requirement("pytest"),
            #            requirement("pytest-black"),
            #            requirement("pytest-pylint"),
        ],
        data = data,
        **kwargs
    )
