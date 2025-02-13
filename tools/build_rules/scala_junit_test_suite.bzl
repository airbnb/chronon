load("@io_bazel_rules_scala//scala:scala.bzl", "scala_binary", "scala_library", "scala_test")

def scala_junit_suite(name, srcs, deps, resources = None, data = None, visibility = None):
    """
    Automatically infers test classes from Scala source files and creates scala_test targets.

    Args:
        name: The name of the overall test suite.
        src_glob: A glob pattern to locate the Scala test files (e.g., "src/test/scala/**/*.scala").
        deps: A list of dependencies required for the tests.
        resources: (Optional) Resources to include in the tests.
        visibility: (Optional) Visibility of the generated test targets.
    """

    # Infer fully qualified test class names from source files
    #    srcs = native.glob([src_glob])
    test_classes = [
        src.replace("/", ".").replace(".scala", "").replace("src.test.scala.", "").lstrip("src.test.")
        for src in srcs
    ]

    # Create scala_test targets for each test class
    # Create scala_test targets for each test class
    test_targets = []
    for test_class in test_classes:
        test_name = test_class.split(".")[-1]  # Use the class name as the target name.
        scala_test(
            name = test_name,
            srcs = [],
            args = [test_class],
            main_class = "org.junit.runner.JUnitCore",
            resources = resources or [],
            data = data,
            visibility = visibility or ["//visibility:private"],
            deps = deps,
        )
        test_targets.append(":{}".format(test_name))

    # Optionally, create an alias target to run all tests in one command
    native.test_suite(
        name = name,
        tests = test_targets,
        visibility = visibility or ["//visibility:private"],
    )
