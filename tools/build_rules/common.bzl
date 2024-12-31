"""
  Translation layer(Pants to Bazel) for common targets.
"""

load("@io_bazel_rules_scala_config//:config.bzl", "SCALA_MAJOR_VERSION")
load("@rules_jvm_external//:defs.bzl", "artifact")

def jar(org, name, rev = None, classifier = None):
    if rev:
        fail("Passing rev is no longer supported in jar() and scala_jar()")
    rev = ""
    if classifier:
        return "{}:{}:jar:{}:{}".format(org, name, classifier, rev)
    else:
        return "{}:{}:{}".format(org, name, rev)

def scala_jar(org, name, rev = None, classifier = None):
    name = "{}_{}".format(name, SCALA_MAJOR_VERSION)
    return jar(org, name, rev, classifier)

def bundle(fileset, mapper = {}, relative_to = ""):
    return {
        "fileset": fileset,
        "mapper": mapper,
        "relative_to": relative_to,
    }

def DirectoryReMapper(src, dest):
    return {
        "src": src,
        "dest": dest,
    }

def shading_relocate(left, right):
    return "rule {} {}".format(left, right)

def shading_zap(arg):
    return "zap {}".format(arg)

def exclude(org, name):
    return "@maven//:" + org + ":" + name

def globs(*args, **kwargs):
    include = args
    exclude = kwargs.get("exclude", [])
    exclude_directories = kwargs.get("exclude_directories", 1)
    allow_empty = kwargs.get("allow_empty", True)
    return native.glob(include, exclude, exclude_directories, allow_empty)

def rglobs(*kargs):
    nested = [arg.replace("*", "**/*") for arg in kargs]
    return native.glob(nested)

def guess_java_class(name):
    source_roots = [
        "spark/src/main/scala",
        "spark/src/test/scala",
    ]
    package = native.package_name()
    for root in source_roots:
        _, _, package = package.rpartition(root)
    return (package + "/" + name).strip("/").replace("/", ".")
