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

def exclude(org, name):
    return "@maven//:" + org + ":" + name
