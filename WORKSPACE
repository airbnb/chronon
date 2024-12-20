workspace(name = "chronon")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_jvm_external",
    strip_prefix = "rules_jvm_external-4.2",
    urls = ["https://github.com/bazelbuild/rules_jvm_external/archive/refs/tags/4.2.zip"],
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        # List your Maven dependencies here
        #        "org.scala-lang:scala-library:2.11.12",
        #        "org.scala-lang:scala-library:2.12.18",
        "org.scala-lang:scala-library:2.13.12",
        "org.scala-lang.modules:scala-parallel-collections_2.13:1.0.4",
        # Add other dependencies
    ],
    repositories = [
        "https://repo1.maven.org/maven2",
    ],
)

#############################
#            C++            #
#############################
RULES_CC_VERSION = "0.0.1"

RULES_CC_SHA256 = "d9f4686206d20d7c5513a39933aa1148d21d6ce16134ae4c4567c40bbac359bd"

http_archive(
    name = "rules_cc",
    sha256 = RULES_CC_SHA256,
    strip_prefix = "rules_cc-{version}".format(version = RULES_CC_VERSION),
    urls = ["https://github.com/bazelbuild/rules_cc/archive/{version}.zip".format(version = RULES_CC_VERSION)],
)

# Remove all the remote_java_tools above when upgrading to rules_java 7.5.0 or greater
http_archive(
    name = "rules_java",
    sha256 = "e81e9deaae0d9d99ef3dd5f6c1b32338447fe16d5564155531ea4eb7ef38854b",
    urls = [
        "https://github.com/bazelbuild/rules_java/releases/download/7.0.6/rules_java-7.0.6.tar.gz",
    ],
)

load("@rules_java//java:repositories.bzl", "rules_java_dependencies", "rules_java_toolchains")

rules_java_dependencies()

rules_java_toolchains()

load("@rules_java//java:repositories.bzl", "remote_jdk8_repos")

remote_jdk8_repos()

http_archive(
    name = "bazel_skylib",
    sha256 = "b8a1527901774180afc798aeb28c4634bdccf19c4d98e7bdd1ce79d1fe9aaad7",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.4.1/bazel-skylib-1.4.1.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.4.1/bazel-skylib-1.4.1.tar.gz",
    ],
)

http_archive(
    name = "io_bazel_rules_scala",
    sha256 = "e734eef95cf26c0171566bdc24d83bd82bdaf8ca7873bec6ce9b0d524bdaf05d",
    strip_prefix = "rules_scala-6.6.0",
    url = "https://github.com/bazelbuild/rules_scala/releases/download/v6.6.0/rules_scala-v6.6.0.tar.gz",
)

#load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
#
#git_repository(
#    name = "io_bazel_rules_scala",
#    branch = "master",
#    #    commit = "719f353b85129106a745d9825be2c09231d4fcae",
#    # patch prevents default namespace being passed to helm
#    remote = "https://github.com/bazelbuild/rules_scala.git",
#    #tag = "v0.5.0",
#)

load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")

scala_config(
    scala_version = "2.13.12",  # Specify your desired Scala version
)

#
load("@io_bazel_rules_scala//scala:scala.bzl", "rules_scala_setup", "rules_scala_toolchain_deps_repositories")

#
## loads other rules Rules Scala depends on
rules_scala_setup()

#
## Loads Maven deps like Scala compiler and standard libs. On production projects you should consider
## defining a custom deps toolchains to use your project libs instead
rules_scala_toolchain_deps_repositories(fetch_sources = True)

#############################
#         Protobuf          #
#############################
http_archive(
    name = "rules_proto",
    sha256 = "dc3fb206a2cb3441b485eb1e423165b231235a1ea9b031b4433cf7bc1fa460dd",
    strip_prefix = "rules_proto-5.3.0-21.7",
    urls = [
        "https://github.com/bazelbuild/rules_proto/archive/refs/tags/5.3.0-21.7.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")

#
scala_register_toolchains()

# optional: setup ScalaTest toolchain and dependencies
load("@io_bazel_rules_scala//testing:scalatest.bzl", "scalatest_repositories", "scalatest_toolchain")

scalatest_repositories()

scalatest_toolchain()
