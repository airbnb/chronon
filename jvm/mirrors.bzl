load("@rules_jvm_external//:defs.bzl", "artifact")
load("@bazel_skylib//lib:dicts.bzl", "dicts")
load("@rules_jvm_external//:defs.bzl", "maven_install")
load(":repos.bzl", "get_override_targets", "repos")

# These mirrors apply to all repos
_mirrors = [
    # Private repositories are supported through HTTP Basic auth
    # "http://username:password@localhost:8081/artifactory/my-repository",
    "https://repo1.maven.org/maven2/",
    "https://mvnrepository.com/artifact",
    "https://packages.confluent.io/maven/",
]

# Any mirrors needed by a particular repo should go here to avoid ballooning the size of the repos that don't use them.
_extra_mirrors = {
    "maven": [
    ],
}

def load_deps():
    for repo in repos:
        maven_install(
            name = repo.name,
            artifacts = repo.artifacts,
            repositories = _mirrors + _extra_mirrors.get(repo.name, []),
            version_conflict_policy = "pinned",
            fetch_sources = True,
            override_targets = dicts.add(get_override_targets(repo.name), repo.overrides),
            duplicate_version_warning = "error",
            fail_if_repin_required = True,
            resolve_timeout = 5000,
            maven_install_json = None,
            excluded_artifacts = repo.excluded_artifacts,
        )
