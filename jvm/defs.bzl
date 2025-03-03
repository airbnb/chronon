load("@rules_jvm_external//:specs.bzl", "parse")
load("//tools/build_rules:utils.bzl", "flatten", "map")
load("@rules_jvm_external//:defs.bzl", "artifact")

def _parse_versioned_artifact(artifact, version, exclusions):
    result = parse.parse_maven_coordinate("{}:{}".format(artifact, version))
    if (exclusions != None):
        result["exclusions"] = exclusions
    return result

def versioned_artifacts(version, artifacts, exclusions = None):
    return map(lambda artifact: _parse_versioned_artifact(artifact, version, exclusions), artifacts)

SUPPORTED_SCALA_VERSIONS = ["2.12", "2.13"]

def repo(name, pinned = True, artifacts = [], overrides = {}, vars = {}, excluded_artifacts = []):
    final_artifacts = []
    flat_artifacts = flatten(artifacts)
    for artifact in parse.parse_artifact_spec_list(flat_artifacts):
        # Empty string in packaging seems to mess up Coursier, maybe a bug in RJE
        if artifact.get("packaging") == "":
            artifact.pop("packaging")
        artifact["version"] = artifact["version"].format(**vars)
        final_artifacts.append(artifact)
    return struct(
        name = name,
        pinned = pinned,
        artifacts = final_artifacts,
        overrides = overrides,
        vars = vars,
        excluded_artifacts = excluded_artifacts,
    )

def get_jars_for_repo(repo_name, jars):
    return [artifact(jar, repository_name = repo_name) for jar in jars]
