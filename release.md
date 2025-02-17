# Chronon Release Process (Bazel based)

## Set up for publishing artifacts to Maven Central (via Sonatype)

1. Get maintainer access to Maven Central on Sonatype
    1. Create a Sonatype account if you don't have one
        1. Sign up at https://issues.sonatype.org/
    2. Ask a current Chronon maintainer to add you to Sonatype project
        1. To add a new member, an existing Chronon maintainer will need to [email Sonatype central support](https://central.sonatype.org/faq/what-happened-to-issues-sonatype-org/#where-did-issuessonatypeorg-go) and request a new member to be added as a maintainer. Include the username for the newly created Sonatype account in the email.
2. Install GPG: `brew install gpg` on macOS
3. Setup GPG - follow the first step in this [link](https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html#step+1%3A+PGP+Signatures)

## Setup for pushing Python API package to PyPi repository

1. Setup your PyPi public account and contact the team to get added to the PyPi package as a [collaborator](https://pypi.org/manage/project/chronon-ai/collaboration/)
2. Install required Python packages:
```bash
python3 -m pip install -U twine
```

3. Fetch the user token from the PyPi website
4. Create or update your PyPi credentials configuration at `~/.pypirc`:
```ini
[distutils]
  index-servers =
    pypi
    chronon-pypi

[pypi]
  username = __token__
  password = <your-token>

[chronon-pypi]
  repository = https://upload.pypi.org/legacy/
  username = __token__
  password = <project-specific-pypi-token>
```

## Publishing Artifacts Locally (for testing)

To publish artifacts to your local Maven repository for testing:

```bash
# Publish API module
bazel run --define "maven_repo=file://$HOME/.m2/repository" \
          --define "version=0.0.1" \
          //api:api-export.publish

# Publish Flink module
bazel run --define "maven_repo=file://$HOME/.m2/repository" \
          --define "version=0.0.1" \
          //flink:flink-export.publish

# Publish Online module (Scala 2.12)
bazel run --define "maven_repo=file://$HOME/.m2/repository" \
          --define "version=0.0.1" \
          --config scala_2.12 \
          //online:online-export.publish

# Publish Spark module
bazel run --define "maven_repo=file://$HOME/.m2/repository" \
          --define "version=0.0.1" \
          //spark:spark-assembly-export.publish
```

## Release Process to Maven Central & PyPi

1. Ensure you're on the main branch and at the correct commit for release
2. Update version number in your project's version file
3. Publish Java/Scala artifacts to Maven Central:
```bash
# Replace <version> with your release version
# Publish API module
bazel run --define "maven_repo=https://s01.oss.sonatype.org/service/local/staging/deploy/maven2" \
          --define "version=<version>" \
          //api:api-export.publish

# Publish Flink module
bazel run --define "maven_repo=https://s01.oss.sonatype.org/service/local/staging/deploy/maven2" \
          --define "version=<version>" \
          //flink:flink-export.publish

# Publish Online module (Scala 2.12)
bazel run --define "maven_repo=https://s01.oss.sonatype.org/service/local/staging/deploy/maven2" \
          --define "version=<version>" \
          --config scala_2.12 \
          //online:online-export.publish

# Publish Spark module
bazel run --define "maven_repo=https://s01.oss.sonatype.org/service/local/staging/deploy/maven2" \
          --define "version=<version>" \
          //spark:spark-assembly-export.publish
```

4. Publish Python package to PyPi:
```bash
bazel run --define "version=<version>" //path/to:upload_chronon_ai
```

5. After publishing to Sonatype:
    1. Login to the [staging repo](https://s01.oss.sonatype.org/#stagingRepositories)
    2. In the staging repos list, select your publish
        1. Click "close" and wait for the steps to finish
        2. Click "refresh" and then "release"
        3. Wait ~30 minutes for sync to [Maven Central](https://repo1.maven.org/maven2/) or check [Sonatype UI](https://search.maven.org/search?q=g:ai.chronon)

6. Verify the Python API package on [PyPi](https://pypi.org/project/chronon-ai/)

7. Update Chronon repository:
    1. Create a tag for the release:
    ```bash
    git tag -a v<version> -m "Release v<version>"
    git push origin v<version>
    ```
    2. Open a PR to update version numbers for next development iteration
    3. Verify the new tag appears in [GitHub releases](https://github.com/airbnb/chronon/tags)

## Troubleshooting

- If Maven Central publishing fails, check the Sonatype staging repository for specific error messages
- For PyPi upload issues, ensure your `.pypirc` credentials are correct and you have the necessary permissions
- If local testing is needed, use the local Maven repository publishing commands with `maven_repo=file://$HOME/.m2/repository`
- 
# Chronon Release Process (SBT based)

## Set up for publishing artifacts to JFrog artifactory

1. Configure `$CHRONON_SNAPSHOT_REPO` env var to point to the JFrog artifactory.
2. Login into JFrog artifactory webapp console and create an API Key under user profile section.
3. In `~/.sbt/1.0/jfrog.sbt` add
```scala
credentials += Credentials(Path.userHome / ".sbt" / "jfrog_credentials")
```
4. In `~/.sbt/jfrog_credentials` add
```
realm=Artifactory Realm
host=<Artifactory domain of $CHRONON_SNAPSHOT_REPO>
user=<your username>
password=<API Key>
```

## Set up for publishing artifacts to MavenCentral (via sonatype)
1. Get maintainer access to Maven Central on Sonatype
    1. Create a sonatype account if you don't have one.
        1. Sign up here https://issues.sonatype.org/
    2. Ask a current Chronon maintainer to add you to Sonatype project.
        1. To add a new member, an existing Chronon maintainer will need to [email Sonatype central support](https://central.sonatype.org/faq/what-happened-to-issues-sonatype-org/#where-did-issuessonatypeorg-go) and request a new member to be added as a maintainer. Include the username for the newly created Sonatype account in the email.
2. `brew install gpg` on your mac
3. In `~/.sbt/1.0/sonatype.sbt` add
```scala
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
```
4. In `~/.sbt/sonatype_credentials` add
```
realm=Sonatype Nexus Repository Manager
host=s01.oss.sonatype.org
user=<your username>
password=<your password>
```
5. setup gpg - just first step in this [link](https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html#step+1%3A+PGP+Signatures)

## Setup for pushing python API package to PyPi repository

1. Setup your pypi public account and contact @Nikhil to get added to the PyPi package as a [collaborator](https://pypi.org/manage/project/chronon-ai/collaboration/)
2. Install `tox, build, twine`. There are three python requirements for the python build process.
* tox: Module for testing. To run the tests run tox in the main project directory.
* build: Module for building. To build run `python -m build` in the main project directory
* twine: Module for publishing. To upload a distribution run `twine upload dist/<distribution>.whl`
```
python3 -m pip install -U tox build twine
```

3. Fetch the user token from the PyPi website.
4. Make sure you have the credentials configuration for the python repositories you manage. Normally in `~/.pypirc`
```
[distutils]
  index-servers =
    local
    pypi
    chronon-pypi

[local]
  repository = # local artifactory
  username = # local username
  password = # token or password

[pypi]
  username = # username or __token__
  password = # password or token

# Or if using a project specific token
[chronon-pypi]
  repository = https://upload.pypi.org/legacy/
  username = __token__
  password = # Project specific pypi token.
```

## Publish SNAPSHOT artifacts to JFrog artifactory

To publish all the Chronon artifacts of the current git HEAD (builds and publishes all the JARs)
```shell
sbt -mem 8192 publish
```

This will publish the artifacts to the JFrog artifactory at `$CHRONON_SNAPSHOT_REPO` and is useful for integration testing.

NOTE: Python API package will also be generated, but it will not be pushed to any PyPi repository. Only `release` will
push the Python artifacts to the public repository.

## Release artifacts to Maven Central & PyPi from main branch
1. Run release command in the right HEAD of chronon repository. Before running this, you may want to activate your Python venv or install the required Python packages on the laptop. Otherwise, the Python release will fail due to missing deps.
```shell
GPG_TTY=$(tty) sbt -mem 8192 release
```
This command will take into the account of `version.sbt` and handles a series of events:
* Marks the current SNAPSHOT codebase as final (git commits).
* Creates a new git tag (e.g v0.7.0) pointing to the release commit.
* Builds the artifacts with released versioning suffix and pushes them to Sonatype, and PyPi central.
* Updates the `version.sbt` to point to the next in line developmental version (git commits).

2. login into the [staging repo](https://s01.oss.sonatype.org/#stagingRepositories) in nexus (same password as sonatype jira)
3. In the staging repos list - select your publish
    1. select "close" wait for the steps to finish
    2. Select "refresh" and "release"
    3. Wait for 30 mins to sync to [maven](https://repo1.maven.org/maven2/) or [sonatype UI](https://search.maven.org/search?q=g:ai.chronon)
4. Verify the Python API from the [PyPi website](https://pypi.org/project/chronon-ai/) that we are pointing to the latest.
5. Update Chronon repo main branch:
    1. Open a PR to main branch to bump version like this one: https://github.com/airbnb/chronon/pull/860
    2. Push release tag to main branch
        1. tag new version to release commit `Setting version to 0.0.xx`. If not already tagged, can be added by
         ```
           git tag -fa v0.0.xx <commit-sha>
         ```
        2. push tag
           ```
             git push origin <tag-name>
           ```
        3. New tag should be available here  - https://github.com/airbnb/chronon/tags
