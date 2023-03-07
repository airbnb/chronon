# Intro

## Commands

***All commands assume you are in the root directory of this project***.
For me, that looks like `~/repos/chronon`.

### Prerequisites

Add the following to your shell run command files e.g. `~/.bashrc`.

```
export CHRONON_OS=<path/to/chronon/repo>
export CHRONON_API=$CHRONON_OS/api/py
alias materialize="PYTHONPATH=$CHRONON_API:$PYTHONPATH $CHRONON_API/ai/chronon/repo/compile.py"
```

### Configuring IntelliJ

Be sure to open the project from the `build.sbt` file (at the root level of the git directory).

Mark the following directories as `Sources Root` by right clicking on the directory in the tree view, and selecting `Mark As` -> `Sources Root`:
- aggregator/src/main/scala
- api/src/main/scala
- spark/src/main/scala


Mark the following directories as `Test Root` in a similar way:
- aggregator/src/test/scala
- api/src/test/scala
- spark/src/test/scala

The project should then automatically start indexing, and when it finishes you should be good to go.

**Troubleshooting**

Try the following if you are seeing flaky issues in IntelliJ 
```
sbt +clean 
sbt +assembly
```

### Generate python thrift definitions

```shell
cd $CHRONON_OS
thrift --gen py -out api/py/ai/chronon api/thrift/api.thrift
```

### Materializing confs

```
materialize  --input_path=<path/to/conf>
```

### Testing

All tests
```shell
sbt test
```

Specific submodule tests
```shell
sbt "testOnly *<Module>"
# example to test FetcherTest with 9G memory 
sbt -mem 9000 "test:testOnly *FetcherTest"
# example to test specific test method from GroupByTest
sbt "test:testOnly *GroupByTest -- -t *testSnapshotEntities"
```

### Check module dependencies
```shell
# Graph based view of all the dependencies
sbt dependencyBrowseGraph

# Tree based view of all the dependencies
sbt dependencyBrowseTree
```

### Build a fat jar
```shell
sbt assembly
```

Building a fat jar for just one submodule
```shell
sbt 'spark_uber/assembly'
```

### Install specific version of thrift
```shell
brew tap-new $USER/local-thrift
brew extract --version=0.13.0 thrift $USER/local-thrift
brew install thrift@0.13.0
```

Thrift is a dependency for compile. The latest version 0.14 is very new - feb 2021, and incompatible with hive metastore. So we force 0.13.


### Pushing python API package to a private Pypi repository

[One-Time] Setup your pypi public account and contact nikhil to get added to the PyPi package as a [collaborator](https://pypi.org/manage/project/chronon-ai/collaboration/)

[One-Time] Install build and twine
```
python3 -m pip install build twine
```

[One-Time] Make sure you have the credentials configuration for the python repositories you manage. Normally in `~/.pypirc`
```
[distutils]
index-servers = pypi

[pypi]
repository = https://upload.pypi.org/legacy/
username = <pypi.org user name>
```

Check the [current version](https://pypi.org/manage/project/chronon-ai/releases/) - you are going to increment the version in `api/py/setup.py`

Generate thrift files, build and upload package

```shell
cd $CHRONON_OS
thrift --gen py -out api/py/ai/chronon api/thrift/api.thrift
cd api/py
python3 -m build
twine upload dist/*
```

# Chronon Build Process
* Inside the `$CHRONON_OS` directory.

To build all the Chronon artifacts locally (builds all the JARs)
```shell
sbt package
```

Note: This will create the artifacts with the version specific naming specified under `version.sbt`
```text
Builds on master will result in:
<artifact-name>-<version>.jar
chronon_2.11-0.7.0-SNAPSHOT.jar

Builds on user branches will result in:
<artifact-name>-<branch-name>-<version>.jar
chronon_2.11-jdoe--branch-0.7.0-SNAPSHOT.jar
```

# Chronon Artifacts Publish Process
* Inside the `$CHRONON_OS` directory.

To publish all the Chronon artifacts of the current git HEAD (builds and publishes all the JARs)
```shell
sbt publish
```

* All the SNAPSHOT ones are published to the maven repository as specified by the env variable `$CHRONON_SNAPSHOT_REPO`.
* All the final artifacts are published to the MavenCentral (via Sonatype)

## Setup for publishing artifacts to the JFrog artifactory
1. Login into JFrog artifactory webapp console and create an API Key under user profile section.
2. In `~/.sbt/1.0/jfrog.sbt` add
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

## Setup for Publishing artifacts to MavenCentral (via sonatype)
1. Create a sonatype account if you don't have one. 
   1. Sign up here https://issues.sonatype.org/ 
   2. Create an issue to add your username created above to `ai.chronon`. Here is a sample [issue](https://issues.sonatype.org/browse/OSSRH-88230).
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


# Chronon Release Process

## Publishing libraries to maven central
1. Run release command in the right HEAD of chronon repository.
```
GPG_TTY=$(tty) sbt release
```
This command will take into the account of `version.sbt` and handles a series of events:
* Marks the current SNAPSHOT codebase as final (git commits).
* Creates a new git tag (e.g v0.7.0) pointing to the release commit.
* Builds the artifacts with released versioning suffix and pushes them to Sonatype.
* Updates the `version.sbt` to point to the next in line developmental version (git commits).

2. login into the [staging repo](https://s01.oss.sonatype.org/#stagingRepositories) in nexus (same password as sonatype jira) 
3. In the staging repos list - select your publish 
     1. select "close" wait for the steps to finish
     2. Select "refresh" and "release"
     3. Wait for 30 mins to sync to [maven](https://repo1.maven.org/maven2/) or [sonatype UI](https://search.maven.org/search?q=g:ai.chronon)
4. Push the local release commits (DO NOT SQUASH), and the new tag created from step 1 to Github.
     1. chronon repo disallow push to master directly, so instead push commits to a branch `git push origin master:your-name--release-xxx`
     2. your PR should contain exactly two commits, 1 setting the release version, 1 setting the new snapshot version. 
     3. make sure to use **Rebase pull request** instead of the regular Merge or Squash options when merging the PR.
5. Push release tag to master branch
     1. tag new version to release commit `Setting version to 0.0.xx`. If not already tagged, can be added by 
     ```
       git tag -fa v0.0.xx <commit-sha>
     ```
     2. push tag to master 
      ```
        git push origin <tag-name>
      ```
     3. New tag should be available here  - https://github.com/airbnb/chronon/tags
## [TODO] Publishing a driver to github releases
We use gh releases to release the driver that can backfill, upload, stream etc. 
Currently the repo is not public and the run.py script can't reach it.

# Chronon Documentation via Sphinx
Run the sbt sphinx command to generate the sphinx docs locally and open it.
```
sbt sphinx
```

# build artifacts and release to gcloud
```shell
bash build.sh
bash gcloud_release.sh
```