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

### Generate python thrift definitions

```shell
cd $CHRONON_OS
thrift --gen py -out api/py/ai/chronon api/thrift/api.thrift
```

### Materializing confs

```
materizlize  --input_path=<path/to/conf>
```

### Testing

All tests
```shell
sbt test
```

Specific submodule tests
```shell
sbt "spark/test"
sbt "aggregator/test"
```

### Build a fat jar
```shell
sbt assemble
```

Building a fat jar for just one submodule
```shell
sbt 'spark/assembly'
```

Without running tests
```shell
sbt 'set test in assembly in aggregator := {}' 'set test in assembly in spark := {}' clean assembly
```

For a submodule without running tests
```shell
sbt 'set test in assembly in aggregator := {}' 'set test in assembly in spark := {}' clean 'spark/assembly'
```

^ The above is the most common command for iteration

### Install specific version of thrift
```shell
brew tap-new $USER/local-thrift
brew extract --version=0.13.0 thrift $USER/local-thrift
brew install thrift@0.13.0
```

Thrift is a dependency for compile. The latest version 0.14 is very new - feb 2021, and incompatible with hive metastore. So we force 0.13.


### Pushing python API package to a private Pypi repository

First make sure the thrift definitions are updated:
```shell
cd $CHRONON_OS
thrift --gen py -out api/py/ai/chronon api/thrift/api.thrift
```

Second make sure you have the credentials configuration for the python repositories you manage. Normally in `~/.pypirc`
```
[distutils]
index-servers = pypi

[pypi]
repository = https://upload.pypi.org/legacy/
username = <pypi.org user name>
```

Finally, go into the folder containing setup.py and run the publish to the required (using setuptools or twine).
For example, following the configuration above to publish into the `<private>` repository.
```
cd $CHRONON_OS
cd api/py
python setup.py sdist upload -r private
```

Don't forget to update the version if necessary


## Setup for Publishing libraries to MavenCentral (via sonatype)
1. Create a sonatype account if you don't have
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

## Publishing libraries to maven central
1.Run publish
```
CHRONON_VERSION=0.0.X GPG_TTY=$(tty) sbt +publishSigned
```
2. login into the [staging repo](https://s01.oss.sonatype.org/#stagingRepositories) in nexus (same password as sonatype jira) 
3. In the staging repos list - select your publish 
     1. select "close" wait for the steps to finish
     2. Select "refresh" and "release"
     3. Wait for 30 mins to sync to [maven](https://repo1.maven.org/maven2/) or [sonatype UI](https://search.maven.org/search?q=g:ai.chronon)

## [TODO] Publishing a driver to github releases
We use gh releases to release the driver that can backfill, upload, stream etc. 
Currently the repo is not public and the run.py script can't reach it.
