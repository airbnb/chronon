# Intro

Brain dump of commands used to do various things

## Commands

***All commands assume you are in the root directory of this project***.
For me, that looks like `~/repos/zipline`.

### Prerequisites

Add the following to your shell run command files e.g. `~/.bashrc`.

```
export ZIPLINE_OS=<path/to/zipline/repo>
export ZIPLINE_API=$ZIPLINE_OS/api/py
alias materialize="PYTHONPATH=$ZIPLINE_API:$PYTHONPATH $ZIPLINE_API/ai/zipline/repo/compile.py"
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
cd $ZIPLINE_OS
thrift --gen py -out api/py/ai/zipline api/thrift/api.thrift
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


### Push the fat jar to afdev

For running backfills
```shell
sbt 'set test in assembly in aggregator := {}' 'set test in assembly in spark := {}' clean 'spark/assembly'
scp spark/target/scala-2.11/zipline-spark.jar $USER@$AFDEV_HOST:~/
```

on afdev box:
```shell
APP_NAME=search_bench3_3 ./spark_submit.sh --class ai.zipline.spark.Join zipline-spark.jar --conf-path bench3_3.json --end-date 2021-01-01 --namespace search_ranking_training --step-days 30
```

### Generate benchmark json
```shell
python ~/repos/ml_models/zipline/joins/new_algo/search_benchmarks.py > ~/bench3_4.json
```
```shell
scp ~/repos/ml_models/zipline/joins/new_algo/spark_submit.sh $AFDEV_HOST:~/
```


# Publishing project fat JAR to Artifactory

0. Create MVN settings file under `~/mvn_settings.xml` in the repo root.

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                          https://maven.apache.org/xsd/settings-1.0.0.xsd">
    <servers>
        <server>
            <id>airbnb</id>
            <username>ARTIFACTORY_USERNAME</username>
            <password>ARTIFACTORY_PASSWORD</password>
        </server>
    </servers>
</settings>
```

Replace `ARTIFACTORY_USERNAME` and `ARTIFACTORY_PASSWORD` with your username and API key from [Artifactory](https://artifactory.d.musta.ch/artifactory/webapp/#/profile).

1. After you merge your PR, check out and pull `master` branch.
2. Create a tag named `release-zl-X.X.X`. Check `git tag` to find out the next version.

```shell
git tag -a -m '<tag message>' release-zl-X.X.X
```

3. Publish to artifactory

```shell
./push_to_artifactory.sh <tag-message>
```

### Using Maven local repository to test Zipline changes with your local treehouse changes

0. Make sure `mvn` is on your `PATH` e.g. `export PATH=/opt/apache-maven-3.8.2/bin:$PATH`

1. Push JARs to local Maven local repository
``` shell
./push_to_mvn_local.sh
```

2. Point to local JARs in Maven local repository in your `build.gradle` file e.g.

``` shell
dependencies {
    ...
    implementation 'ai.zipline:spark_uber_2.11:local'
```

3. Add Maven local repository to your `build.gradle` file's `repositories` (remove this before checking in)

``` shell
repositories {
  mavenLocal()
}
 ```

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
cd $ZIPLINE_OS
thrift --gen py -out api/py/ai/zipline api/thrift/api.thrift
```

Second make sure you have the credentials configuration for the python repositories you manage. Normally in `~/.pypirc`
```
[distutils]
index-servers = private

[private]
repository: <private pypi artifactory url>
username: <username>
password: <password or token>
```

Finally, go into the folder containing setup.py and run the publish to the required (using setuptools or twine).
For example, following the configuration above to publish into the `<private>` repository.
```
cd $ZIPLINE_OS
cd api/py
python setup.py sdist upload -r private
```

Don't forget to update the version if necessary
