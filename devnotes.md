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
```
sbt assemble
```

Building a fat jar for just one submodule
```
sbt 'spark/assembly'
```

Without running tests
```
sbt 'set test in assembly in aggregator := {}' 'set test in assembly in spark := {}' clean assembly
```

For a submodule without running tests
```
sbt 'set test in assembly in aggregator := {}' 'set test in assembly in spark := {}' clean 'spark/assembly'
```

^ The above is the most common command for iteration


### Push the fat jar to afdev

For running backfills
```
sbt 'set test in assembly in aggregator := {}' 'set test in assembly in spark := {}' clean 'spark/assembly'
scp spark/target/scala-2.11/zipline-spark.jar $USER@$AFDEV_HOST:~/
```

on afdev box:
```
APP_NAME=search_bench3_3 ./spark_submit.sh --class ai.zipline.spark.Join zipline-spark.jar --conf-path bench3_3.json --end-date 2021-01-01 --namespace search_ranking_training --step-days 30
```

### Generate benchmark json
```
python ~/repos/ml_models/zipline/joins/new_algo/search_benchmarks.py > ~/bench3_4.json
```
```
scp ~/repos/ml_models/zipline/joins/new_algo/spark_submit.sh $AFDEV_HOST:~/
```

### Pushing the JAR to artifactory

- Switch to master branch.
- Tag your change.

``` shell
git tag -a -m 'new release' release-zl-0.0.2
```
- Push the tag to artifactory

``` shell
 git push origin release-zl-0.0.2
```
### Install a specific version of thrift
```
brew tap-new $USER/local-thrift
brew extract --version=0.13.0 thrift $USER/local-thrift
brew install thrift@0.13.0
```

Thrift is a dependency for compile. The latest version 0.14 is incompatible with hive metastore. So we force 0.13

