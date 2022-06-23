# Intro

Brain dump of commands used to do various things. For a complete devnotes with Airbnb-specific commands go to this Quip [doc](https://airbnb.quip.com/3ZTyABbDwDUZ/Zipline-V21-dev-notes).



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
materialize --input_path=<path/to/conf>
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



## ztool

ztool is the mega cli tool that allows users to access all zipline functionalities.
We use the [xerial/sbt-pack](https://github.com/xerial/sbt-pack) plugin to accomplish the bundling.
```bash
# Building ztool. We use xerial/sbt-pack plugin.
$> sbt spark/pack
# produces an executable at spark/target/pack/bin/ztool
```

Running a fetch command
```bash
$> spark/target/pack/bin/ztool fetch \
    --online-class <YOUR_online.Api_implmentation> \
    --online-jar <jar path to this implementation> \
    # the values below pass Map[String, String] that is used to construct an instance of online api.
    -Zkey1=value1 -Zkey2=value2 \ 
    --key '{"user_id": "bob"}' --name zipline_test.online_join.v1 -t join

```


### Packaging ztool for distribution
sbt-pack plugin produces a `Makefile` at `spark/target`. Running an `archive` command on this mainfile inturn produces a distributable tar archive.
```bash
$> cd spark/target/pack; make archive
```

**TODO**: The idea here is to use this archive as a [github release](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository). 

Users should be able to: 
 1. download this archive - `wget https://github.com/airbnb/zipline/releases/download/vx.x.x/ztool.tar.gz`
 2. untar it, `tar -xvzf ztool.tar.gz`
 3. `cd ztool` and `make install` to put the ztool in their bin path.


### ztool man page 

```
-h, --help   Show help message

Subcommand: join
  -c, --conf-path  <arg>   Path to conf
  -e, --end-date  <arg>    End date to compute as of, start date is taken from
                           conf.
      --skip-equal-check   Check if this join has already run with a different
                           conf, if so it will fail the job
  -s, --step-days  <arg>   Runs backfill in steps, step-days at a time
  -h, --help               Show help message

Subcommand: group-by-backfill
  -c, --conf-path  <arg>   Path to conf
  -e, --end-date  <arg>    End date to compute as of, start date is taken from
                           conf.
  -s, --step-days  <arg>   Runs backfill in steps, step-days at a time
  -h, --help               Show help message

Subcommand: group-by-upload
  -c, --conf-path  <arg>   Path to conf
  -e, --end-date  <arg>    End date to compute as of, start date is taken from
                           conf.
  -h, --help               Show help message

Subcommand: fetch
  -k, --key-json  <arg>        json of the keys to fetch
  -n, --name  <arg>            name of the join/group-by to fetch
      --online-class  <arg>    Fully qualified Online.Api based class. We expect
                               the jar to be on the class path
  -o, --online-jar  <arg>      Path to the jar contain the implementation of
                               Online.Api class
  -t, --type  <arg>            the type of conf to fetch Choices: join, group-by
  -Zkey=value [key=value]...
  -h, --help                   Show help message

Subcommand: metadata-upload
  -c, --conf-path  <arg>       Path to the Zipline config file or directory
      --online-class  <arg>    Fully qualified Online.Api based class. We expect
                               the jar to be on the class path
  -o, --online-jar  <arg>      Path to the jar contain the implementation of
                               Online.Api class
  -Zkey=value [key=value]...
  -h, --help                   Show help message

Subcommand: group-by-streaming
  -c, --conf-path  <arg>         path to groupBy conf
  -d, --debug                    Prints details of data flowing through the
                                 streaming job
  -k, --kafka-bootstrap  <arg>   host:port of a kafka bootstrap server
  -l, --local                    Launches the job locally
  -m, --mock-writes              flag - to ignore writing to the underlying kv
                                 store
      --online-class  <arg>      Fully qualified Online.Api based class. We
                                 expect the jar to be on the class path
  -o, --online-jar  <arg>        Path to the jar contain the implementation of
                                 Online.Api class
  -Zkey=value [key=value]...
  -h, --help                     Show help message
```