# Intro

Brain dump of commands used to do various things

## Commands

***All commands assume you are in the root directory of this project***. 
For me, that looks like `~/repos/zipline`.

### Generate python thrift definitions
 
```shell
thrift --gen py -out api/py/ai/zipline api/thrift/api.thrift
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
