#!/usr/bin/env bash

### ******************* NOTE ***************************
### This is just a template, you will most likely need to modify this file to get things to work
### The environment variables - PARALLELISM, MAX_EXECUTORS, EXECUTOR_CORES, EXECUTOR_MEMORY are what you can tune
### The are specified in `teams.json`, in the conf file itself or as environment variable to run.py. 
### ******************* END ****************************

set -euxo pipefail

mkdir -p ${CHRONON_TMPDIR:-/tmp}/${USER}
export LOG4J_FILE="${CHRONON_TMPDIR:-/tmp}/${USER}/log4j.properties"

cat > ${LOG4J_FILE} << EOF
log4j.rootLogger=INFO, rolling

log4j.appender.rolling=org.apache.log4j.RollingFileAppender
log4j.appender.rolling.layout=org.apache.log4j.PatternLayout
log4j.appender.rolling.layout.conversionPattern=[%d] %p %m (%c)%n
log4j.appender.rolling.maxFileSize=50MB
log4j.appender.rolling.maxBackupIndex=5
log4j.appender.rolling.file=/var/log/spark/\${dm.logging.name}.log
log4j.appender.rolling.encoding=UTF-8

log4j.logger.org.apache.spark=WARN
log4j.logger.org.eclipse.jetty=WARN

log4j.logger.ai.chronon.spark=\${dm.logging.level}
EOF

APP_OUTPUT=$(echo "${CHRONON_TMPDIR:-/tmp}/${APP_NAME}.out")

touch $APP_OUTPUT
chmod a+w $APP_OUTPUT
touch ${APP_OUTPUT}.gc
chmod a+w ${APP_OUTPUT}.gc

GC_OPTION="-XX:+UseParallelGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:${APP_OUTPUT}.gc"

# TODO: export HADOOP_DIR=""
export SPARK_EXTERNAL="$HADOOP_DIR/spark/external/lib"
export STREAMING_JARS="$SPARK_EXTERNAL/spark-sql-kafka-0-10_2.11-2.4.0.jar,$SPARK_EXTERNAL/spark-streaming-kafka-0-10-assembly_2.11-2.4.0.jar"
# TODO: You might need to also add jersey-core-1.9.jar if it isn't bundled in already
export SPARK_JARS="$HADOOP_DIR/hadoop/hadoop-aws.jar,$HADOOP_DIR/java/Hive-JSON-Serde/hive-openx-serde.jar"
/usr/bin/spark-submit \
--spark-version ${SPARK_VERSION:-2.4.0} \
--emr-cluster ${EMR_CLUSTER:-streaming-infra-shared-prod} \
--hive-cluster ${EMR_HIVE_CLUSTER:-silver} \
--deploy-mode cluster \
--queue ${EMR_QUEUE:-zipline-prod} \
--master ${JOB_MODE:-yarn} \
--num-executors ${NUM_EXECUTORS:-16} \
--executor-cores ${EXECUTOR_CORES:-1} \
--executor-memory ${EXECUTOR_MEMORY:-1G} \
--driver-memory ${DRIVER_MEMORY:-1G} \
--conf spark.executor.userClassPathFirst=false \
--conf spark.scheduler.mode=FAIR \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD:-256} \
--conf spark.app.name=${APP_NAME} \
--conf spark.extraListeners= \
--conf spark.sql.queryExecutionListeners= \
--conf spark.sql.extensions=ai.chronon.spark.DummyExtensions \
--conf spark.sql.catalogImplementation=hive \
--conf spark.sql.hive.metastore.version=0.13.1 \
--conf spark.sql.hive.metastore.jars=maven \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
--conf spark.yarn.submit.waitAppCompletion=false \
--files "$CHRONON_CONF_PATH,$LOG4J_FILE" \
--jars "$SPARK_JARS,$STREAMING_JARS,$CHRONON_ONLINE_JAR" \
"$@" 2>&1
