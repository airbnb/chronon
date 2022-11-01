#!/usr/bin/env bash

### ******************* NOTE ***************************
### This is just a template, you will most likely need to modify this file to get things to work
### ******************* END ****************************

set -euxo pipefail
mkdir -p ${CHRONON_TMPDIR:-/tmp}/${USER}
export LOG4J_FILE="${CHRONON_TMPDIR:-/tmp}/${USER}/log4j_file"
cat > ${LOG4J_FILE} << EOF
log4j.rootLogger=INFO, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.Target=System.out
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
EOF

export TEST_NAME="${APP_NAME}_${USER}_test"
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_PYTHON
unset SPARK_HOME
unset SPARK_CONF_DIR
# You will need to point your spark submit to the correct yarn cluster if you happen 
spark-submit \
--driver-java-options " -Dlog4j.configuration=file:${LOG4J_FILE}" \
--conf "spark.executor.extraJavaOptions= -XX:ParallelGCThreads=4 -XX:+UseParallelGC -XX:+UseCompressedOops" \
--conf spark.reducer.maxReqsInFlight=1024 \
--conf spark.reducer.maxBlocksInFlightPerAddress=1024 \
--conf spark.reducer.maxSizeInFlight=256M \
--conf spark.shuffle.file.buffer=1M \
--conf spark.shuffle.service.enabled=true \
--conf spark.shuffle.service.index.cache.entries=2048 \
--conf spark.shuffle.io.serverThreads=128 \
--conf spark.shuffle.io.backLog=1024 \
--conf spark.shuffle.registration.timeout=2m \
--conf spark.sql.broadcastTimeout=1200 \
--conf spark.shuffle.registration.maxAttempts=5 \
--conf spark.unsafe.sorter.spill.reader.buffer.size=1M \
--conf spark.sql.shuffle.partitions=${PARALLELISM:-4000} \
--conf spark.kryoserializer.buffer.max=2000 \
--conf spark.rdd.compress=true \
--conf spark.shuffle.compress=true \
--conf spark.shuffle.spill.compress=true \
--conf spark.io.compression.codec=zstd \
--conf spark.io.compression.zstd.level=2 \
--conf spark.io.compression.zstd.bufferSize=1M \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS:-1000} \
--conf spark.default.parallelism=${PARALLELISM:-4000} \
--conf spark.port.maxRetries=20 \
--conf spark.task.maxFailures=20 \
--conf spark.stage.maxConsecutiveAttempts=12 \
--conf spark.maxRemoteBlockSizeFetchToMem=2G \
--conf spark.network.timeout=230s \
--conf spark.executor.heartbeatInterval=200s \
--conf spark.local.dir=${CHRONON_TMPDIR:-/tmp} \
--conf spark.jars.ivy=${CHRONON_TMPDIR:-/tmp} \
--conf spark.executor.cores=${EXECUTOR_CORES:-1} \
--conf spark.sql.files.maxPartitionBytes=1073741824 \
--conf spark.debug.maxToStringFields=1000 \
--conf spark.driver.maxResultSize=32G \
--deploy-mode client \
--master yarn \
--executor-memory "${EXECUTOR_MEMORY:-8G}" \
--driver-memory "${DRIVER_MEMORY:-8G}" \
--conf spark.executor.memoryOverhead=2G \
--conf spark.app.name=${APP_NAME} \
--jars "${CHRONON_ONLINE_JAR:-}" \
"$@" 2>&1                                  |
grep -v "YarnScheduler:70"                 |
grep -v "TransportResponseHandler:144"     |
grep -v "TransportClient:331"              |
grep -v "io.netty.channel.AbstractChannel" |
grep -v "ClosedChannelException"           |
grep -v "TransportResponseHandler:154"     |
grep -v "TransportRequestHandler:293"      |
grep -v "TransportResponseHandler:144"     |
tee /tmp/${APP_NAME}_spark.log
