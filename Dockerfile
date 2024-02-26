# Start from a Debian base image
FROM openjdk:8-jre-slim

# Set this manually before building the image, requires a local build of the jar

ENV CHRONON_JAR_PATH=spark/target-embedded/scala-2.12/your_build.jar

# Update package lists and install necessary tools
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-dev \
    python3-setuptools \
    vim \
    wget \
    procps \
    python3-pip

ENV THRIFT_VERSION 0.13.0
ENV SCALA_VERSION 2.12.12

# Install thrift
RUN curl -sSL "http://archive.apache.org/dist/thrift/$THRIFT_VERSION/thrift-$THRIFT_VERSION.tar.gz" -o thrift.tar.gz \
       && mkdir -p /usr/src/thrift \
       && tar zxf thrift.tar.gz -C /usr/src/thrift --strip-components=1 \
       && rm thrift.tar.gz \
       && cd /usr/src/thrift \
       && ./configure  --without-python --without-cpp \
       && make \
       && make install \
       && cd / \
       && rm -rf /usr/src/thrift

RUN curl https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb -k -o scala.deb && \
    apt install -y ./scala.deb && \
    rm -rf scala.deb /var/lib/apt/lists/*

ENV SCALA_HOME="/usr/bin/scala"
ENV PATH=${PATH}:${SCALA_HOME}/bin

## Download spark and hadoop dependencies and install

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
ENV SPARK_VERSION=${SPARK_VERSION:-"3.1.1"}
ENV HADOOP_VERSION=${HADOOP_VERSION:-"3.2"}
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
RUN mkdir -p /opt/spark/spark-events
WORKDIR ${SPARK_HOME}


RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz \
 && tar xvzf spark.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark.tgz


# Install python deps
COPY quickstart/requirements.txt .
RUN pip3 install -r requirements.txt


ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"

COPY quickstart/conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:/srv/chronon/:$PYTHONPATH

# If trying a standalone docker cluster
WORKDIR ${SPARK_HOME}
# If doing a regular local spark box.
WORKDIR /srv/chronon

ENV DRIVER_JAR_PATH="/srv/spark/spark_embedded.jar"

COPY api/py/test/sample ./
COPY quickstart/mongo-online-impl /srv/onlineImpl
COPY $CHRONON_JAR_PATH "$DRIVER_JAR_PATH"

ENV CHRONON_DRIVER_JAR="$DRIVER_JAR_PATH"
