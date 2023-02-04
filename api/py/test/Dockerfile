FROM openjdk:8-jdk-slim-buster
USER root

ENV JAVA_HOME /usr/local/openjdk-8/
ENV PYTHONPATH /repos/chronon/api/py/:/repos/chronon/api/py/test/sample/:$PYTHONPATH
ENV USER chronon_docker
ENV THRIFT_VERSION 0.13.0
ENV SPARK_SUBMIT_PATH=/spark-2.4.8-bin-hadoop2.7/bin/spark-submit

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        python3.7 \
        python3-pip \
        python3.7-dev \
        python3-setuptools \
        python3-wheel \
        python3-venv \
        automake \
        bison \
        cmake \
        curl \
        flex \
        g++ \
        git \
        libboost-dev \
        libboost-filesystem-dev \
        libboost-program-options-dev \
        libboost-system-dev \
        libboost-test-dev \
        libevent-dev \
        libssl-dev \
        libtool \
        make \
        && apt-get clean


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


# set python3
RUN mkdir env
RUN python3 -m venv env
RUN mkdir repos
RUN /bin/bash -c "source env/bin/activate && pip install chronon-ai"

# set spark submit
RUN curl -O https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz
RUN tar xf spark-2.4.8-bin-hadoop2.7.tgz
RUN rm -rf /img-build
WORKDIR repos/chronon/api/py/test/sample/


