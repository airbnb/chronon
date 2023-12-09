FROM cimg/base:2020.01 
USER root
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV THRIFT_VERSION 0.13.0

WORKDIR /img-build


# Add sbt repo to sources list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add

# Install prereqs
RUN apt-get update && apt-get -y -q install \
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
    openjdk-8-jdk \
    pkg-config \
    python3.7 \
    python3-pip \
    python3-setuptools \
    sbt \
    vim \
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

# Install Scala
ENV SCALA_VERSION 2.12.12
ENV SCALA_DEB http://www.scala-lang.org/files/archive/scala-$SCALA_VERSION.deb

RUN wget --quiet --output-document=scala.deb $SCALA_DEB && \
    dpkg -i scala.deb && \
    rm -f *.deb

RUN rm -rf /img-build

# Download and install Apache Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz | tar xvz -C /opt \
    && mv /opt/spark-3.1.1-bin-hadoop3.2 /opt/spark

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Copy your chronon-ai code into the container
RUN pip install chronon-ai

CMD ["tail", "-f", "/dev/null"]
