ARG base_image
FROM ${base_image}
USER root
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH /opt/conda/bin:$PATH
ENV THRIFT_VERSION 0.11.0

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
    sbt \
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
ENV SCALA_VERSION 2.11.7
ENV SCALA_DEB http://www.scala-lang.org/files/archive/scala-$SCALA_VERSION.deb

RUN wget --quiet --output-document=scala.deb $SCALA_DEB && \
    dpkg -i scala.deb && \
    rm -f *.deb

# Install sbt
RUN apt-get update && apt-get -y -q install \
    sbt \
    && apt-get clean

# Install conda
RUN /var/lib/dpkg/info/ca-certificates-java.postinst configure && \
    wget -q https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    bash ~/miniconda.sh -b -p /opt/conda && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    # clean up
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm ~/*.sh

RUN conda create -y -n chronon_py python=3.7
RUN conda install -y -q -n chronon_py --no-deps virtualenv
RUN  /opt/conda/envs/chronon_py/bin/pip install \
   flake8==5.0.4 flake8-quotes==3.3.1 thrift==0.11.0 click==7.0 thrift_json==0.1.0 nose>=1.3.7

# Install python build dependencies
RUN  /opt/conda/envs/chronon_py/bin/pip install build

WORKDIR /
RUN rm -rf /img-build
