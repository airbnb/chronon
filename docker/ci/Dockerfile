FROM stripe/veneur:6.0.0 AS veneur

FROM containers.global.prod.stripe.io/stripe/build/ubuntu-20.04:latest

# A lot of the pre-reqs that we install mimic the ones installed by the upstream project Docker (.circleci/Dockerfile)
# Just that we're using Stripe's CI images and also using our Scala versions
ENV THRIFT_VERSION 0.11.0
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN curl -sSL "https://github.com/sbt/sbt/releases/download/v1.5.0/sbt-1.5.0.tgz" -o sbt-1.5.0.tgz
RUN tar xzvf sbt-1.5.0.tgz -C /usr/share/
ENV PATH $JAVA_HOME/bin:/opt/conda/bin:/usr/share/sbt/bin:$PATH
RUN export JAVA_HOME

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

RUN curl -sSL $SCALA_DEB -o scala.deb && \
    dpkg -i scala.deb && \
    rm -f *.deb

# Install conda
RUN /var/lib/dpkg/info/ca-certificates-java.postinst configure && \
    curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o ~/miniconda.sh && \
    bash ~/miniconda.sh -b -p /opt/conda && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    # clean up
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm ~/*.sh

RUN conda create -y -n zipline_py python=3.7
RUN conda install -y -q -n zipline_py --no-deps virtualenv
RUN  /opt/conda/envs/zipline_py/bin/pip install \
   flake8==5.0.4 build flake8-quotes==3.3.1 thrift==0.11.0 click==7.0 thrift_json==0.1.0 nose>=1.3.7 distlib>=0.3.4 filelock>=3.7.0 platformdirs>=2.5.2

WORKDIR /src
ENV PATH $PATH:/src/

CMD /src/jenkins-build.sh
