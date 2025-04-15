# Set a default base image version (e.g., ubuntu:25.04)
ARG base_image=ubuntu:25.04

# Use the argument in the FROM statement
FROM ${base_image}

ARG DEBIAN_FRONTEND=noninteractive
ENV LC_ALL=C.utf-8
ENV THRIFT_VERSION 0.13.0

RUN apt-get update && apt-get install -y software-properties-common && \
    apt-add-repository ppa:deadsnakes/ppa \
    && apt-get install -y \
  automake \
  autotools-dev \
  bison \
  byacc \
  build-essential \
  ca-certificates \
  chromium-browser \
  clang \
  cmake \
  curl \
  docker.io \
  gettext-base \
  git \
  git-lfs \
  jq \
  libbz2-dev \
  libc++-dev \
  libc++abi-dev \
  libcurl4-openssl-dev \
  libffi-dev \
  libgdbm-dev \
  liblzma-dev \
  libmysqlclient-dev \
  libncurses5-dev \
  libnss3-dev \
  libpcre3 \
  libpcre3-dev \
  libreadline-dev \
  libsqlite3-dev \
  libssl-dev \
  libtinfo-dev \
  locales \
  pkg-config \
  python3.8 \
  python3.8-dev \
  python3.8-distutils \
  zlib1g-dev \
  openjdk-11-jdk \
  zip && \
  rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++ 100 && \
    update-alternatives --install /usr/bin/cc cc /usr/bin/clang 100

# Install Bazel
ADD https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-amd64 /usr/local/bin/bazel
RUN chmod 755 /usr/local/bin/bazel

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


# Create a non-root user
RUN useradd -m -s /bin/bash chronon

# Set working directory
WORKDIR /home/chronon
RUN chown -R chronon:chronon /home/chronon/
USER chronon

# Cmd to run when starting the container
CMD ["/bin/bash"]