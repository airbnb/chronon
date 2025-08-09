#!/bin/bash

set -efx

# Clean up any existing thrift_build directory
rm -rf thrift_build
mkdir thrift_build
pushd thrift_build

# Download archive and verify it matches our expected checksum.
THRIFT_HTTP_ARCHIVE=https://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz
THRIFT_ARCHIVE=thrift.tar.gz
THRIFT_EXPECTED_CHECKSUM_SHA256=7ad348b88033af46ce49148097afe354d513c1fca7c607b59c33ebb6064b5179
curl "$THRIFT_HTTP_ARCHIVE" -o "$THRIFT_ARCHIVE"
THRIFT_ACTUAL_CHECKSUM_SHA256=$(sha256sum "$THRIFT_ARCHIVE" | awk '{ print $1 }')
if [ "$THRIFT_EXPECTED_CHECKSUM_SHA256" != "$THRIFT_ACTUAL_CHECKSUM_SHA256" ]; then
    echo "Checksum does not match expected value" >&2
    echo " - location: $THRIFT_HTTP_ARCHIVE" >&2
    echo " - expected: $THRIFT_EXPECTED_CHECKSUM_SHA256" >&2
    echo " - obtained: $THRIFT_ACTUAL_CHECKSUM_SHA256" >&2
    exit 1
fi

echo "Building Thrift from source"
# Build thrift from source.
mkdir src 
tar zxvf thrift.tar.gz -C src --strip-components=1
pushd src

# Install build dependencies
sudo apt update
sudo apt install -y build-essential libssl-dev pkg-config flex bison

# Configure and build
./configure --without-python --without-cpp
make

# Install
sudo make install

popd

# Verify installation
thrift -version

popd

echo "Thrift 0.13.0 installation completed successfully"