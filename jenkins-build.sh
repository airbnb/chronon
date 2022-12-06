#! /usr/bin/env bash

. /usr/stripe/bin/docker/stripe-init-build

echo Building Chronon.

# The steps in this file largely follow the commands in .circleci/config.yml

# Build the Scala code
# TODO: Enable tests once we have them passing (currently a large number fail)
# Increase if we see OOMs
export SBT_OPTS="-XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=4G -Xmx4G -Xms2G"
sbt "++ 2.12.12 compile"

echo Kicking off Python tests and build

source /opt/conda/etc/profile.d/conda.sh
conda activate zipline_py
pushd api
thrift --gen py -out py/ai/chronon thrift/api.thrift   # Generate thrift files
cd py                                                  # Go to Python module
pip install -r requirements/dev.txt                    # Install latest requirements
tox                                                    # Run tests
python -m build                                        # Build
popd
