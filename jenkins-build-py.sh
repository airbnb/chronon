#! /usr/bin/env bash

. /usr/stripe/bin/docker/stripe-init-build

echo "Kicking off Python tests and build"
source /opt/conda/etc/profile.d/conda.sh
conda activate zipline_py
pushd api
thrift --gen py -out py/ai/chronon thrift/api.thrift   # Generate thrift files
cd py                                                  # Go to Python module
pip install -r requirements/dev.txt                    # Install latest requirements
tox                                                    # Run tests
/opt/conda/envs/zipline_py/bin/python3.7 -m build                                        # Build
mkdir -p /src/build                                    # Create build directory
cp -rv dist/* /src/build                               # Copy build files to /src/build so they can be accessed by the artifact publish box 
popd