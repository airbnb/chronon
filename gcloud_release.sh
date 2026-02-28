#!/usr/bin/env bash

# Steps to setup gcloud push ability
# Download gcloud cli and unpack: https://cloud.google.com/sdk/docs/install-sdk#installing_the_latest_version
# Install gcloud cli: ./google-cloud-sdk/install.sh
# Connect your account: ./google-cloud-sdk/bin/gcloud init
# Tell Nikhil to run this for permissions: gsutil iam ch user:<you email>@gmail.com:objectCreator,objectViewer gs://chronon


set -euxo pipefail

thrift --gen py -out api/py/ai/chronon api/thrift/api.thrift

DOC_BUILD=docs/build
VIRTUAL_ENV=${DOC_BUILD}/sphinx

# Cleanup old artifacts
rm -rf ${DOC_BUILD}

# Setup Virtualenv for Sphinx with all its dependencies
virtualenv ${VIRTUAL_ENV}
source ${VIRTUAL_ENV}/bin/activate
pip install -r docs/sphinx-requirements.txt

# Run the Sphinx build
${VIRTUAL_ENV}/bin/sphinx-build -b html docs/source/ ${DOC_BUILD}/html

deactivate

rm -rf releases
mkdir releases

mv ${DOC_BUILD}/html/* releases/

echo "Wrote doc site into ./releases"
tree -L 1 releases

gsutil -m -o GSUtil:parallel_process_count=1 -o GSUtil:parallel_thread_count=24 cp -r releases/* gs://chronon