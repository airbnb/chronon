#!/bin/bash

# Builds a tar file with
#   1. Chronon spark jar that can drive all workflows
#   2. Chronon doc site
#   3. Test repo that can be used by init
set -euxo pipefail

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$BRANCH" != "master" ]]; then
  echo "$(tput bold) You are not on master!"
  echo "$(tput sgr0) Are you sure you want to release? (y to continue)"
  read response
  if [[ "$response" != "y" ]]; then
    echo "Not releasing then. Wise choice."
    exit 0
  fi
fi

thrift --gen py -out api/py/ai/chronon api/thrift/api.thrift

DOC_BUILD=docs/build
VIRTUAL_ENV=${DOC_BUILD}/sphinx

# Cleanup old artifacts
rm -rf ${DOC_BUILD}

# Setup Virtualenv for Sphinx with all its dependencies
virtualenv ${VIRTUAL_ENV}
source ${VIRTUAL_ENV}/bin/activate
pip install -r docs/sphinx-requirements.txt

# Install the repo's Chronon python API
rm -rf api/py/dist/
python -m build api/py
pip install api/py/dist/chronon-ai*.tar.gz

# Run the Sphinx build
${VIRTUAL_ENV}/bin/sphinx-build -b html docs/source/ ${DOC_BUILD}/html

# Exit the virtualenv
deactivate

sbt spark_uber/assembly
SBT_JAR=$(ls -rt spark/target/scala-2.11/ | grep ".*uber-assembly.*\.jar$" |tail -n 1 | awk '{print $(NF)}')

rm -rf releases
mkdir releases
mv ${DOC_BUILD}/html releases/docsite
rm -rf ${DOC_BUILD}
tar -zcf releases/repo.tar.gz -C api/py/test/sample .
mkdir -p releases/jar/
mv "spark/target/scala-2.11/${SBT_JAR}" releases/jar/
cp init.sh releases/init.sh

echo "Wrote release artifacts into ./releases"
tree -L 2 releases

