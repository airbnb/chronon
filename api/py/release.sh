#!/bin/bash
set -o xtrace
if [[ -z "$1" ]] ; then
  echo "Please provide PyPi repository to upload to."
  exit 1
fi
echo "Releasing to PyPi repository: $1"
echo "Finding working directory.."
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Compiling thrift files..."
thrift --gen py -out $SCRIPT_DIR/ai/chronon $SCRIPT_DIR/../thrift/api.thrift
echo "Running tests..."
cd $SCRIPT_DIR && tox
if [[ $? -eq 0 ]]; then
  echo "Checking for straggling files."
  git add .
  git diff-index --cached --quiet HEAD
  if [[ $? -eq 0 ]]; then
    echo "Running upload."
    python setup.py sdist upload -r $1
  else
    echo "Found straggling git files. Run git status for details."
    exit 1
  fi
else
  echo "Failed test. Aborting upload."
fi
