#!/bin/bash
if [[ -z "$1" ]] ; then
  echo "Please provide PyPi repository to upload to."
  exit 1
fi
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
set -o xtrace
echo "Releasing to PyPi repository: $1"
echo "Finding working directory.."
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Removing old distributions..."
rm $SCRIPT_DIR/dist/*
echo "Compiling thrift files..."
thrift --gen py -out $SCRIPT_DIR/ai/chronon $SCRIPT_DIR/../thrift/api.thrift
echo "Running tests..."
cd $SCRIPT_DIR && tox
if [[ $? -eq 0 ]]; then
  echo "Checking for straggling files."
  git add .
#  git diff-index --cached --quiet HEAD
  if [[ $? -eq 0 ]]; then
    echo "Building"
    python -m build
#    echo "Running upload."
#    python -m twine upload --repository $1 dist/*
  else
    echo "Found straggling git files. Run git status for details."
    exit 1
  fi
else
  echo "Failed test. Aborting upload."
fi
