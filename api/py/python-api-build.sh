#!/bin/bash
set -o xtrace

export CHRONON_VERSION_STR=$1
export CHRONON_BRANCH_STR=$2
ACTION=$3

echo "Finding working directory.."
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Removing old distributions..."
rm -f $SCRIPT_DIR/dist/*

# The default action is "build"
if [[ -z "${ACTION}" ]] || [[ "${ACTION}" == "build" ]]; then
  PYPI_REPOSITORY="internal"
  echo "Running build..."
  cd $SCRIPT_DIR
  set -e
  python -m build
elif [[ "${ACTION}" == "release" ]]; then
  PYPI_REPOSITORY="chronon-pypi"
  # Sanity checks, git state, Run Tests, Build, Release
  # Make sure the version string doesn't contain SNAPSHOT if so it signifies development build and cannot be released.
  if [[ "${CHRONON_VERSION_STR}" == *"SNAPSHOT"* ]]; then
    echo "Python releases cannot be done for in development versions. Version: ${CHRONON_VERSION_STR}"
    exit 1
  fi
  echo "Running tests, git check, build and release..."
  cd $SCRIPT_DIR && tox
  if [[ $? -eq 0 ]]; then
    echo "Checking for straggling files."
    git add .
    git diff-index --cached --quiet HEAD
    if [[ $? -eq 0 ]]; then
      set -e
      echo "Building"
      python -m build
      echo "Releasing to PyPi repository: ${PYPI_REPOSITORY}"
      python -m twine upload --repository ${PYPI_REPOSITORY} dist/*
    else
      echo "Found straggling git files. Run git status for details."
      exit 1
    fi
  else
    echo "Failed test. Aborting upload."
  fi
else
  echo "Invalid action for Python API: ${ACTION}. Please select either [build, release]"
  exit 1
fi
