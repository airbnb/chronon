#!/bin/bash
set -o xtrace

export CHRONON_PY_VERSION=$1
ACTION=$2

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
  # Check git state, Run Tests, Build, Release
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
