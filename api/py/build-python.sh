#!/bin/bash
set -e

# Install the repo's Chronon python API
export CHRONON_PY_VERSION=$1
echo "-------------------------------------- $2 -----------------------------------"
#python -m build api/py