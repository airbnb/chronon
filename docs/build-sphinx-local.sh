#!/bin/bash
set -euxo pipefail

# Fast iteration flow for building docs
# You need to have run the following commands ahead of tiem
# >> pip3 install -r docs/sphinx-requirements.txt

# Install the repo's Chronon python API
# >> python3 -m build api/py
# >> pip3 install api/py/dist/chronon-ai*.tar.gz


BUILD_DIR=docs/build
# Cleanup old artifacts
rm -rf ${BUILD_DIR}

# Run the Sphinx build
sphinx-build -b html docs/source/ ${BUILD_DIR}/html

# Open the docs entry page locally
open ${BUILD_DIR}/html/index.html