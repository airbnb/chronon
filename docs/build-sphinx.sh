#!/bin/bash
set -e

BUILD_DIR=docs/build
VIRTUAL_ENV=${BUILD_DIR}/sphinx

# Cleanup old artifacts
# rm -rf ${BUILD_DIR}

# Setup Virtualenv for Sphinx with all its dependencies
virtualenv ${VIRTUAL_ENV}
source ${VIRTUAL_ENV}/bin/activate
pip install -r docs/sphinx-requirements.txt

# Install the repo's Chronon python API
# python -m build api/py
pip install api/py/dist/chronon-ai*.tar.gz

# Run the Sphinx build
${VIRTUAL_ENV}/bin/sphinx-build -b html docs/source/ ${BUILD_DIR}/html

# Exit the virtualenv
deactivate

# Open the docs entry page locally
open docs/build/html/index.html
