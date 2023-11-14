#!/bin/bash
#
#    Copyright (C) 2023 The Chronon Authors.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

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
