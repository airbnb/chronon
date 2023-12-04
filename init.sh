#!/usr/bin/env bash

# Script to initialize chronon as sub directory

#set -euxo pipefail
GRAY='\033[0;34m'
NC='\033[0m'
BOLD='\033[1m'
BLUE='\033[34m'
PC=$BLUE$BOLD
GREEN='\033[32m'
MAGENTA='\033[35m'

# Display logo:
echo -e "${PC}
                                ╔═╗┬ ┬┬─┐┌─┐┌┐┌┌─┐┌┐┌
                                ║  ├─┤├┬┘│ │││││ ││││
                                ╚═╝┴ ┴┴└─└─┘┘└┘└─┘┘└┘
${NC}"

echo -e "Installing ${PC}chronon-ai${NC} python package"
pip3 install chronon-ai
echo -e "-------------\n"

## download initial repo
CWD="$(pwd)/chronon"
if [ -d "chronon" ]; then
  echo -e "Folder named 'chronon' already exists. Not initializing chronon into current folder."
elif [[ "$(pwd)" == */chronon ]] && [ -f "teams.json" ]; then
  echo -e "You are already in a chronon repo. Not initializing repo again"
  CWD="$(PWD)"
else
  echo -e "Setting up chronon folder"
  mkdir chronon
  pushd chronon || exit
  curl -o repo.tar.gz https://chronon.ai/repo.tar.gz
  tar -xzf repo.tar.gz
  rm repo.tar.gz
  popd || exit
  echo -e "${PC}Finished setting up chronon folder${NC}"
fi
echo -e "-------------\n"
SHELL_FILE="$HOME/.$(echo $SHELL | awk -F/ '{print $NF}')rc"
echo -e "Using shell initializer file: ${PC}${SHELL_FILE}${NC}"

if [[ $PYTHONPATH == *"$CWD"* ]];
then
  echo -e "${PC}${CWD}${NC} Already found in PYTHONPATH: ${PC}${PYTHONPATH}${NC}"
else
  echo -e "Appending chronon repo path ${PC}$CWD${NC} to PYTHONPATH: ${PC}${PYTHONPATH}${NC}"
  cat >> "$SHELL_FILE" <<- EOF
export PYTHONPATH=${CWD}:\$PYTHONPATH
EOF
fi

if [[ $SPARK_SUBMIT_PATH == *"spark-submit"* ]];
then
  echo -e "SPARK_SUBMIT_PATH is already set to ${PC}${SPARK_SUBMIT_PATH}${NC}. Not downloading spark"
else
  echo -e "Setting up spark for local testing.."

  pushd ~ || exit
  curl -O "https://archive.apache.org/dist/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz"
  echo -e "Unpacking spark.."
  tar xf spark-3.2.4-bin-hadoop3.2.tgz
  echo -e "Setting env variables.."
  popd || exit
  cat >> "$SHELL_FILE" <<- EOF
export SPARK_SUBMIT_PATH=/Users/$USER/spark-3.2.4-bin-hadoop3.2/bin/spark-submit
export SPARK_LOCAL_IP="127.0.0.1"
# This redirects to scala 2.12 within run.py - spark struggles with 2.13 hence we need this out of sync version.
export SPARK_VERSION="3.1.1"
EOF
  echo -e "Done setting up spark"
fi

echo -e "\nInitialized repo, edit ${PC}'./chronon/teams.json'${NC} to specify your spark_submit path.\n"
echo -e "Please run \`${BOLD}source $SHELL_FILE${NC}\` to update your shell with the new env variables"



