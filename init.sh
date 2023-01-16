#!/usr/bin/env bash

# Script to initialize chronon as sub directory

set +euxo pipefail
GRAY='\033[0;37m'
NC='\033[0m'
BOLD='\033[1m'
BLUE='\033[34m'
GREEN='\033[32m'
MAGENTA='\033[35m'

# Display logo:
echo -e "${BLUE}
                                ╔═╗┬ ┬┬─┐┌─┐┌┐┌┌─┐┌┐┌
                                ║  ├─┤├┬┘│ │││││ ││││
                                ╚═╝┴ ┴┴└─└─┘┘└┘└─┘┘└┘
${NC}"

echo -e "Installing chronon-ai python package $NC"
pip3 install chronon-ai
echo -e "\n"
CWD="$(pwd)/chronon"
## download initial repo
if [ -d "chronon" ]; then
  echo -e "Folder named 'chronon' already exists. Not initializing chronon into current folder. $NC"
elif [[ "$(pwd)" == */chronon ]] && [ -f "teams.json" ]; then
  echo -e "You are already in a chronon repo. Not initializing repo again"
  CWD="$(PWD)"
else
  echo -e "Setting up 'chronon' folder $NC"
  mkdir chronon
  pushd chronon || exit
  curl -o repo.tar.gz https://storage.googleapis.com/chronon/releases/repo.tar.gz
  tar -xzf repo.tar.gz
  rm repo.tar.gz
  popd || exit
fi

SHELL_FILE="$HOME/.$(echo $SHELL | awk -F/ '{print $NF}')rc"
echo -e "Using shell initializer file: ${SHELL_FILE}"

if [[ $PYTHONPATH == *"$CWD"* ]];
then
  echo -e "${CWD} Already found in PYTHONPATH: ${PYTHONPATH}${NC}"
else
  echo -e "Appending chronon repo path $CWD to PYTHONPATH: ${PYTHONPATH}${NC}"
  cat >> "$SHELL_FILE" <<- EOF
export PYTHONPATH=\$PYTHONPATH:${CWD}
EOF
  export PYTHONPATH=${PYTHONPATH}:${CWD}
  echo -e "\n!!Please run:!!\n  ${BLUE}source $SHELL_FILE ${NC}"
fi

echo -e "\n!!Initialized repo, edit ${BLUE}'./chronon/teams.json'${NC} to specify your spark_submit path!!${NC}\n"