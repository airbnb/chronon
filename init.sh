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
export PYTHONPATH=\$PYTHONPATH:${CWD}
EOF
  export PYTHONPATH=${PYTHONPATH}:${CWD}
fi

tree -L 1 chronon
echo -e "\nInitialized repo, edit ${PC}'./chronon/teams.json'${NC} to specify your spark_submit path.\n"