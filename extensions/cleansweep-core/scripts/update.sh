#!/bin/bash
# A script to update the installed packages in a python virtual environment.
# 
# usage: update.sh [-h] [-p PATH]
# 
# Dependencies:
#   - pip


source "$(dirname "$0")/colours.sh"
source "$(dirname "$0")/pip_install_helpers.sh"

print_help() {
  echo -e "Usage: $0 [-h]"
  echo -e "  -h                       Print this help message"
  echo -e "  -p <PATH>                [OPTIONAL] The path to the virtual environment"
}

if [[ "$1" == "-h" ]]
then
  print_help
  exit 0
fi

VENV_PATH=".venv"

while [ $# -gt 0 ]
do
  case $1 in
    -h)
      print_help
      exit 0
      ;;
    -p)
      shift
      VENV_PATH=$1
      ;;
    *)
      echo -e "Invalid argument $1"
      print_help
      exit 1
      ;;
  esac
  shift
done

IFS=$'\n' read -rd '' -a REQ_FILES <<< "$(find . -name requirements\*.txt -not -path ".venv/*" -not -path "*/.venv/*" -not -path "./build/*" -not -path "./dist/*")"


{
  echo -e "#########################################"
  echo -e "Using virtual environment: ${PURPLE}$VENV_PATH${RESET}"
  echo -e "Using requirements files:  "
  for file in "${REQ_FILES[@]}"
  do
    echo -e "                           ${PURPLE}$file${RESET}"
  done
  echo -e "#########################################"
  echo -e ""
}

venv_activate $VENV_PATH

# upgrade pip
pip_upgrade

# install requirements if requirements.txt exists
if [[ -n $REQ_FILES ]]
then
    for file in "${REQ_FILES[@]}"
    do
        if [[ -n $file ]]
        then
            pip_install -r $file
        fi
    done
fi

echo -e "${GREEN}Dependencies updated successfully.${RESET}"