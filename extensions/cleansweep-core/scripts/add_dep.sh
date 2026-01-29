#!/bin/bash
# A script to create add a dependency to a python virtual environment.
# 
# usage: add_dep.sh [package name] [-h] [-dev] [-p PATH]
#
# Dependencies:
#   - pip

source "$(dirname "$0")/colours.sh"
source "$(dirname "$0")/pip_install_helpers.sh"

print_help() {
  echo -e "Usage: $0 [package name] [-h] [-dev]"
  echo -e "  [package name]           The name of the package(s) to install"
  echo -e "  -h                       Print this help message"
  echo -e "  -dev <BOOLEAN>           [OPTIONAL] Boolean flag to install development dependencies"
  echo -e "  -p <PATH>                [OPTIONAL] The path to the virtual environment"
}

if [[ $# -eq 0 ]]
then
  echo -e "${RED}No package name provided${RESET}"
  print_help
  exit 1
fi

DEV=false
PACKAGES=() # Step 1: Declare an array to store package names
VENV_PATH=".venv"

while [ $# -gt 0 ]
do
  case $1 in
    -h)
      print_help
      exit 0
      ;;
    -dev)
      shift
      if [[ $1 == true || $1 == false ]]
      then
        DEV=$1
      else
        echo -e "${RED}Invalid -dev value $1${RESET}"
        print_help
        exit 1
      fi
      ;;
    -p)
      shift
      VENV_PATH=$1
      ;;
    *)
      if [[ $1 =~ ^- ]]
      then
        echo -e "${RED}Invalid argument $1${RESET}"
        print_help
        exit 1
      elif [[ $1 =~ ',' ]]
      then
        IFS=',' read -ra PKG <<< "$1"
        for package in ${PKG[@]}
        do
          PACKAGES+=($package)
        done
      else
        PACKAGES+=($1) # Step 2: Append package name to PACKAGES array
      fi
      ;;
  esac
  shift
done

if [[ ${#PACKAGES[@]} -eq 0 ]]
then
  echo -e "${RED}No package name provided${RESET}"
  print_help
  exit 1
fi

venv_activate $VENV_PATH

# Step 3: identify the requirements file
if [[ $DEV == true ]]
then
  REQ_FILE="requirements.dev.txt"
else
  REQ_FILE="requirements.txt"
fi

# Step 4: check if the requirements file exists
if [[ ! -f $REQ_FILE ]]
then
  echo -e "${YELLOW}No requirements file found, creating...${RESET}"
  touch $REQ_FILE
fi

# Before Step 6: Ensure REQ_FILE ends with a newline
if [ -n "$(tail -c1 "$REQ_FILE")" ]; then
  echo >> "$REQ_FILE"
fi

# Step 5: install the package(s)
pip_install ${PACKAGES[@]}

# Step 6: add the package(s) to the requirements file from pip freeze
for package in ${PACKAGES[@]}
do
  INSTALLED=$(pip freeze | grep "^$package==")
  if [[ -z $INSTALLED ]]
  then
    echo -e "${RED}Failed to add $package to ${REQ_FILE}${RESET}"
    continue
  fi

  echo $INSTALLED >> $REQ_FILE
done

