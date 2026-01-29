#!/bin/bash
# A script to create a python virtual environment and install dependencies
# for a given python version.
# 
# usage: py_venv.sh [-h] [-u GIT_USER] [-t GIT_TOKEN] [-v VERSION] [-r REQUIREMENTS] [-p PATH]
# 
# Dependencies:
#   - python3
#   - python3-venv
#   - pip
#   - pyenv

source "$(dirname "$0")/colours.sh"
source "$(dirname "$0")/pip_install_helpers.sh"

print_help() {
  echo -e "Usage: $0 [-h] [-v VERSION] [-r REQUIREMENTS] [-p PATH]"
  echo -e "  -h                       Print this help message"
  echo -e "  -u <GIT_USER>            The git user to use"
  echo -e "  -t <GIT_TOKEN            The git token to use"
  echo -e "  -v <VERSION>             [OPTIONAL] The python version to use"
  echo -e "  -r <REQUIREMENTS>        [OPTIONAL] The requirements file to use"
  echo -e "  -p <PATH>                [OPTIONAL] The path to the virtual environment"
}

PYENV=$(pyenv --version)

if [[ -z $PYENV ]]
then
  echo -e "${RED}pyenv is not installed. Please install pyenv.${RESET}"
  exit 1
fi

if [[ "$1" == "-h" ]]
then
  print_help
  exit 0
fi

PYTHON_VERSION=$(pyenv version | cut -d " " -f 1)

IFS=$'\n' read -rd '' -a REQ_FILES <<< "$(find . -name requirements\*.txt -not -path ".venv/*" -not -path "*/.venv/*" -not -path "./build/*" -not -path "./dist/*")"

INPUT_REQFILES=()

VENV_PATH=".venv"

while [ $# -gt 0 ]
do
  case $1 in
    -h)
      print_help
      exit 0
      ;;
    -v)
      shift
      PYTHON_VERSION=$1
      ;;
    -r)
      shift
      INPUT_REQFILES+=($1)
      ;;
    -p)
      shift
      VENV_PATH=$1
      ;;
    -u)
      shift
      GIT_USER=$1
      ;;
    -t)
      shift
      GIT_TOKEN=$1
      ;;
    *)
      echo -e "Invalid argument $1"
      print_help
      exit 1
      ;;
  esac
  shift
done

if [[ -z $GIT_USER || -z $GIT_TOKEN ]]
then
  echo -e "${RED}Please provide a git user and token${RESET}"
  exit 1
fi

if [[ -n $INPUT_REQFILES ]]
then
  REQ_FILES=$INPUT_REQFILES
fi

{
  echo -e "#########################################"
  echo -e "Using python version:      ${PURPLE}$PYTHON_VERSION${RESET}"
  echo -e "Using virtual environment: ${PURPLE}$VENV_PATH${RESET}"
  echo -e "Using requirements files:  "
  for file in "${REQ_FILES[@]}"
  do
    echo -e "                           ${PURPLE}$file${RESET}"
  done
  echo -e "#########################################"
  echo -e ""
}

# check if desired version exists
if [[ $(pyenv versions | grep -c $PYTHON_VERSION) -eq 0 ]]
then
  echo -e "${YELLOW}Python version $PYTHON_VERSION not found. Install it...${RESET}"
  pyenv install $PYTHON_VERSION
fi

if [[ $? -ne 0 ]]
then
  echo -e "${RED}Failed to install python version $PYTHON_VERSION${RESET}"
  exit 1
fi

# set python version
pyenv local $PYTHON_VERSION
eval "$(pyenv init --path)"
echo -e "${GREEN}Python version set to $(python --version)${RESET}"

# create virtual environment
if [[ -d $VENV_PATH ]]
then
  echo -e "${YELLOW}Virtual environment already exists. Removing...${RESET}"
  if [[ $(command -v deactivate) == "deactivate" ]]
  then
    deactivate
  fi
  rm -rf $VENV_PATH
  echo -e "Virtual environment removed."
  echo -e ""
fi

echo -e "Creating virtual environment..."
python -m venv $VENV_PATH

if [[ $? -ne 0 ]]
then
  echo -e "${RED}Failed to create virtual environment${RESET}"
  exit 1
fi

# activate virtual environment
source $VENV_PATH/bin/activate

if [[ $? -ne 0 ]]
then
  echo -e "${RED}Failed to activate virtual environment${RESET}"
  exit 1
fi


# upgrade pip
pip_upgrade
        
# install keyring
pip_install keyrings.google-artifactregistry-auth

# other general installs
pip_install black ipython pylint ipykernel

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

echo -e "${GREEN}Virtual environment created successfully.${RESET}"
echo -e ""


{
  echo -e "#########################################"
  echo -e "To activate the virtual environment, run:"
  echo -e "source $VENV_PATH/bin/activate"
  echo -e "#########################################"
  echo -e ""
}