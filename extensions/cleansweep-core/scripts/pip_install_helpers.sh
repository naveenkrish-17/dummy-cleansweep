
source "$(dirname "$0")/colours.sh"

export PIP_INSTALl_TEMP=$(mktemp)

venv_activate() {

  if [[ -z $1 ]]
  then
    echo -e "${RED}No virtual environment path provided${RESET}"
    exit 1
  fi

  VENV_PATH=$1

  if [[ -z $VIRTUAL_ENV ]]
  then
    echo -e "${YELLOW}Activating virtual environment at ${VENV_PATH}...${RESET}"
    echo -e ""
    source $VENV_PATH/bin/activate
  fi
}

post_install() {

  if [[ $# -ne 1 ]]
  then
    echo -e "${RED}Invalid number of arguments${RESET}"
    exit 1
  fi

  if [[ $1 -ne 0 ]]
  then
    echo -e "${RED}Failed to install packages${RESET}"
    cat $PIP_INSTALl_TEMP
    exit 1
  fi

  echo $(tail -n 1 $PIP_INSTALl_TEMP)
  echo -e ""
}

pip_install() {

  if [[ $1 == "-r" ]]
  then
    shift
    echo -e "${BLUE}Installing from ${@}...${RESET}"
    pip install -r $@ > $PIP_INSTALl_TEMP
    post_install $?
    return
  fi

  echo -e "${BLUE}Installing ${@}...${RESET}"
  pip install $@ > $PIP_INSTALl_TEMP
  post_install $?
}

pip_upgrade() {
  echo -e "${BLUE}Install and upgrade pip, setuptools and wheel${RESET}"
  python -m pip install --upgrade pip setuptools wheel pip-tools > $PIP_INSTALl_TEMP
  post_install $?
}

split_deps() {
  if [[ $# -ne 1 ]]
  then
    echo ""
  fi

  echo ${1//,/ }
}