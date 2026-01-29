PROJECT ?= grp-cec-kosmo-dev
region ?= europe-west1
PYTHON_VERSION ?= "3.12.2"
GITHUB_USER=${GIT_USER}
GITHUB_TOKEN=${GIT_TOKEN}
DEPS?=""
DEV?=false

.PHONY: install update add_dep lint fix
install:
	@./scripts/install.sh -v ${PYTHON_VERSION} -u ${GITHUB_USER} -t ${GITHUB_TOKEN}

update:
	@./scripts/update.sh

add_dep:
	@./scripts/add_dep.sh ${DEPS} -dev ${DEV}

lint:
	@cargo clippy

fix:
	@cargo clippy --fix

.PHONY: build run test

build:
	@rm -rf ./target/wheels
	@maturin build

develop:
	@rm -f python/cleansweep_core/*.so
	@maturin develop --release
