PROJECT ?= grp-cec-kosmo-dev
region ?= europe-west1
PYTHON_VERSION ?= "3.12.2"
GITHUB_USER=${GIT_USER}
GITHUB_TOKEN=${GIT_TOKEN}

.PHONY: install
install:
	@uv sync --all-extras --no-install-project --no-progress
	@uv run maturin build -m ./extensions/cleansweep-core/Cargo.toml
	@uv run pip install ./extensions/cleansweep-core/target/wheels/*.whl

.PHONY: copy-env
copy-env:
	@uv run ./scripts/copy_cloud_run_env.sh ${EXECUTION_NAME} ${PROJECT}

.PHONY: check fix test test-full deploy create-image deploy-job execute-job test-file-coverage
deploy: create-image deploy-job

image: create-image

create-image:
	gcloud builds submit --project ${PROJECT} --config .cloudbuild/local/create_image.yaml --region=${region}

check:
	@echo "\033[0;34m*** Running Python checks ***\033[0m"
	@echo "\033[1;33mruff\033[0m"
	-@ uv run ruff check || true
	@echo ""
	@echo "\033[1;33mpyright\033[0m"
	-@ uv run pyright || true
	@echo ""
	@echo "\033[0;34m*** Running Yaml checks ***\033[0m"
	@echo "\033[1;33myamllint\033[0m"
	-@ uv run yamllint -c .github/linters/.yamllint . || true
	@echo ""
	@echo "\033[0;34m*** Running markdown checks ***\033[0m"
	@echo "\033[1;33mpymarkdown\033[0m"
	@echo ""
	-@ uv run pymarkdown --config .github/linters/pymarkdown.yaml scan . || true

fix:
	@echo "\033[0;34m*** Making Python fixes ***\033[0m"
	@echo "\033[1;33mruff\033[0m"
	-@ uv run ruff check --fix || true
	@echo "\033[0;34m*** Making Markdown fixes ***\033[0m"
	@echo "\033[1;33mpymarkdown\033[0m"
	-@ uv run pymarkdown --config .github/linters/pymarkdown.yaml fix . || true

test:
	@uv run coverage run --branch -m pytest tests -vv -p no:warnings \
      --html="tests/pytest_html/test_report.html" --self-contained-html \
			-m "not slow"
	@uv run coverage html
	@uv run coverage report --fail-under=90

test-full:
	@uv run coverage run --branch -m pytest tests -vv -p no:warnings \
      --html="tests/pytest_html/test_report.html" --self-contained-html
	@uv run coverage html
	@uv run coverage report --fail-under=90

deploy-job:
	gcloud run jobs replace config/cloud_run.yml --project ${PROJECT} --region=${region}

execute-job:
	gcloud run jobs execute cleansweep --project ${PROJECT} --region=${region} --args="-m,${ARGS}" --update-env-vars="input_file_uri=${INPUT_FILE_URI},config_file_uri=${CONFIG_FILE_URI}" --wait

test-file-coverage:
	@uv run coverage run --branch -m pytest ${TEST_FILE} -vv -p no:warnings \
      --html="tests/pytest_html/test_report.html" --self-contained-html \
			-m "not slow"
	@uv run coverage html
	@uv run coverage report --fail-under=90
