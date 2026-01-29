# <img src="https://github.com/sky-uk/GenAI-CleanSweep/assets/55431380/657ec859-4a84-4d40-aced-ade4cf120248" height="128"/>

The `cleansweep` package provides generic utilities used to provide the GenAI
Content Pipeline.

<a name="readme-top"></a>

## About The Project

The `cleansweep` package provides generic utilities used to provide the GenAI
Content Pipeline.

The project provides four key functionalities:

* `transform` - transform source JSON files from the source schema to the target
 data model
* `clean` - apply data cleansing functions to the curated source file
* `chunk` - apply model based chunking strategies to the cleansed data
* `embed` - apply the specified text embedding to the chunked data

Detailed technical documentation can be found
[here](https://fictional-adventure-j5g6r4o.pages.github.io/).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With

`cleansweep` is written in Python with some additional Rust components (`cleansweep-core`).

The core dependencies are:

* [langchain](https://python.langchain.com/docs/get_started/introduction) -
used for chunking and embedding of documents
* [openai](https://github.com/openai/openai-python) - used for translations, metadata generation and embedding of
documents
* [pandas](https://pandas.pydata.org/) - used for Data Frames
* [pluggy](https://pluggy.readthedocs.io/en/stable/) - used to provide plug in capabilities
* [Pydantic](https://docs.pydantic.dev/latest/) - used to define and validate
the data model and App settings
* [Jinja2](https://jinja.palletsprojects.com/en/3.1.x/) - used for prompt templating
* [Maturin](https://www.maturin.rs/) - used to build rust components
* [Great Expectations](https://greatexpectations.io/) - used for data quality checks

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Getting Started

### Prerequisites

* [gcloud CLI](https://cloud.google.com/cli?hl=en)
* [Python](https://www.python.org/downloads/) - at least version 3.10
* [UV](https://docs.astral.sh/uv/)
* [Rust](https://www.rust-lang.org/tools/install)

### Installation

You are recommended to use the Makefile to install the package:

```shell
make
```

### Unit Tests

Unit tests are executed by Cloud Build when a Pull Request is raised.

Tests can be executed locally by running `make test`.

### `local` tests

Some tests, such as those for the `cleansweep.clean.contractions` module, must be
executed locally. These can be marked with `local`.

```python
@pytest.mark.local
def a_local_only_test():
```

**Cloud Build will not execute tests marked `local`.**

### `slow` tests

Tests with a long runtime can be marked `slow`, these can be excluded from local
executions by passing "not slow" to your `pytest` command.

```shell
pytest tests -m "not slow"
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Environment Configuration

### Copying Cloud Run Environment Variables

The `copy-env` command allows you to copy environment variables from an existing Google Cloud Run job execution to your local `.env` file. This is useful for replicating production or staging configurations locally.

#### Command Usage

```shell
make copy-env EXECUTION_NAME=<execution-name> PROJECT=<project-id>
```

#### Parameters

* `EXECUTION_NAME` - The name of the Cloud Run execution to copy environment variables from
* `PROJECT` - The Google Cloud project ID where the Cloud Run job is located

#### Example

```shell
make copy-env EXECUTION_NAME=cleansweep-job-abc123 PROJECT=grp-cec-kosmo-dev
```

#### What it does

1. Retrieves the environment variables from the specified Cloud Run execution
2. Filters out sensitive variables (defined in `excluded_env_vars` in the script)
3. Updates your local `.env` file with the retrieved environment variables
4. Maintains security by excluding authentication tokens and other sensitive data

This command is particularly useful when:

* Setting up a new development environment
* Debugging issues that occurred in a specific Cloud Run execution
* Ensuring your local environment matches a deployed configuration

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Code Quality

Code quality should be assessed using the following packages and any issues
resolved.

* markdownlint
* ruff
* yamllint

Checks can be executed locally by running `make check`.

`ruff` fixes can be executed by running `make format`.

Checks will be executed by Cloud Build when a Pull Request is raised.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Type checking

`pyright` is used for static type checking. It is expected that no errors are pushed, although directives can be used where required.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Usage

See the [Wiki](https://github.com/sky-uk/GenAI-CleanSweep/wiki) for detailed
user guides.

<p align="right">(<a href="#readme-top">back to top</a>)</p>
