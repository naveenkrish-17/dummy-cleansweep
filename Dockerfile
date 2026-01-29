FROM python:3.12.10-slim AS builder

# System deps + uv
RUN apt-get update && apt-get install -y curl build-essential && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Make uv less memory-hungry during install
# - no bytecode compilation while building
# - copy link mode (safe in Docker)
# - single concurrent build
ENV UV_COMPILE_BYTECODE=0 \
    UV_LINK_MODE=copy \
    UV_CONCURRENT_BUILDS=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 🔐 Copy only lock + pyproject to maximize Docker layer cache
COPY pyproject.toml uv.lock ./

# ⚡ Install third‑party deps ONLY (no project)
RUN uv sync --locked --no-dev --no-install-project --no-editable

# Now copy the rest of your source
ADD . .

# Install Rust toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:$PATH"

# Build and install the Rust extension with low parallelism
ENV CARGO_BUILD_JOBS=1
RUN uv run maturin build -m ./extensions/cleansweep-core/Cargo.toml
RUN uv run pip install ./extensions/cleansweep-core/target/wheels/*.whl

# Finally, install CleanSweep
RUN uv pip install .

# Export the venv for the runtime stage
ENV PATH="/app/.venv/bin:$PATH"

FROM python:3.12.10-slim AS app
RUN apt-get update && apt-get install -y && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY ./app /app/

# Install spaCy model (kept here to avoid inflating builder layer)
RUN python -c 'from spacy.cli.download import download as d; d("en_core_web_sm")'

# NLTK resource
RUN python -c 'import nltk; nltk.download("punkt_tab")'
