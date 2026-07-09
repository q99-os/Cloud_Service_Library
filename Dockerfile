FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /code

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/code/.venv/bin:$PATH"

# Install locked dependencies (incl. dev group for in-container test/debug)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY cloud_services ./cloud_services
COPY tests ./tests

RUN uv sync --frozen
