FROM python:3.12-slim

ENV UV_LINK_MODE=copy
ENV PATH="/root/.local/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src

RUN uv sync --no-dev

CMD ["uv", "run", "alphapulse", "run", "--config", "/app/settings.toml"]

