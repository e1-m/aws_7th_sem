FROM python:3.13-slim-bookworm
LABEL authors="e1_m"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    build-essential \
    g++ && \
    rm -rf /var/lib/apt/lists/*

ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app

COPY pyproject.toml ./
RUN uv sync --no-dev

COPY src src

CMD ["uv", "run", "python", "-m", "src.main"]