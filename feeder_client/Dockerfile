FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim

# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates git build-essential vim nano \
    && rm -rf /var/lib/apt/lists/* \

# Download the latest installer
RUN curl -Ls https://astral.sh/uv/install.sh | sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

ADD . /app
WORKDIR /app
RUN uv sync

CMD ["sleep", "infinity"]
