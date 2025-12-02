FROM python:3.14

ARG NODE_MAJOR=24

# inspiration from https://github.com/ml-tooling/ml-workspace/blob/master/Dockerfile
ENV PYTHONUNBUFFERED=1

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

# This Dockerfile adds a non-root 'vscode' user with sudo access. However, for Linux,
# this user's GID/UID must match your local user UID/GID to avoid permission issues
# with bind mounts. Update USER_UID / USER_GID if yours is not 1000. See
# https://aka.ms/vscode-remote/containers/non-root-user for details.

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# --- Common tools and certificates ---
RUN apt-get update && \
    # core packages
    apt-get -y install --no-install-recommends apt-utils ca-certificates gnupg && \
    # additional tools
    apt-get -y install cron curl git iproute2 jq locales lsb-release procps unzip && \
    # prep directory for keyrings
    mkdir -p /etc/apt/keyrings && \
    # odbc
    apt-get -y install unixodbc unixodbc-dev

# This Dockerfile adds a non-root 'vscode' user with sudo access. However, for Linux,
# this user's GID/UID must match your local user UID/GID to avoid permission issues
# with bind mounts. Update USER_UID / USER_GID if yours is not 1000. See
# https://aka.ms/vscode-remote/containers/non-root-user for details.

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME && \
    useradd -s /bin/bash --uid $USER_UID --gid $USER_GID -m $USERNAME && \
    # [Optional] Add sudo support for the non-root user
    apt-get install -y sudo && \
    echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME && \
    chmod 0440 /etc/sudoers.d/$USERNAME

# --- Node.js LTS (via NodeSource) ---
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_${NODE_MAJOR}.x nodistro main" > \
      /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends nodejs && \
    node -v && \
    corepack enable && \
    rm -rf /var/lib/apt/lists/*

# --- Postgres Client ---
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/keyrings/postgresql.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client-17 && \
    psql --version && \
    rm -rf /var/lib/apt/lists/*

# install task cli (via npm)
RUN npm install -g @go-task/cli

# --- Python tools ---
RUN curl -LsSf https://astral.sh/uv/install.sh | sh


# set workspace
ENV WORK_DIR=/workspace
WORKDIR /workspace

# Install Python dependencies using poetry
# COPY pyproject.toml poetry.lock* ./

ENV PYTHONPATH=${WORK_DIR}
ENV PATH="${PATH}:/~/.local/bin"
# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=dialog
# set shell
ENV SHELL=/bin/bash
