#! /bin/bash

# Workaround for newer linux kernel 
# https://github.com/devcontainers/features/issues/1235#event-21749942947
set -ex
if ! docker info > /dev/null 2>&1; then
    sudo update-alternatives --set iptables /usr/sbin/iptables-nft
fi

# Dapr - installs the CLI/binaries only, no auto-started containers. The
# placement service, sidecar, and Redis-backed state store are managed by
# docker-compose.yml instead, so they're the single source of truth (and
# work outside the devcontainer too).
dapr init --slim --runtime-version 1.18.2

# Diagrid dev dashboard
curl -sSL https://raw.githubusercontent.com/diagridio/dev-dashboard/main/scripts/install.sh | sh
