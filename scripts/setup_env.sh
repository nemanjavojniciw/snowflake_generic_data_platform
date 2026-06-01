#!/usr/bin/env bash
# setup_env.sh - Snowflake Generic Data Platform local environment bootstrap
# Run from repo root:  bash scripts/setup_env.sh
# Self-contained: checks every prerequisite, asks to install if missing.
#
# Prerequisites handled automatically:
#   pyenv   (Python version manager)
#   Python 3.11.9 (via pyenv)
#   nvm     (Node.js version manager)
#   Node.js 18 (via nvm)
#   Docker Desktop

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(dirname "$SCRIPT_DIR")

PYTHON_VERSION="3.11.9"
NODE_VERSION="18"
VENV_DIR="$ROOT/.venv"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; GRAY='\033[0;90m'; NC='\033[0m'

step() { echo ""; echo -e "${CYAN}>>> $1${NC}"; }
ok()   { echo -e "  ${GREEN}[OK]${NC}   $1"; }
warn() { echo -e "  ${YELLOW}[WARN]${NC} $1"; }
info() { echo -e "  ${GRAY}[INFO]${NC} $1"; }
fail() { echo -e "  ${RED}[FAIL]${NC} $1"; exit 1; }

ask_yn() {
    # Usage: ask_yn "Prompt text" [default_no]
    local prompt="$1"
    local default_no="${2:-}"
    local hint="[Y/n]"
    [ -n "$default_no" ] && hint="[y/N]"
    local ans
    read -r -p "  $prompt $hint " ans </dev/tty
    if [ -n "$default_no" ]; then
        [[ "$ans" =~ ^[Yy] ]]
    else
        [ -z "$ans" ] || [[ "$ans" =~ ^[Yy] ]]
    fi
}

OS=$(uname -s)

echo "Repo root: $ROOT"

# ==============================================================================
# pyenv
# ==============================================================================
step "pyenv  (Python version manager)"

export PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
export PATH="$PYENV_ROOT/bin:$PATH"

if ! command -v pyenv &>/dev/null; then
    if ask_yn "pyenv not found. Install it?"; then
        info "Installing pyenv..."
        if [ "$OS" = "Darwin" ] && command -v brew &>/dev/null; then
            brew install pyenv
        else
            curl -fsSL https://pyenv.run | bash
        fi

        # Load pyenv for the rest of this script
        export PYENV_ROOT="$HOME/.pyenv"
        export PATH="$PYENV_ROOT/bin:$PATH"

        if ! command -v pyenv &>/dev/null; then
            warn "pyenv installed but not yet in PATH."
            warn "Add the following to your shell profile (~/.bashrc or ~/.zshrc):"
            echo '    export PYENV_ROOT="$HOME/.pyenv"'
            echo '    export PATH="$PYENV_ROOT/bin:$PATH"'
            echo '    eval "$(pyenv init -)"'
            warn "Then restart your shell and rerun this script."
            exit 0
        fi
        ok "pyenv installed"
    else
        fail "pyenv is required. Install from: https://github.com/pyenv/pyenv"
    fi
else
    ok "pyenv $(pyenv --version)"
fi

eval "$(pyenv init -)"

# Install target Python version if missing
info "Checking Python $PYTHON_VERSION..."
if ! pyenv versions --bare | grep -qx "$PYTHON_VERSION"; then
    info "Installing Python $PYTHON_VERSION (this takes a few minutes)..."
    pyenv install "$PYTHON_VERSION"
fi

pyenv local "$PYTHON_VERSION"
ok "Python $PYTHON_VERSION set as local version"

PYTHON_CMD="$(pyenv which python)"
ok "python -> $PYTHON_CMD"

# ==============================================================================
# Virtual environment
# ==============================================================================
step "Virtual environment  ($VENV_DIR)"

if [ -d "$VENV_DIR" ]; then
    ok "Already exists - skipping creation"
else
    "$PYTHON_CMD" -m venv "$VENV_DIR"
    ok "Created $VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
ok "Virtual environment activated"

# ==============================================================================
# Python dependencies
# ==============================================================================
step "Python dependencies"
pip install --upgrade pip -q
REQ_FILE="$SCRIPT_DIR/requirements.txt"
if [ -f "$REQ_FILE" ]; then
    pip install -r "$REQ_FILE"
    ok "Installed from scripts/requirements.txt"
else
    warn "scripts/requirements.txt not found - skipping"
fi

# ==============================================================================
# nvm
# ==============================================================================
step "nvm  (Node.js version manager)"

export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
# Try loading nvm if already installed
[ -s "$NVM_DIR/nvm.sh" ] && source "$NVM_DIR/nvm.sh" || true

if ! command -v nvm &>/dev/null; then
    if ask_yn "nvm not found. Install it?"; then
        info "Installing nvm..."
        curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
        export NVM_DIR="$HOME/.nvm"
        # shellcheck disable=SC1091
        source "$NVM_DIR/nvm.sh"

        if ! command -v nvm &>/dev/null; then
            warn "nvm installed but not yet in PATH. Restart shell, then run:"
            echo "    nvm install $NODE_VERSION && nvm use $NODE_VERSION"
        else
            ok "nvm installed"
        fi
    else
        warn "nvm skipped. Node.js will not be configured."
    fi
else
    ok "nvm $(nvm --version)"
fi

# Install and activate Node.js if nvm is available
if command -v nvm &>/dev/null; then
    info "Ensuring Node.js $NODE_VERSION is installed..."
    nvm install "$NODE_VERSION"
    nvm use "$NODE_VERSION"
    ok "Node.js $(node --version)  (npm $(npm --version))"
fi

# ==============================================================================
# Docker
# ==============================================================================
step "Docker"

if ! command -v docker &>/dev/null; then
    if ask_yn "Docker not found. Install it?"; then
        if [ "$OS" = "Darwin" ]; then
            if command -v brew &>/dev/null; then
                info "Installing Docker Desktop via Homebrew..."
                brew install --cask docker
                warn "Launch Docker Desktop from Applications, then rerun this script."
                exit 0
            else
                fail "Install Docker Desktop manually: https://www.docker.com/products/docker-desktop/"
            fi
        elif [ "$OS" = "Linux" ]; then
            info "Installing Docker Engine via convenience script..."
            curl -fsSL https://get.docker.com | sh
            sudo usermod -aG docker "$USER"
            warn "Log out and back in for docker group to take effect, then rerun this script."
            exit 0
        else
            fail "Unsupported OS for auto-install. Install Docker manually: https://www.docker.com/products/docker-desktop/"
        fi
    else
        fail "Docker is required. Install from: https://www.docker.com/products/docker-desktop/"
    fi
else
    if docker info &>/dev/null; then
        ok "$(docker --version) - daemon running"
    else
        warn "Docker installed but daemon not running. Start Docker Desktop, then run step 2 below."
    fi
fi

# ==============================================================================
# .env file
# ==============================================================================
step ".env configuration"
ENV_FILE="$ROOT/compose/.env"
ENV_EXAMPLE="$ROOT/compose/.env.example"
if [ -f "$ENV_FILE" ]; then
    ok "compose/.env already exists"
elif [ -f "$ENV_EXAMPLE" ]; then
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    ok "Copied compose/.env.example -> compose/.env"
    warn "Open compose/.env and fill in your Snowflake credentials!"
else
    warn "compose/.env.example not found - skipping"
fi

# ==============================================================================
# Airbyte (clone optional)
# ==============================================================================
step "Airbyte OSS"
AIRBYTE_DIR="$ROOT/local/airbyte"
if [ -d "$AIRBYTE_DIR" ]; then
    ok "local/airbyte already cloned"
else
    if ask_yn "Airbyte not cloned. Clone it now? (~1 GB download)"; then
        if ! command -v git &>/dev/null; then
            warn "git not found - cannot clone. Install git and run manually."
        else
            mkdir -p "$ROOT/local"
            git clone https://github.com/airbytehq/airbyte.git "$AIRBYTE_DIR"
            ok "Cloned to local/airbyte"
            info "Start Airbyte: cd local/airbyte && bash run-ab-platform.sh"
        fi
    else
        info "Skipped. Clone later: git clone https://github.com/airbytehq/airbyte.git local/airbyte"
    fi
fi

# ==============================================================================
# Astro CLI (optional)
# ==============================================================================
step "Astro CLI  (optional - alternative Airflow runner)"
if command -v astro &>/dev/null; then
    ok "Astro CLI found"
else
    if ask_yn "Astro CLI not found. Install it? (optional)" "default_no"; then
        if [ "$OS" = "Darwin" ] && command -v brew &>/dev/null; then
            brew install astro
        else
            curl -sSL install.astronomer.io | sudo bash
        fi
        ok "Astro CLI installed"
    else
        info "Skipped. Use Docker Compose: docker compose -f compose/docker-compose.yml up -d"
    fi
fi

# ==============================================================================
# Summary
# ==============================================================================
echo ""
echo -e "${CYAN}==================================================${NC}"
echo -e "${CYAN}  Environment bootstrap complete!${NC}"
echo -e "${CYAN}==================================================${NC}"
echo ""
echo "Next steps (from repo root: $ROOT):"
echo "  1. Edit compose/.env with your Snowflake credentials"
echo "  2. Start Airflow:  docker compose -f compose/docker-compose.yml up -d"
echo "  3. Start Airbyte:  cd local/airbyte && bash run-ab-platform.sh"
echo "  4. Init Snowflake: python platform/scripts/init_snowflake.py"
echo "  5. Health check:   python platform/scripts/health_check.py"
echo ""
echo "  Airflow UI -> http://localhost:8080  (admin / admin)"
echo "  Airbyte UI -> http://localhost:8000"
