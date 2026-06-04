#!/usr/bin/env bash
# setup_env.sh - Snowflake Generic Data Platform local environment bootstrap
# Run from repo root:  bash scripts/setup_env.sh
#
# Python is managed entirely by uv — no system Python or pyenv needed.
# Node.js is managed by nvm.
#
# Prerequisites handled automatically:
#   uv      (Python + venv manager — installs Python 3.11 itself)
#   nvm     (Node.js version manager)
#   Node.js 18 (via nvm)
#   Docker Desktop
#   abctl   (Airbyte CLI)

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(dirname "$SCRIPT_DIR")

PYTHON_VERSION="3.11"
NODE_VERSION="18"
VENV_DIR="$ROOT/.venv"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; GRAY='\033[0;90m'; NC='\033[0m'

step() { echo ""; echo -e "${CYAN}>>> $1${NC}"; }
ok()   { echo -e "  ${GREEN}[OK]${NC}   $1"; }
warn() { echo -e "  ${YELLOW}[WARN]${NC} $1"; }
info() { echo -e "  ${GRAY}[INFO]${NC} $1"; }
fail() { echo -e "  ${RED}[FAIL]${NC} $1"; exit 1; }

ask_yn() {
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
# uv  (Python + virtualenv manager — no system Python required)
# ==============================================================================
step "uv  (Python + venv manager)"

# Load uv from its default install location if not yet on PATH
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

if ! command -v uv &>/dev/null; then
    if ask_yn "uv not found. Install it? (recommended — manages Python 3.11 automatically)"; then
        info "Installing uv..."
        if [ "$OS" = "Darwin" ] || [ "$OS" = "Linux" ]; then
            curl -LsSf https://astral.sh/uv/install.sh | sh
            # Reload PATH so uv is available immediately
            export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
        else
            # Windows (Git Bash / WSL2)
            curl -LsSf https://astral.sh/uv/install.sh | sh
            export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
        fi

        if ! command -v uv &>/dev/null; then
            warn "uv installed but not yet in PATH."
            warn "Add to your shell profile (~/.bashrc or ~/.zshrc):"
            echo '    export PATH="$HOME/.local/bin:$PATH"'
            warn "Then restart your shell and rerun this script."
            exit 0
        fi
        ok "uv installed"
    else
        fail "uv is required. Install from: https://docs.astral.sh/uv/getting-started/installation/"
    fi
else
    ok "uv $(uv --version)"
fi

# ==============================================================================
# Virtual environment  (Python 3.11 bundled by uv — no system Python needed)
# ==============================================================================
step "Virtual environment  ($VENV_DIR)"

if [ -d "$VENV_DIR" ]; then
    ok "Already exists - skipping creation"
else
    uv venv "$VENV_DIR" --python "$PYTHON_VERSION"
    ok "Created $VENV_DIR with Python $PYTHON_VERSION"
fi

# Activate so subsequent uv pip / pip calls land in the venv
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
ok "Virtual environment activated"

# ==============================================================================
# Python dependencies
# ==============================================================================
step "Python dependencies"

REQ_FILE="$SCRIPT_DIR/requirements.txt"
if [ -f "$REQ_FILE" ]; then
    uv pip install -r "$REQ_FILE"
    ok "Installed from scripts/requirements.txt"
else
    warn "scripts/requirements.txt not found - skipping"
fi

step "Platform CLI"
uv pip install -e "$ROOT"
ok "'platform' command installed"

# ==============================================================================
# nvm  (Node.js version manager)
# ==============================================================================
step "nvm  (Node.js version manager)"

export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
[ -s "$NVM_DIR/nvm.sh" ] && source "$NVM_DIR/nvm.sh" || true

if ! command -v nvm &>/dev/null; then
    if ask_yn "nvm not found. Install it?"; then
        info "Installing nvm..."
        curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
        export NVM_DIR="$HOME/.nvm"
        # shellcheck disable=SC1091
        source "$NVM_DIR/nvm.sh"

        if ! command -v nvm &>/dev/null; then
            warn "nvm installed but not yet in PATH. Restart your shell, then run:"
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
            fail "Install Docker Desktop manually: https://www.docker.com/products/docker-desktop/"
        fi
    else
        fail "Docker is required. Install from: https://www.docker.com/products/docker-desktop/"
    fi
else
    if docker info &>/dev/null; then
        ok "$(docker --version) - daemon running"
    else
        warn "Docker installed but daemon not running. Start Docker Desktop, then rerun this script."
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
    warn "Open compose/.env and fill in your Snowflake + Airbyte credentials!"
else
    warn "compose/.env.example not found - skipping"
fi

# ==============================================================================
# abctl  (Airbyte CLI — manages Airbyte OSS via a local k3d cluster)
# ==============================================================================
step "abctl  (Airbyte CLI)"

if command -v abctl &>/dev/null; then
    ok "abctl $(abctl version 2>/dev/null | head -1 || echo 'found')"
else
    if ask_yn "abctl not found. Install it?"; then
        if [ "$OS" = "Darwin" ] && command -v brew &>/dev/null; then
            brew install airbytehq/tap/abctl
            ok "abctl installed via Homebrew"
        elif [ "$OS" = "Darwin" ] || [ "$OS" = "Linux" ]; then
            curl -LsfS https://get.airbyte.com | bash -s
            ok "abctl installed"
        else
            warn "Windows detected. Download abctl manually from:"
            warn "  https://github.com/airbytehq/abctl/releases/latest"
            warn "Add the binary to your PATH, then rerun this script."
            exit 0
        fi
    else
        warn "abctl skipped. Airbyte will not be available."
        warn "Install from: https://github.com/airbytehq/abctl/releases/latest"
    fi
fi

# Install Airbyte (idempotent — safe to run again; skips if already running)
if command -v abctl &>/dev/null; then
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "airbyte-abctl-control-plane"; then
        ok "Airbyte already installed (container running)"
    else
        if ask_yn "Airbyte not yet installed. Run 'abctl local install' now? (takes ~5 min first time)"; then
            abctl local install
            ok "Airbyte installed"
            info "Get Airbyte credentials: abctl local credentials"
            info "Copy client-id and client-secret into compose/.env"
        else
            info "Skipped. Install later: abctl local install"
            info "Then get credentials: abctl local credentials"
        fi
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
echo "  1. Get Airbyte credentials: abctl local credentials"
echo "  2. Edit compose/.env with your Snowflake + Airbyte credentials"
echo "  3. Init Snowflake:          python sgdp/scripts/init_snowflake.py"
echo "  4. Start Airflow:           docker compose -f compose/docker-compose.yml up -d"
echo "  5. Health check:            python sgdp/scripts/health_check.py"
echo ""
echo "  Daily startup: compose/start.bat  (Windows)  or  make -C compose up"
echo ""
echo "  Airflow UI -> http://localhost:8080  (admin / admin)"
echo "  Airbyte UI -> http://localhost:8000"
echo ""
echo "  See STARTUP.md for full documentation."
