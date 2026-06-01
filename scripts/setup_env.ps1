# setup_env.ps1 - Snowflake Generic Data Platform local environment bootstrap
# Run from repo root:  .\scripts\setup_env.ps1
# Self-contained: checks every prerequisite, asks to install if missing.
#
# Prerequisites handled automatically:
#   uv          (Python version manager + venv + package installer)
#   Python 3.11.9 (via uv)
#   nvm-windows (Node.js version manager)
#   Node.js 18  (via nvm-windows)
#   Docker Desktop

$ErrorActionPreference = "Stop"

$ROOT           = Split-Path $PSScriptRoot -Parent
$PYTHON_VERSION = "3.11.9"
$NODE_VERSION   = "18"
$VENV_DIR       = Join-Path $ROOT ".venv"

function Write-Step { param($msg) Write-Host ""; Write-Host ">>> $msg" -ForegroundColor Cyan }
function Write-OK   { param($msg) Write-Host "  [OK]   $msg" -ForegroundColor Green }
function Write-Warn { param($msg) Write-Host "  [WARN] $msg" -ForegroundColor Yellow }
function Write-Info { param($msg) Write-Host "  [INFO] $msg" -ForegroundColor DarkGray }
function Write-Fail { param($msg) Write-Host "  [FAIL] $msg" -ForegroundColor Red; exit 1 }

function Ask-YN {
    param($Prompt, [switch]$DefaultNo)
    $hint = if ($DefaultNo) { "[y/N]" } else { "[Y/n]" }
    $ans = Read-Host "  $Prompt $hint"
    if ($DefaultNo) { return $ans -match '^[Yy]' }
    return ($ans -eq '' -or $ans -match '^[Yy]')
}

function Require-Winget {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        Write-Fail "winget not found. Install 'App Installer' from the Microsoft Store, then rerun."
    }
}

function Reload-Path {
    $machinePath = [System.Environment]::GetEnvironmentVariable("PATH", "Machine")
    $userPath    = [System.Environment]::GetEnvironmentVariable("PATH", "User")
    $env:PATH    = "$userPath;$machinePath"
}

Write-Host "Repo root: $ROOT" -ForegroundColor DarkGray

# ==============================================================================
# uv  (Python version manager + venv + package installer)
# ==============================================================================
Write-Step "uv  (Python manager)"

$uvBin = "$env:USERPROFILE\.local\bin"
if ((Test-Path $uvBin) -and ($env:PATH -notlike "*$uvBin*")) { $env:PATH = "$uvBin;$env:PATH" }

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    if (Ask-YN "uv not found. Install it?") {
        Require-Winget
        Write-Info "Installing uv via winget..."
        winget install --id astral-sh.uv --accept-source-agreements --accept-package-agreements
        Reload-Path
        if ((Test-Path $uvBin) -and ($env:PATH -notlike "*$uvBin*")) { $env:PATH = "$uvBin;$env:PATH" }
        if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
            Write-Warn "uv installed but not yet in PATH. Restart your terminal and rerun."
            exit 0
        }
        Write-OK "uv installed"
    } else {
        Write-Fail "uv is required. Install from: https://docs.astral.sh/uv/"
    }
} else {
    Write-OK "uv $(uv --version)"
}

# Ensure target Python version is available via uv
Write-Info "Ensuring Python $PYTHON_VERSION is available..."
uv python install $PYTHON_VERSION
Write-OK "Python $PYTHON_VERSION ready"

# ==============================================================================
# Virtual environment
# ==============================================================================
Write-Step "Virtual environment  ($VENV_DIR)"

if (Test-Path $VENV_DIR) {
    Write-OK "Already exists - skipping creation"
} else {
    uv venv --python $PYTHON_VERSION $VENV_DIR
    Write-OK "Created $VENV_DIR"
}

$activateScript = Join-Path $VENV_DIR "Scripts\Activate.ps1"
if (-not (Test-Path $activateScript)) { Write-Fail "Activation script missing: $activateScript" }
& $activateScript
Write-OK "Virtual environment activated"

# ==============================================================================
# Python dependencies
# ==============================================================================
Write-Step "Python dependencies"
$reqFile = Join-Path $PSScriptRoot "requirements.txt"
if (Test-Path $reqFile) {
    uv pip install -r $reqFile
    Write-OK "Installed from scripts\requirements.txt"
} else {
    Write-Warn "scripts\requirements.txt not found - skipping"
}

# ==============================================================================
# nvm-windows
# ==============================================================================
Write-Step "nvm-windows  (Node.js version manager)"

# Inject nvm path into current session
$nvmHome = if ($env:NVM_HOME) { $env:NVM_HOME } else { "$env:APPDATA\nvm" }
if ((Test-Path $nvmHome) -and ($env:PATH -notlike "*$nvmHome*")) { $env:PATH = "$nvmHome;$env:PATH" }

if (-not (Get-Command nvm -ErrorAction SilentlyContinue)) {
    if (Ask-YN "nvm-windows not found. Install it?") {
        Require-Winget
        Write-Info "Installing nvm-windows via winget..."
        winget install --id CoreyButler.NVMforWindows --accept-source-agreements --accept-package-agreements
        Reload-Path
        $nvmHome = if ($env:NVM_HOME) { $env:NVM_HOME } else { "$env:APPDATA\nvm" }
        if ((Test-Path $nvmHome) -and ($env:PATH -notlike "*$nvmHome*")) { $env:PATH = "$nvmHome;$env:PATH" }

        if (-not (Get-Command nvm -ErrorAction SilentlyContinue)) {
            Write-Warn "nvm-windows installed but not yet available in this session."
            Write-Warn "Restart your terminal, then run:  nvm install $NODE_VERSION  &&  nvm use $NODE_VERSION"
        } else {
            Write-OK "nvm-windows installed"
        }
    } else {
        Write-Warn "nvm-windows skipped. Node.js will not be configured."
    }
} else {
    Write-OK "nvm-windows $(nvm version)"
}

# Install and activate Node.js if nvm is available
if (Get-Command nvm -ErrorAction SilentlyContinue) {
    Write-Info "Ensuring Node.js $NODE_VERSION is installed..."
    nvm install $NODE_VERSION | Out-Null
    nvm use $NODE_VERSION | Out-Null
    Reload-Path
    # nvm symlink dir may not be in PATH yet in this session
    $nvmSymlink = [System.Environment]::GetEnvironmentVariable("NVM_SYMLINK", "User")
    if ($nvmSymlink -and (Test-Path $nvmSymlink) -and ($env:PATH -notlike "*$nvmSymlink*")) {
        $env:PATH = "$nvmSymlink;$env:PATH"
    }
    if (Get-Command node -ErrorAction SilentlyContinue) {
        Write-OK "Node.js $(node --version)  (npm $(npm --version))"
    } else {
        Write-Warn "Node.js installed but not yet in PATH. Restart your terminal to use node/npm."
    }
}

# ==============================================================================
# Docker Desktop
# ==============================================================================
Write-Step "Docker Desktop"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    if (Ask-YN "Docker Desktop not found. Install it?") {
        Require-Winget
        Write-Info "Installing Docker Desktop via winget..."
        winget install --id Docker.DockerDesktop --accept-source-agreements --accept-package-agreements
        Write-Warn "Docker Desktop installed. A system RESTART is required before it works."
        Write-Warn "After restarting, rerun this script to complete setup."
        exit 0
    } else {
        Write-Fail "Docker Desktop is required. Install from: https://www.docker.com/products/docker-desktop/"
    }
} else {
    $dockerRunning = $false
    try { docker info 2>&1 | Out-Null; $dockerRunning = ($LASTEXITCODE -eq 0) } catch {}
    if ($dockerRunning) {
        Write-OK "$(docker --version) - daemon running"
    } else {
        Write-Warn "Docker installed but daemon not running. Start Docker Desktop, then run step 2 below."
    }
}

# ==============================================================================
# .env file
# ==============================================================================
Write-Step ".env configuration"
$envFile    = Join-Path $ROOT "compose\.env"
$envExample = Join-Path $ROOT "compose\.env.example"
if (Test-Path $envFile) {
    Write-OK "compose\.env already exists"
} elseif (Test-Path $envExample) {
    Copy-Item $envExample $envFile
    Write-OK "Copied compose\.env.example -> compose\.env"
    Write-Warn "Open compose\.env and fill in your Snowflake credentials!"
} else {
    Write-Warn "compose\.env.example not found - skipping"
}

# ==============================================================================
# abctl  (Airbyte local CLI — downloads binary from GitHub releases)
# ==============================================================================
Write-Step "abctl  (Airbyte local CLI)"

$abctlBin = "$env:USERPROFILE\.abctl\bin"
if ((Test-Path $abctlBin) -and ($env:PATH -notlike "*$abctlBin*")) { $env:PATH = "$abctlBin;$env:PATH" }

if (-not (Get-Command abctl -ErrorAction SilentlyContinue)) {
    if (Ask-YN "abctl not found. Download and install it?") {
        # Detect processor architecture
        $cpuArch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture
        $arch = if ($cpuArch -eq [System.Runtime.InteropServices.Architecture]::Arm64) { "arm64" } else { "amd64" }
        Write-Info "Architecture: $arch"

        # Fetch latest release tag from GitHub API
        Write-Info "Fetching latest abctl release from GitHub..."
        $release  = Invoke-RestMethod "https://api.github.com/repos/airbytehq/abctl/releases/latest" -UseBasicParsing
        $version  = $release.tag_name   # e.g. v0.30.4
        $url      = "https://github.com/airbytehq/abctl/releases/download/$version/abctl-$version-windows-$arch.zip"
        Write-Info "Downloading abctl $version ($arch)..."

        $zipPath = "$env:TEMP\abctl-$version.zip"
        Invoke-WebRequest -Uri $url -OutFile $zipPath -UseBasicParsing

        New-Item -ItemType Directory -Force $abctlBin | Out-Null
        $extractTemp = "$env:TEMP\abctl-extract"
        if (Test-Path $extractTemp) { Remove-Item -Recurse -Force $extractTemp }
        Expand-Archive -Path $zipPath -DestinationPath $extractTemp -Force
        Remove-Item $zipPath

        # The zip expands into a versioned subfolder — move abctl.exe up to bin root
        $exeSource = Get-ChildItem -Path $extractTemp -Filter "abctl.exe" -Recurse | Select-Object -First 1
        if ($exeSource) {
            Move-Item -Path $exeSource.FullName -Destination "$abctlBin\abctl.exe" -Force
        } else {
            Write-Fail "abctl.exe not found in downloaded archive."
        }
        Remove-Item -Recurse -Force $extractTemp

        # Persist to user PATH so it survives terminal restarts
        $currentUserPath = [System.Environment]::GetEnvironmentVariable("PATH", "User")
        if ($currentUserPath -notlike "*$abctlBin*") {
            [System.Environment]::SetEnvironmentVariable("PATH", "$abctlBin;$currentUserPath", "User")
        }
        $env:PATH = "$abctlBin;$env:PATH"

        if (Get-Command abctl -ErrorAction SilentlyContinue) {
            Write-OK "abctl $version installed"
        } else {
            Write-Warn "abctl installed to $abctlBin but not yet in PATH of this session."
            Write-Warn "Restart your terminal, then run: abctl local install"
        }
    } else {
        Write-Warn "abctl skipped. Download manually: https://github.com/airbytehq/abctl/releases/latest"
    }
} else {
    Write-OK "abctl $(abctl version 2>&1 | Select-Object -First 1)"
}

# ==============================================================================
# Summary
# ==============================================================================
Write-Host ""
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  Environment bootstrap complete!" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps (from repo root):" -ForegroundColor White
Write-Host "  1. Edit compose\.env with your Snowflake credentials"
Write-Host "  2. Start Airflow:  docker compose -f compose/docker-compose.yml up -d --build"
Write-Host "  3. Start Airbyte:  abctl local install          (first run ~20 min)"
Write-Host "     Get creds:      abctl local credentials"
Write-Host "  4. Init Snowflake: python platform/scripts/init_snowflake.py"
Write-Host "  5. Health check:   python platform/scripts/health_check.py"
Write-Host ""
Write-Host "  Airflow UI -> http://localhost:8080  (admin / admin)"
Write-Host "  Airbyte UI -> http://localhost:8000  (run 'abctl local credentials' for password)"
