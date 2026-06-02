# start.ps1 — Start the full platform: Airflow (Docker Compose) + Airbyte (abctl) in parallel
# Run from repo root:  .\start.ps1
# Add -Build to rebuild Airflow image:  .\start.ps1 -Build

param([switch]$Build)

$ErrorActionPreference = "Stop"

$root = $PSScriptRoot

# Ensure abctl is in PATH for this session
$abctlBin = "$env:USERPROFILE\.abctl\bin"
if ((Test-Path $abctlBin) -and ($env:PATH -notlike "*$abctlBin*")) {
    $env:PATH = "$abctlBin;$env:PATH"
}

# Prereq checks
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "[FAIL] docker not found. Run .\scripts\setup_env.ps1 first." -ForegroundColor Red; exit 1
}
try { docker info 2>&1 | Out-Null } catch {}
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] Docker daemon not running. Start Docker Desktop and retry." -ForegroundColor Red; exit 1
}
if (-not (Get-Command abctl -ErrorAction SilentlyContinue)) {
    Write-Host "[FAIL] abctl not found. Run .\scripts\setup_env.ps1 first." -ForegroundColor Red; exit 1
}

$composeArgs = if ($Build) { "compose -f compose/docker-compose.yml up -d --build" } else { "compose -f compose/docker-compose.yml up -d" }

Write-Host ""
Write-Host "  Starting platform (parallel)..." -ForegroundColor Cyan
Write-Host "  [docker]  docker $composeArgs" -ForegroundColor DarkGray
Write-Host "  [airbyte] abctl local install" -ForegroundColor DarkGray
Write-Host ""

$pathSnapshot = $env:PATH

$dockerJob = Start-Job -Name "docker" -ScriptBlock {
    $env:PATH = $using:pathSnapshot
    Set-Location $using:root
    Invoke-Expression "docker $using:composeArgs" 2>&1
}

$airbytejob = Start-Job -Name "airbyte" -ScriptBlock {
    $env:PATH = $using:pathSnapshot
    Set-Location $using:root
    abctl local install 2>&1
}

# Stream both jobs' output until both finish
while ($dockerJob.State -eq "Running" -or $airbytejob.State -eq "Running") {
    Receive-Job $dockerJob  | ForEach-Object { Write-Host "  [docker]  $_" -ForegroundColor Blue }
    Receive-Job $airbytejob | ForEach-Object { Write-Host "  [airbyte] $_" -ForegroundColor Magenta }
    Start-Sleep -Milliseconds 500
}

# Drain any remaining output
Receive-Job $dockerJob  | ForEach-Object { Write-Host "  [docker]  $_" -ForegroundColor Blue }
Receive-Job $airbytejob | ForEach-Object { Write-Host "  [airbyte] $_" -ForegroundColor Magenta }

$dockerFailed  = $dockerJob.State  -eq "Failed"
$airbyteFailed = $airbytejob.State -eq "Failed"

Remove-Job $dockerJob, $airbytejob

Write-Host ""
if ($dockerFailed)  { Write-Host "  [docker]  FAILED — check output above." -ForegroundColor Red }
else                { Write-Host "  [docker]  done." -ForegroundColor Green }

if ($airbyteFailed) { Write-Host "  [airbyte] FAILED — check output above." -ForegroundColor Red }
else                { Write-Host "  [airbyte] done." -ForegroundColor Green }

if (-not $dockerFailed -and -not $airbyteFailed) {
    Write-Host ""
    Write-Host "  Platform is up!" -ForegroundColor Cyan
    Write-Host "  Airflow -> http://localhost:8080  (admin / admin)"
    Write-Host "  Airbyte -> http://localhost:8000  (run 'abctl local credentials' for password)"
    Write-Host ""
}
