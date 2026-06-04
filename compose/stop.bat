@echo off
cd /d "%~dp0"
docker compose down
docker stop airbyte-abctl-control-plane