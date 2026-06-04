@echo off
cd /d "%~dp0"
docker start airbyte-abctl-control-plane
docker compose up -d