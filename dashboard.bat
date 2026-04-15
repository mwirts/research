@echo off
title Analise FIP-IE - Oikos
cd /d "%~dp0"

echo ==========================================
echo   Analise FIP-IE - Dashboard
echo ==========================================
echo.

echo [1/3] Atualizando cotacoes e dividendos...
python -m etl.downloader.market_data --update 2>&1 | findstr /i "INFO ERROR summary"
echo.

echo [2/3] Atualizando TIR implicita...
python -m etl.downloader.tir_calculator --update-all 2>&1 | findstr /i "breakpoints FAILED"
echo.

echo [3/3] Iniciando servidor...
echo.
echo   Dashboard: http://127.0.0.1:8000
echo   API docs:  http://127.0.0.1:8000/docs
echo.
echo   Pressione Ctrl+C para encerrar.
echo ==========================================

start http://127.0.0.1:8000
python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
