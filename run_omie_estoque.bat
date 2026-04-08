@echo off
setlocal EnableExtensions
title P79-MAQTOOLS [REPOSICAO]

rem Executa o ETL do omie_estoque sem depender de venv fixo.
rem Tenta "py" primeiro e usa "python" como fallback.

set "ROOT=%~dp0"
set "MAIN=%ROOT%main_omie_estoque.py"

if not exist "%MAIN%" (
  echo [ERRO] main_omie_estoque.py nao encontrado em: "%MAIN%"
  pause
  exit /b 1
)

chcp 65001 >nul
echo == Iniciando: omie_estoque ==

rem Repassa todos os argumentos: --silver-only, --gold-only, etc.
where py >nul 2>nul
if %ERRORLEVEL%==0 (
  py "%MAIN%" %*
  set "EXITCODE=%ERRORLEVEL%"
  echo == Fim: omie_estoque (exit_code=%EXITCODE%) ==
  exit /b %EXITCODE%
)

where python >nul 2>nul
if %ERRORLEVEL%==0 (
  python "%MAIN%" %*
  set "EXITCODE=%ERRORLEVEL%"
  echo == Fim: omie_estoque (exit_code=%EXITCODE%) ==
  exit /b %EXITCODE%
)

echo [ERRO] Nem 'py' nem 'python' encontrados no PATH.
exit /b 1
