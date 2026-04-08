@echo off
setlocal EnableExtensions
title P79-MAQTOOLS [VENDAS]

rem Executa o ETL do omie_vendas sem depender de venv fixo.
rem Tenta "py" primeiro e usa "python" como fallback.
rem Pensado para rodar no Agendador de Tarefas do Windows (sem pause).

set "ROOT=%~dp0"
set "MAIN=%ROOT%main_omie_vendas.py"

if not exist "%MAIN%" (
  echo [ERRO] main_omie_vendas.py nao encontrado em: "%MAIN%"
  exit /b 1
)

chcp 65001 >nul
echo == Iniciando: omie_vendas ==

rem Repassa todos os argumentos: --camada silver, --paralelo, --workers 8, etc.
where py >nul 2>nul
if %ERRORLEVEL%==0 (
  py "%MAIN%" %*
  set "EXITCODE=%ERRORLEVEL%"
  echo == Fim: omie_vendas (exit_code=%EXITCODE%) ==
  exit /b %EXITCODE%
)

where python >nul 2>nul
if %ERRORLEVEL%==0 (
  python "%MAIN%" %*
  set "EXITCODE=%ERRORLEVEL%"
  echo == Fim: omie_vendas (exit_code=%EXITCODE%) ==
  exit /b %EXITCODE%
)

echo [ERRO] Nem 'py' nem 'python' encontrados no PATH.
exit /b 1
