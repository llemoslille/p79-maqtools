#!/usr/bin/env sh
set -e

MODE="${1:-vendas}"
shift || true

case "$MODE" in
  vendas)
    exec python main_omie_vendas.py "$@"
    ;;
  estoque)
    exec python main_omie_estoque.py "$@"
    ;;
  *)
    echo "[ERRO] Modo invalido: '$MODE'"
    echo "Use: vendas ou estoque"
    exit 1
    ;;
esac
