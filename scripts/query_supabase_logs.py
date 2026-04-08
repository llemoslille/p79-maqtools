#!/usr/bin/env python3
"""
Consulta eventos de log no Supabase (REST API).

Uso:
  python scripts/query_supabase_logs.py --limit 50
  python scripts/query_supabase_logs.py --project-key p79-maqtools --limit 100
  python scripts/query_supabase_logs.py --csv -o logs.csv --limit 500
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent

CSV_COLUMNS = [
    "id",
    "created_at",
    "run_id",
    "event_type",
    "level",
    "app",
    "env",
    "project_key",
    "table_name",
    "id_empresa",
    "duration_seconds",
    "rows_count",
    "error_type",
    "error_message",
    "extra",
]


def _normalize_supabase_base_url(raw: str) -> str:
    url = raw.rstrip("/")
    if url.endswith("/rest/v1"):
        url = url[: -len("/rest/v1")]
    return url


def _rows_to_csv_rows(data: list[dict]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in data:
        flat: dict[str, str] = {}
        for col in CSV_COLUMNS:
            if col == "extra":
                ex = row.get("extra")
                if ex is None:
                    flat[col] = ""
                elif isinstance(ex, (dict, list)):
                    flat[col] = json.dumps(ex, ensure_ascii=False)
                else:
                    flat[col] = str(ex)
            else:
                val = row.get(col)
                flat[col] = "" if val is None else str(val)
        out.append(flat)
    return out


def _write_csv(data: list[dict], path: Path | None, *, utf8_bom: bool) -> None:
    rows = _rows_to_csv_rows(data)
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    text = buf.getvalue()
    if path is None:
        sys.stdout.write(text)
        return
    enc = "utf-8-sig" if utf8_bom else "utf-8"
    path.write_text(text, encoding=enc, newline="")


def main() -> int:
    parser = argparse.ArgumentParser(description="Lista últimos eventos de log no Supabase.")
    parser.add_argument("--limit", type=int, default=20, help="Quantidade máxima de linhas (default: 20).")
    parser.add_argument("--table", type=str, default=None, help="Tabela (default: SUPABASE_LOG_TABLE ou lille_atualizacoes_dw).")
    parser.add_argument("--app", type=str, default=None, help="Filtra por app (ex.: p79-maqtools).")
    parser.add_argument(
        "--project-key",
        type=str,
        default=None,
        help="Filtra por project_key (default: lê PROJECT_KEY do .env; se vazio, não filtra).",
    )
    parser.add_argument("--event-type", type=str, default=None, help="Filtra por event_type (ex.: run_start).")
    parser.add_argument("--level", type=str, default=None, help="Filtra por level (ex.: ERROR).")
    parser.add_argument("--order", type=str, default="created_at.desc.nullslast", help='PostgREST order (ex.: "id.desc").')
    parser.add_argument("--no-order", action="store_true", help="Não envia parâmetro order.")
    parser.add_argument("--csv", action="store_true", help="Saída CSV.")
    parser.add_argument("--output", "-o", type=str, default=None, help="Arquivo de saída.")
    parser.add_argument("--no-excel-bom", action="store_true", help="CSV sem BOM UTF-8.")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")

    raw_url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    table = (args.table or os.environ.get("SUPABASE_LOG_TABLE") or "lille_atualizacoes_dw").strip()

    if not raw_url or not key:
        print("Erro: defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no .env", file=sys.stderr)
        return 1

    base = _normalize_supabase_base_url(raw_url)
    url = f"{base}/rest/v1/{table}"

    default_project_key = (os.environ.get("PROJECT_KEY") or "").strip() or None
    project_key = (args.project_key or default_project_key or "").strip() or None

    params: dict[str, str] = {
        "select": "*",
        "limit": str(max(1, min(args.limit, 1000))),
    }
    if not args.no_order and (args.order or "").strip():
        params["order"] = args.order.strip()

    if args.app:
        params["app"] = f"eq.{args.app.strip()}"
    if project_key:
        params["project_key"] = f"eq.{project_key}"
    if args.event_type:
        params["event_type"] = f"eq.{args.event_type.strip()}"
    if args.level:
        params["level"] = f"eq.{args.level.strip().upper()}"

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
    except requests.RequestException as e:
        print(f"Erro de rede: {e}", file=sys.stderr)
        return 1

    if r.status_code != 200:
        print(f"HTTP {r.status_code}", file=sys.stderr)
        print(r.text[:4000], file=sys.stderr)
        return 1

    try:
        data = r.json()
    except json.JSONDecodeError:
        print("Resposta não é JSON:", r.text[:2000], file=sys.stderr)
        return 1

    if not isinstance(data, list):
        print(json.dumps(data, indent=2, ensure_ascii=False))
        return 0

    if args.csv:
        out_path = Path(args.output) if args.output else None
        use_bom = out_path is not None and not args.no_excel_bom
        _write_csv(data, out_path, utf8_bom=use_bom)
        if out_path:
            print(f"CSV gravado: {out_path.resolve()} ({len(data)} linhas)", file=sys.stderr)
        return 0

    print(f"Registros: {len(data)}\n")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
