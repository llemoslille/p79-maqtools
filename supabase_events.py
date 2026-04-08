import os
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

import requests


@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    service_role_key: str
    table: str = "lille_atualizacoes_dw"

    @property
    def insert_url(self) -> str:
        return f"{self.url.rstrip('/')}/rest/v1/{self.table}"


def get_supabase_config_from_env() -> Optional[SupabaseConfig]:
    raw_url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    table = (os.environ.get("SUPABASE_LOG_TABLE") or "lille_atualizacoes_dw").strip()
    if not raw_url or not key:
        return None

    url = raw_url.rstrip("/")
    if url.endswith("/rest/v1"):
        url = url[: -len("/rest/v1")]
    return SupabaseConfig(url=url, service_role_key=key, table=table)


class SupabaseEventLogger:
    """Logger assíncrono de eventos para Supabase (não bloqueia o ETL)."""

    def __init__(
        self,
        cfg: SupabaseConfig,
        *,
        run_id: uuid.UUID,
        app: str = "p79-maqtools",
        env: str = "prod",
        project_key: str = "p79-maqtools",
    ):
        self.cfg = cfg
        self.run_id = run_id
        self.app = app
        self.env = env
        self.project_key = project_key
        self._buf: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stop = False
        self._thread = threading.Thread(
            target=self._worker,
            name="supabase-event-logger",
            daemon=True,
        )
        self._thread.start()

    def emit(
        self,
        event_type: str,
        *,
        level: str = "INFO",
        table_name: str | None = None,
        id_empresa: str | None = None,
        duration_seconds: float | None = None,
        rows_count: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        row: dict[str, Any] = {
            "run_id": str(self.run_id),
            "event_type": event_type,
            "level": level,
            "app": self.app,
            "env": self.env,
            "project_key": self.project_key,
            "table_name": table_name,
            "id_empresa": id_empresa,
            "duration_seconds": duration_seconds,
            "rows_count": rows_count,
            "error_type": error_type,
            "error_message": error_message,
            "extra": extra or None,
        }
        with self._lock:
            self._buf.append(row)

    def flush(self, *, timeout_sec: float = 10.0) -> None:
        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            with self._lock:
                pending = len(self._buf)
            if pending == 0:
                return
            time.sleep(0.05)

    def close(self) -> None:
        self._stop = True
        self.flush(timeout_sec=5.0)

    def _worker(self) -> None:
        session = requests.Session()
        headers = {
            "apikey": self.cfg.service_role_key,
            "Authorization": f"Bearer {self.cfg.service_role_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

        while not self._stop:
            batch: list[dict[str, Any]] = []
            with self._lock:
                if self._buf:
                    batch = self._buf[:50]
                    del self._buf[:50]

            if not batch:
                time.sleep(0.05)
                continue

            try:
                response = session.post(
                    self.cfg.insert_url,
                    headers=headers,
                    json=batch,
                    timeout=8,
                )
                if response.status_code >= 300:
                    print(
                        f"[AVISO] Supabase log falhou ({response.status_code}): "
                        f"{response.text[:300]}"
                    )
            except Exception as exc:  # noqa: BLE001
                print(f"[AVISO] Supabase log exception: {exc}")
