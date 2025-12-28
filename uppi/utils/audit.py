from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Optional


def mask_username(username: str, keep_start: int = 2, keep_end: int = 2) -> str:
    """
    Маскує логін (AE_USERNAME) для збереження в БД.
    Приклад: mario.rossi -> ma***si
    """
    u = (username or "").strip()
    if not u:
        return ""
    if keep_start < 0 or keep_end < 0:
        raise ValueError("keep_start/keep_end must be >= 0")
    if len(u) <= keep_start + keep_end:
        return "*" * len(u)
    return f"{u[:keep_start]}***{u[-keep_end:]}"


def sha256_hex(value: str) -> str:
    """SHA256 від строки (hex)."""
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def sha256_file(path: str | Path) -> str:
    """SHA256 від файлу (hex)."""
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def get_git_commit(repo_dir: str | Path = ".") -> Optional[str]:
    """Повертає git commit SHA або None, якщо git/.git недоступні."""
    try:
        out = subprocess.check_output(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or None
    except Exception:
        return None


def sha256_json(data: Any) -> str:
    """
    Стабільний sha256 для Python-структури (для дедуп/контролю).
    У БД ти маєш params_hash, але інколи корисно мати такий хеш і в коді.
    """
    dumped = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def build_params_snapshot(
    *,
    client_yaml: dict[str, Any] | None,
    parsed: dict[str, Any] | None,
    computed: dict[str, Any] | None,
    template_context: dict[str, Any] | None,
    template_path: str | Path | None = None,
    template_version: str | None = None,
    schema_version: str | None = None,
    repo_dir: str | Path = ".",
    extra_versions: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Канонічний snapshot параметрів генерації.
    Цей об’єкт кладеш у attestazioni.params_snapshot (JSONB).
    """
    versions: dict[str, Any] = {
        "git_commit": get_git_commit(repo_dir),
        "template_version": template_version,
        "schema_version": schema_version,
    }

    if template_path is not None:
        tp = Path(template_path)
        versions.update(
            {
                "template_path": str(tp),
                "template_checksum_sha256": sha256_file(tp) if tp.exists() else None,
            }
        )

    if extra_versions:
        versions.update(extra_versions)

    snapshot: dict[str, Any] = {
        "client_yaml": client_yaml or {},
        "parsed": parsed or {},
        "computed": computed or {},
        "template_context": template_context or {},
        "versions": versions,
    }
    return snapshot
