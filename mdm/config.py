import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


def load_config(path: str | None = None) -> dict:
    path = path or os.environ.get("MDM_CONFIG") or ROOT / "config.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


def _load_dotenv(path: Path = ROOT / ".env") -> None:
    """Minimal .env reader (KEY=value lines). Real env vars take precedence."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()


def get_secret(name: str, scope: str = "market-monitor") -> str | None:
    """Env var (or .env file) first, then Databricks secret scope (key = lowercased name)."""
    if os.environ.get(name):
        return os.environ[name]
    try:
        from databricks.sdk.runtime import dbutils  # type: ignore

        return dbutils.secrets.get(scope, name.lower())
    except Exception:
        return None
