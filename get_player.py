from pathlib import Path
from typing import Dict, Any

import polars as pl
import yahoo_fantasy_api as yfa

from common import get_session

OUTPUT_DIR = Path("Data") / "players"


def _player_path(player_key: str) -> Path:
    # Accept numeric keys (int) by coercing to str first
    key_str = str(player_key)
    safe = key_str.replace("/", "_").replace(".", "_")
    return OUTPUT_DIR / f"{safe}.parquet"


def fetch_player(sc, player_key: str) -> Dict[str, Any]:
    """Fetch player metadata/stats for a given player_key.

    If `yahoo_fantasy_api.Player` is not present in the installed package, return a
    minimal serializable payload instead of raising.
    """
    try:
        PlayerCls = getattr(yfa, "Player")
    except Exception:
        PlayerCls = None

    # coerce numeric keys to string
    player_key = str(player_key)

    if PlayerCls is None:
        # Best-effort fallback: return repr and key only
        return {"player_key": player_key, "repr": f"Player({player_key}) - fallback"}

    plr = PlayerCls(sc, player_key)
    info = None
    stats = None
    try:
        info = plr.metadata()
    except Exception:
        info = None
    try:
        stats = plr.stats() if hasattr(plr, "stats") else None
    except Exception:
        stats = None

    return {"metadata": info, "stats": stats}


def transform_player(payload: Dict[str, Any]) -> pl.DataFrame:
    md = payload.get("metadata") or {}
    df = pl.DataFrame([md])
    return df


def save_player(df: pl.DataFrame, path: Path) -> None:
    # Deprecated: kept for compatibility
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="snappy")
    print(f"[DEBUG] Saved player to {path}")


def get_player(player_key: str) -> None:
    sc = get_session()
    payload = fetch_player(sc, player_key)
    df = transform_player(payload)
    path = _player_path(player_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="snappy")
    print(f"[DEBUG] Saved player to {path}")
