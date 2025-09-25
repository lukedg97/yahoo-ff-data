from pathlib import Path
from typing import Dict, Any

import polars as pl
import yahoo_fantasy_api as yfa

from common import get_session

OUTPUT_DIR = Path("Data") / "players"


def _player_path(player_key: str) -> Path:
    safe = player_key.replace("/", "_").replace(".", "_")
    return OUTPUT_DIR / f"{safe}.parquet"


def fetch_player(sc, player_key: str) -> Dict[str, Any]:
    """Fetch player metadata/stats for a given player_key."""
    plr = yfa.Player(sc, player_key)
    info = plr.metadata()
    stats = plr.stats() if hasattr(plr, "stats") else None
    return {"metadata": info, "stats": stats}


def transform_player(payload: Dict[str, Any]) -> pl.DataFrame:
    md = payload.get("metadata") or {}
    df = pl.DataFrame([md])
    return df


def save_player(df: pl.DataFrame, path: Path) -> None:
    # Deprecated: kept for compatibility
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="snappy")
    print(f"Saved player to {path}")


def get_player(player_key: str) -> None:
    sc = get_session()
    payload = fetch_player(sc, player_key)
    df = transform_player(payload)
    path = _player_path(player_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="snappy")
    print(f"Saved player to {path}")
