from pathlib import Path
from typing import Dict, Any

import polars as pl
import yahoo_fantasy_api as yfa

from .common import get_session

# Per-player parquet writes removed by request; fetch/transform kept for enrichment

# Simple in-memory cache for player fetches within a single process/run.
# Keyed by player_key (string). This avoids repeated network calls when the
# same player appears multiple times during a teams run.
_PLAYER_CACHE: Dict[str, Dict[str, Any]] = {}


def fetch_player(sc, player_key: str, stat_type: str = "week", week: int | None = None) -> Dict[str, Any]:
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

    # Cache key includes requested stat granularity and week so weekly/season requests differ
    cache_key = f"{player_key}::{stat_type}::{week if week is not None else 'None'}"
    cached = _PLAYER_CACHE.get(cache_key)
    if cached is not None:
        return cached

    # If PlayerCls isn't available, continue and attempt league-based fallbacks below.
    # (Do not return early; we want to try league.player_details/player_stats.)

    plr = None
    info = None
    stats = None
    try:
        plr = PlayerCls(sc, player_key)
    except Exception:
        plr = None

    # Try Player class methods first (if available)
    if plr is not None:
        try:
            info = plr.metadata()
        except Exception:
            info = None
        try:
            stats = plr.stats() if hasattr(plr, "stats") else None
        except Exception:
            stats = None

    # If Player-based calls returned nothing, try League-based lookup as a fallback
    if (info is None or stats is None):
        try:
            # import lazily to avoid extra dependencies at module import time
            from .common import select_league
            league_key = select_league(sc)
            if league_key:
                lg = yfa.League(sc, league_key)
                # player_details accepts an int id or name
                try:
                    pid = int(player_key)
                except Exception:
                    pid = None

                # Try player_details
                try:
                    if pid is not None:
                        details = lg.player_details([pid])
                    else:
                        details = lg.player_details(str(player_key))
                    if details:
                        # details is a list of dicts
                        info = details[0]
                except Exception:
                    pass

                # Try player_stats with the requested granularity
                if stats is None:
                    try:
                        if pid is not None:
                            # If stat_type is 'week', pass the week number; otherwise use stat_type directly
                            if stat_type == "week":
                                if week is not None:
                                    # pass week as a keyword to match library signature
                                    st = lg.player_stats(
                                        [pid], "week", week=week)
                                else:
                                    # no week provided; fall back to season totals
                                    st = lg.player_stats([pid], "season")
                            else:
                                # e.g., 'season'
                                st = lg.player_stats([pid], stat_type)
                        else:
                            st = None
                        if st:
                            # player_stats returns a list aligned with the ids
                            stats = st[0]
                    except Exception:
                        pass
        except Exception:
            # any fallback failure is non-fatal; return whatever we have
            pass

    result = {"metadata": info, "stats": stats}
    # store result in cache (even if None values) to avoid repeated attempts
    try:
        _PLAYER_CACHE[cache_key] = result
    except Exception:
        # non-fatal: if cache write fails, just return the result
        pass

    return result


def transform_player(payload: Dict[str, Any]) -> pl.DataFrame:
    md = payload.get("metadata") or {}
    df = pl.DataFrame([md])
    return df


def save_player(df: pl.DataFrame, path: Path) -> None:
    # Deprecated: removed to avoid writing per-player parquet files.
    from .common import debug_print
    debug_print(
        "[DEBUG] save_player called but per-player writes are disabled")


def get_player(player_key: str) -> None:
    sc = get_session()
    payload = fetch_player(sc, player_key)
    df = transform_player(payload)
    # Previously this function wrote per-player parquet files. Writes are disabled.
    from .common import debug_print
    debug_print(
        f"[DEBUG] get_player fetched payload for {player_key} (writes disabled)")
