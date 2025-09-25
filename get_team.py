from pathlib import Path
from typing import Dict, Any

import polars as pl
import yahoo_fantasy_api as yfa

from common import get_session
from common import save_parquet

# We no longer persist per-team parquet files; only build flat Data/team_players.parquet


def fetch_team(sc, team_key: str) -> Dict[str, Any]:
    """Fetch the raw Team object and return a simple serializable summary.

    We intentionally return a minimal structure: team_key, repr(team_obj), and a small
    attrs dict with common attribute names if present. This guarantees the payload
    is serializable and easy to inspect while we iterate on a final schema.
    """
    team_obj = yfa.Team(sc, team_key)

    summary = {"team_key": team_key, "repr": repr(team_obj), "attrs": {}}

    # Try to capture a few common attributes; ignore failures
    for name in ("name", "team_key", "teamKey", "roster", "managers", "team_id", "players"):
        try:
            val = getattr(team_obj, name, None)
            if val is not None:
                summary["attrs"][name] = val
        except Exception:
            # Some attributes might be callables or require network calls; skip them
            pass

    # If there is an explicit roster() method that returns data, try to include it
    try:
        if hasattr(team_obj, "roster") and callable(team_obj.roster):
            r = team_obj.roster()
            summary["attrs"]["roster"] = r
    except Exception:
        pass

    return summary


def transform_team(payload: Dict[str, Any]) -> pl.DataFrame:
    """Turn the serializable team summary into a one-row DataFrame.

    The payload shape is intentionally simple (team_key, repr, attrs dict). We
    flatten attrs keys into top-level columns where possible.
    """
    md = {"team_key": payload.get("team_key"), "repr": payload.get("repr")}
    attrs = payload.get("attrs") or {}
    # Flatten simple scalar attributes into md
    for k, v in attrs.items():
        # Only include scalar-ish values; lists/dicts will be kept as JSON-like repr
        if isinstance(v, (str, int, float, bool)) or v is None:
            md[k] = v
        else:
            md[k] = repr(v)

    df = pl.DataFrame([md])
    return df


# per-team writes removed


def get_team(team_key: str) -> list:
    """Fetch, transform, save a team and return a list of player_keys on the roster.

    Returns an empty list if no roster/player keys found.
    """
    sc = get_session()
    try:
        payload = fetch_team(sc, team_key)
        df = transform_team(payload)

        # Flatten roster into a shared team_players.parquet
        try:
            roster_df = roster_to_rows(payload, team_key)
            if roster_df is not None and roster_df.shape[0] > 0:
                team_players_path = Path("Data") / "team_players.parquet"
                # If file exists, read and concatenate; otherwise save fresh
                if team_players_path.exists():
                    existing = pl.read_parquet(team_players_path)
                    combined = pl.concat([existing, roster_df], how="vertical")
                    save_parquet(combined, team_players_path)
                else:
                    save_parquet(roster_df, team_players_path)
        except Exception as e:
            print(f"Failed to flatten roster for {team_key}: {e}")

        # Extract player keys from roster payload if available
        player_keys = []
        roster = payload.get("roster") or payload.get(
            "attrs", {}).get("roster") or []
        for item in roster:
            if isinstance(item, dict):
                # common field names
                pk = item.get("player_key") or item.get(
                    "playerKey") or item.get("player_id") or item.get("playerId")
                if pk:
                    player_keys.append(pk)
            elif isinstance(item, str):
                player_keys.append(item)

        return player_keys
    except Exception as e:
        # Fallback: try to locate the team row in standings, but do not write per-team files
        print(
            f"Team API fetch failed for {team_key}: {e}. Falling back to standings row.")
        standings_path = Path("Data") / "standings.parquet"
        try:
            s = pl.read_parquet(standings_path)
            # try multiple team_key column names
            if "team_key" in s.columns:
                row = s.filter(pl.col("team_key") == team_key)
            elif "teamKey" in s.columns:
                row = s.filter(pl.col("teamKey") == team_key)
            else:
                row = s.filter(pl.col("Team") == team_key)

            if row.shape[0] > 0:
                # no per-team file is written; nothing to append
                return []
        except Exception:
            pass

        # Last resort: nothing to append
        return []


def roster_to_rows(payload: Dict[str, Any], team_key: str) -> pl.DataFrame | None:
    """Convert a team's roster payload into a flat DataFrame with the columns:
    team_key, team_name, player, player_positions, week, team_position, points

    This function is defensive: roster entries can be dicts, strings, or nested structures.
    """
    attrs = payload.get("attrs") or {}
    team_name = attrs.get("name") or attrs.get(
        "team_name") or attrs.get("Team") or None

    roster = attrs.get("roster")
    if roster is None:
        # Some payloads may put roster at top-level
        roster = payload.get("roster")
    if not roster:
        return None

    rows = []
    for item in roster:
        row = {"team_key": team_key, "team_name": team_name}
        # item could be a dict with varying keys
        if isinstance(item, dict):
            # common field names
            player = item.get("player_key") or item.get(
                "playerKey") or item.get("player_id") or item.get("id")
            name = item.get("name") or item.get(
                "full_name") or item.get("player_name")
            positions = item.get("eligible_positions") or item.get(
                "positions") or item.get("position")
            week = item.get("week") or item.get("week_number")
            team_position = item.get("selected_position") or item.get(
                "lineup_position") or item.get("slot")
            points = item.get("points") or item.get(
                "projected_points") or item.get("season_points")
            row.update({"player": player or name, "player_positions": positions,
                       "week": week, "team_position": team_position, "points": points})
        elif isinstance(item, str):
            row.update({"player": item, "player_positions": None,
                       "week": None, "team_position": None, "points": None})
        else:
            # Fallback: store repr
            row.update({"player": repr(item), "player_positions": None,
                       "week": None, "team_position": None, "points": None})

        rows.append(row)

    if not rows:
        return None

    df = pl.DataFrame(rows)
    return df
