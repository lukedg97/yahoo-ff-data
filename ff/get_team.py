from pathlib import Path
from typing import Dict, Any

import polars as pl
import yahoo_fantasy_api as yfa

from .common import get_session
from .common import save_parquet, debug_print

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
        # Debug info: show a short repr and available attrs keys
        try:
            debug_print(
                f"[DEBUG] team_key={team_key} payload_repr={str(payload.get('repr'))[:200]}")
            debug_print(
                f"[DEBUG] payload attrs keys: {list((payload.get('attrs') or {}).keys())}")
        except Exception:
            pass

        # Flatten roster across all weeks (1..current_week) into a shared team_players.parquet
        try:
            # Determine league and current week from the team_key (league_key is the first two segments)
            try:
                league_key = ".".join(team_key.split(".")[:2])
                lg = yfa.League(sc, league_key)
                current_week = int(lg.current_week())
            except Exception:
                # Fallback: if we cannot determine current_week, default to 1
                current_week = 1

            all_rows = []
            # Iterate weeks from 1 to current_week inclusive
            for wk in range(1, current_week + 1):
                try:
                    team_obj = yfa.Team(sc, team_key)
                    try:
                        roster = team_obj.roster(wk)
                    except Exception:
                        roster = None

                    # If roster is missing, fallback to payload's roster once
                    if not roster:
                        roster = payload.get("attrs", {}).get(
                            "roster") or payload.get("roster") or []

                    week_payload = {"team_key": team_key, "repr": payload.get(
                        "repr"), "attrs": {"name": payload.get("attrs", {}).get("name"), "roster": roster}}
                    roster_df = roster_to_rows(
                        week_payload, team_key, sc, roster_week=wk)
                    if roster_df is not None and roster_df.shape[0] > 0:
                        all_rows.append(roster_df)
                except Exception as e:
                    debug_print(
                        f"[DEBUG] failed to fetch/flatten roster for {team_key} week={wk}: {e}")

            if all_rows:
                roster_df = pl.concat(all_rows, how="vertical")
            else:
                roster_df = None

            if roster_df is not None:
                debug_print(
                    f"[DEBUG] roster rows to append for {team_key}: {roster_df.shape[0]}")
            if roster_df is not None and roster_df.shape[0] > 0:
                # Normalize schema before append
                try:
                    roster_df = _normalize_team_players_df(roster_df)
                except Exception as e:
                    debug_print(
                        f"[DEBUG] normalization failed for {team_key}: {e}")
                from .common import DATA_DIR
                team_players_path = DATA_DIR / "team_players.parquet"
                # If file exists, read and concatenate; otherwise save fresh
                if team_players_path.exists():
                    existing = pl.read_parquet(team_players_path)
                    # Normalize existing before concatenation to avoid dtype mismatches
                    try:
                        existing = _normalize_team_players_df(existing)
                    except Exception:
                        pass
                    try:
                        existing_dtypes = {c: str(t) for c, t in zip(
                            existing.columns, existing.dtypes)}
                        debug_print(
                            f"[DEBUG] existing team_players dtypes: {existing_dtypes}")
                    except Exception:
                        pass
                    try:
                        incoming_dtypes = {c: str(t) for c, t in zip(
                            roster_df.columns, roster_df.dtypes)}
                        debug_print(
                            f"[DEBUG] incoming roster_df dtypes: {incoming_dtypes}")
                    except Exception:
                        pass
                    combined = pl.concat([existing, roster_df], how="vertical")
                    save_parquet(combined, team_players_path)
                else:
                    save_parquet(roster_df, team_players_path)
        except Exception as e:
            debug_print(
                f"[DEBUG] Failed to flatten roster for {team_key}: {e}")

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
        from .common import DATA_DIR
        standings_path = DATA_DIR / "standings.parquet"
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


def roster_to_rows(payload: Dict[str, Any], team_key: str, sc=None, roster_week: int | None = None) -> pl.DataFrame | None:
    """Convert a team's roster payload into a flat DataFrame with the columns:
    team_key, team_name, player, player_positions, week, team_position, points

    This function is defensive: roster entries can be dicts, strings, or nested structures.
    """
    attrs = payload.get("attrs") or {}
    team_name = attrs.get("name") or attrs.get(
        "team_name") or attrs.get("Team") or None

    # If team_name is missing, try to look it up from standings parquet
    if not team_name:
        try:
            from .common import DATA_DIR
            standings_path = DATA_DIR / "standings.parquet"
            if standings_path.exists():
                s = pl.read_parquet(standings_path)
                if "team_key" in s.columns:
                    row = s.filter(pl.col("team_key") == team_key)
                elif "teamKey" in s.columns:
                    row = s.filter(pl.col("teamKey") == team_key)
                else:
                    row = s.filter(pl.col("Team") == team_key)

                if row.shape[0] > 0:
                    # prefer Team column if present
                    if "Team" in row.columns:
                        team_name = row.select("Team").to_series()[0]
                    elif "name" in row.columns:
                        team_name = row.select("name").to_series()[0]
        except Exception:
            pass

    roster = attrs.get("roster")
    if roster is None:
        # Some payloads may put roster at top-level
        roster = payload.get("roster")
    if not roster:
        return None
    debug_print(
        f"[DEBUG] resolved team_name={team_name} for team_key={team_key}; roster_len={len(roster) if roster else 0}")
    try:
        sample = roster[0]
        debug_print(f"[DEBUG] sample roster item: {repr(sample)[:400]}")
    except Exception:
        pass

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
            # prefer explicit week in the roster item, otherwise use roster_week passed in
            week = item.get("week") or item.get("week_number") or roster_week
            team_position = item.get("selected_position") or item.get(
                "lineup_position") or item.get("slot")
            points = item.get("points") or item.get(
                "projected_points") or item.get("season_points")

            # Basic roster-level fields
            row.update({"player": player or name, "player_positions": positions,
                       "week": week, "team_position": team_position, "points": points})

            # Enrichment: add player metadata/stats if session available
            # Populate player_id, player_name, status, position_type, eligible_positions,
            # selected_position and include a serialized metadata/stats fallback
            row["player_id"] = player or None
            row["player_name"] = name or None
            row["player_status"] = item.get(
                "status") or item.get("injury_status") or None
            row["position_type"] = item.get("position_type") or None
            # normalize eligible_positions to a joined string later in normalize
            row["eligible_positions"] = positions
            row["selected_position"] = item.get(
                "selected_position") or item.get("selectedPos") or None
            # initialize structured enrichment fields
            row["player_metadata"] = None
            row["player_stats"] = None
            row["player_full_name"] = None
            row["primary_position"] = None
            # stat fields (defaults None)
            row["pass_yds"] = None
            row["pass_td"] = None
            row["interceptions"] = None
            row["rush_att"] = None
            row["rush_yds"] = None
            row["rush_td"] = None
            row["rec"] = None
            row["rec_yds"] = None
            row["rec_td"] = None
            row["targets"] = None
            row["fum_lost"] = None
            row["total_points"] = None

            if sc and player:
                try:
                    # import here to avoid circular imports at module load
                    from .get_player import fetch_player
                    # Request weekly stats by default; fall back to season if week is None
                    payload = fetch_player(
                        sc, player, stat_type="week", week=week)
                    if isinstance(payload, dict):
                        md = payload.get("metadata") or {}
                        st = payload.get("stats") or {}

                        # metadata
                        try:
                            # full_name preferred
                            row["player_full_name"] = md.get("full_name") or md.get(
                                "fullName") or md.get("full_name") or name
                        except Exception:
                            row["player_full_name"] = name
                        try:
                            row["primary_position"] = md.get(
                                "primary_position") or md.get("primaryPosition")
                        except Exception:
                            row["primary_position"] = None

                        # stats: map common keys to normalized columns
                        try:
                            # some stats use spaces and capitalization; handle permissively
                            def g(d, *keys):
                                for k in keys:
                                    if k in d:
                                        return d[k]
                                return None

                            row["pass_yds"] = g(
                                st, "Pass Yds", "PassYds", "pass_yds")
                            row["pass_td"] = g(
                                st, "Pass TD", "PassTD", "pass_td")
                            row["interceptions"] = g(
                                st, "Int", "INT", "interceptions")
                            row["rush_att"] = g(
                                st, "Rush Att", "RushAtt", "rush_att")
                            row["rush_yds"] = g(
                                st, "Rush Yds", "RushYds", "rush_yds")
                            row["rush_td"] = g(
                                st, "Rush TD", "RushTD", "rush_td")
                            row["rec"] = g(st, "Rec", "REC", "rec")
                            row["rec_yds"] = g(
                                st, "Rec Yds", "RecYds", "rec_yds")
                            row["rec_td"] = g(st, "Rec TD", "RecTD", "rec_td")
                            row["targets"] = g(st, "Targets", "targets")
                            row["fum_lost"] = g(
                                st, "Fum Lost", "FumLost", "fum_lost")
                            # total_points sometimes comes as string
                            tp = g(st, "total_points",
                                   "Total Points", "total_points")
                            try:
                                if tp is not None:
                                    row["total_points"] = float(str(tp))
                                else:
                                    row["total_points"] = None
                            except Exception:
                                row["total_points"] = None

                        except Exception:
                            # ignore any stat-parsing errors
                            pass
                except Exception:
                    # If enrichment fails, continue without additional fields
                    pass
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


def _normalize_team_players_df(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure the roster DataFrame has a stable schema and dtypes for safe appends.

    Canonical columns: team_key, team_name, player, player_positions, week, team_position, points
    """
    cols = ["team_key", "team_name", "player",
            "player_positions", "week", "team_position", "points",
            # enrichment fields
            "player_id", "player_name", "player_status", "position_type",
            "eligible_positions", "selected_position",
            # structured metadata/stats
            "player_full_name", "primary_position",
            "pass_yds", "pass_td", "interceptions",
            "rush_att", "rush_yds", "rush_td",
            "rec", "rec_yds", "rec_td", "targets", "fum_lost", "total_points"]
    # Add missing columns as nulls
    for c in cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))

    # Cast types conservatively
    # Detect list-like columns and convert them to joined strings to stabilize dtype
    try:
        dtypes = {c: str(t) for c, t in zip(df.columns, df.dtypes)}
    except Exception:
        # fallback: empty mapping
        dtypes = {}

    # If any canonical columns are Null-typed, replace them with a typed Null Utf8/Float/Int
    for c in cols:
        try:
            dtype = str(df[c].dtype)
        except Exception:
            dtype = ""
        if dtype == "Null":
            # create a typed-null series of the correct length
            if c in ("week",):
                df = df.with_columns(
                    pl.Series([None] * df.height).cast(pl.Int64).alias(c))
            elif c in ("points",):
                df = df.with_columns(
                    pl.Series([None] * df.height).cast(pl.Float64).alias(c))
            else:
                df = df.with_columns(
                    pl.Series([None] * df.height).cast(pl.Utf8).alias(c))

    # Convert player_positions and eligible_positions to a stable string representation by materializing the column
    if "player_positions" in df.columns:
        try:
            ser = df.select(pl.col("player_positions")).to_series()
            pyvals = ser.to_list()
            converted = []
            for v in pyvals:
                if isinstance(v, (list, tuple)):
                    converted.append(",".join([str(x) for x in v]))
                elif v is None:
                    converted.append(None)
                else:
                    converted.append(str(v))
            df = df.with_columns(pl.Series(converted).cast(
                pl.Utf8).alias("player_positions"))
        except Exception as e:
            debug_print(
                f"[DEBUG] _normalize: could not materialize/convert player_positions: {e}")

    # eligible_positions -> string
    if "eligible_positions" in df.columns:
        try:
            ser = df.select(pl.col("eligible_positions")).to_series()
            pyvals = ser.to_list()
            converted = []
            for v in pyvals:
                if isinstance(v, (list, tuple)):
                    converted.append(",".join([str(x) for x in v]))
                elif v is None:
                    converted.append(None)
                else:
                    converted.append(str(v))
            df = df.with_columns(pl.Series(converted).cast(
                pl.Utf8).alias("eligible_positions"))
        except Exception as e:
            debug_print(
                f"[DEBUG] _normalize: could not materialize/convert eligible_positions: {e}")

    # If some columns are Null-only, casting will set a concrete dtype so concatenation works
    df = df.with_columns([
        pl.col("team_key").cast(pl.Utf8, strict=False).alias("team_key"),
        pl.col("team_name").cast(pl.Utf8, strict=False).alias("team_name"),
        pl.col("player").cast(pl.Utf8, strict=False).alias("player"),
        pl.col("player_positions").cast(
            pl.Utf8, strict=False).alias("player_positions"),
        pl.col("player_id").cast(pl.Utf8, strict=False).alias("player_id"),
        pl.col("player_name").cast(pl.Utf8, strict=False).alias("player_name"),
        pl.col("player_status").cast(
            pl.Utf8, strict=False).alias("player_status"),
        pl.col("position_type").cast(
            pl.Utf8, strict=False).alias("position_type"),
        pl.col("eligible_positions").cast(
            pl.Utf8, strict=False).alias("eligible_positions"),
        pl.col("selected_position").cast(
            pl.Utf8, strict=False).alias("selected_position"),
        pl.col("player_metadata").cast(
            pl.Utf8, strict=False).alias("player_metadata"),
        pl.col("player_stats").cast(
            pl.Utf8, strict=False).alias("player_stats"),
        pl.col("player_full_name").cast(
            pl.Utf8, strict=False).alias("player_full_name"),
        pl.col("primary_position").cast(
            pl.Utf8, strict=False).alias("primary_position"),
        # numeric stat casts
        pl.col("pass_yds").cast(pl.Float64, strict=False).alias("pass_yds"),
        pl.col("pass_td").cast(pl.Float64, strict=False).alias("pass_td"),
        pl.col("interceptions").cast(
            pl.Float64, strict=False).alias("interceptions"),
        pl.col("rush_att").cast(pl.Float64, strict=False).alias("rush_att"),
        pl.col("rush_yds").cast(pl.Float64, strict=False).alias("rush_yds"),
        pl.col("rush_td").cast(pl.Float64, strict=False).alias("rush_td"),
        pl.col("rec").cast(pl.Float64, strict=False).alias("rec"),
        pl.col("rec_yds").cast(pl.Float64, strict=False).alias("rec_yds"),
        pl.col("rec_td").cast(pl.Float64, strict=False).alias("rec_td"),
        pl.col("targets").cast(pl.Float64, strict=False).alias("targets"),
        pl.col("fum_lost").cast(pl.Float64, strict=False).alias("fum_lost"),
        pl.col("total_points").cast(
            pl.Float64, strict=False).alias("total_points"),
        pl.col("team_position").cast(
            pl.Utf8, strict=False).alias("team_position"),
        pl.col("week").cast(pl.Int64, strict=False).alias("week"),
        pl.col("points").cast(pl.Float64, strict=False).alias("points"),
    ])

    # Attempt to fill missing team_name values from standings.parquet when present
    try:
        null_count = df.filter(pl.col("team_name").is_null()).height
        if null_count > 0:
            standings_path = Path("Data") / "standings.parquet"
            if standings_path.exists():
                s = pl.read_parquet(standings_path)
                key_col = None
                name_col = None
                if "team_key" in s.columns:
                    key_col = "team_key"
                elif "teamKey" in s.columns:
                    key_col = "teamKey"
                if "Team" in s.columns:
                    name_col = "Team"
                elif "name" in s.columns:
                    name_col = "name"

                if key_col and name_col:
                    keys = s.select(key_col).to_series().to_list()
                    names = s.select(name_col).to_series().to_list()
                    mapping = {k: v for k, v in zip(keys, names)}
                    mapped = [mapping.get(k) for k in df.select(
                        pl.col("team_key")).to_series().to_list()]
                    df = df.with_columns(pl.Series(mapped).cast(
                        pl.Utf8).alias("__mapped_name"))
                    df = df.with_columns(pl.when(pl.col("team_name").is_null()).then(
                        pl.col("__mapped_name")).otherwise(pl.col("team_name")).alias("team_name"))
                    df = df.drop("__mapped_name")
                    new_null_count = df.filter(
                        pl.col("team_name").is_null()).height
                    debug_print(
                        f"[DEBUG] filled {null_count - new_null_count} missing team_name entries from standings; remaining nulls={new_null_count}")
    except Exception as e:
        debug_print(f"[DEBUG] failed to fill team_name from standings: {e}")

    # Debug: show a small sample of player_positions after normalization
    try:
        sample_vals = df.select(
            pl.col("player_positions")).to_series().head(5).to_list()
        debug_print(
            f"[DEBUG] sample player_positions post-normalize: {sample_vals}")
    except Exception:
        pass

    # Ensure column order
    return df.select(cols)
