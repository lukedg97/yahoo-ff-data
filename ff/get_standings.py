from pathlib import Path
from typing import List, Dict

import polars as pl
import yahoo_fantasy_api as yfa

# Import shared helpers from package common to avoid duplication
from .common import get_session, select_league, DATA_DIR
from .common import save_parquet as _save_parquet

OUTPUT_PARQUET = DATA_DIR / "standings.parquet"


def fetch_standings(sc, league_key: str) -> List[Dict]:
    """Fetch standings for a specific league_key using yahoo_fantasy_api.League."""
    lg = yfa.League(sc, league_key)
    return lg.standings()


# Parquet validation is provided by common.validate_parquet


def transform_standings(raw: List[Dict]) -> pl.DataFrame:
    """Transform the Yahoo standings payload into a flat, Parquet-friendly DataFrame."""
    df = pl.DataFrame(raw)

    # Clean up/rename a few common columns if present
    rename_map = {
        "name": "Team",
        "wins": "W",
        "losses": "L",
        "ties": "T",
        "pct": "WinPct",
        "points_for": "PF",
        "points_against": "PA",
        "streak": "Streak",
        "rank": "Rank",
    }
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    # Normalize dtypes for Parquet compatibility and predictable sorting/display
    casts: list[pl.Expr] = []
    if "PF" in df.columns:
        casts.append(pl.col("PF").cast(pl.Float64).alias("PF"))
    if "PA" in df.columns:
        casts.append(pl.col("PA").cast(pl.Float64).alias("PA"))
    if "W" in df.columns:
        casts.append(pl.col("W").cast(pl.Int64).alias("W"))
    if "L" in df.columns:
        casts.append(pl.col("L").cast(pl.Int64).alias("L"))
    if "T" in df.columns:
        casts.append(pl.col("T").cast(pl.Int64).alias("T"))
    if "Rank" in df.columns:
        casts.append(pl.col("Rank").cast(pl.Int64, strict=False).alias("Rank"))
    if "WinPct" in df.columns:
        casts.append(pl.col("WinPct").cast(
            pl.Float64, strict=False).alias("WinPct"))
    if casts:
        df = df.with_columns(casts)

    # Convert Streak from nested struct/list (e.g., {"win","2"}) to flat columns + compact string
    if "Streak" in df.columns:
        df = df.with_columns([
            pl.col("Streak").map_elements(
                lambda s: (
                    s[0] if isinstance(s, (list, tuple)) and len(s) > 0 else
                    (s.get("type") if isinstance(s, dict) else None)
                )
            ).cast(pl.Utf8).alias("StreakType"),
            pl.col("Streak").map_elements(
                lambda s: (
                    int(s[1]) if isinstance(s, (list, tuple)) and len(s) > 1 and str(s[1]).isdigit() else
                    (int(s.get("value")) if isinstance(s, dict)
                     and str(s.get("value")).isdigit() else None)
                )
            ).cast(pl.Int64).alias("StreakLen"),
        ])
        df = (
            df.with_columns([
                pl.when(pl.col("StreakType").str.to_lowercase()
                        == "win").then(pl.lit("W"))
                  .when(pl.col("StreakType").str.to_lowercase() == "loss").then(pl.lit("L"))
                  .otherwise(pl.col("StreakType").fill_null("")).alias("_StreakLetter"),
            ])
            .with_columns([
                (pl.col("_StreakLetter") +
                 pl.col("StreakLen").cast(pl.Utf8).fill_null(""))
                .alias("StreakStr"),
            ])
            .drop(["Streak", "_StreakLetter"])  # drop nested and temp
            .rename({"StreakStr": "Streak"})
        )

    # Flatten nested outcome_totals into primitives (DuckDB-friendly)
    if "outcome_totals" in df.columns:
        df = df.with_columns([
            pl.col("outcome_totals").map_elements(lambda s: s.get("wins") if isinstance(s, dict) else (
                s[0] if isinstance(s, (list, tuple)) and len(s) > 0 else None)).alias("wins"),
            pl.col("outcome_totals").map_elements(lambda s: s.get("losses") if isinstance(s, dict) else (
                s[1] if isinstance(s, (list, tuple)) and len(s) > 1 else None)).alias("losses"),
            pl.col("outcome_totals").map_elements(lambda s: s.get("ties") if isinstance(s, dict) else (
                s[2] if isinstance(s, (list, tuple)) and len(s) > 2 else None)).alias("ties"),
            pl.col("outcome_totals").map_elements(lambda s: s.get("percentage") if isinstance(s, dict) else (
                s[3] if isinstance(s, (list, tuple)) and len(s) > 3 else None)).alias("percentage"),
        ])
        df = df.with_columns([
            pl.col("wins").cast(pl.Int64, strict=False),
            pl.col("losses").cast(pl.Int64, strict=False),
            pl.col("ties").cast(pl.Int64, strict=False),
            pl.col("percentage").cast(
                pl.Float64, strict=False).alias("WinPctFromTotals"),
        ]).drop(["outcome_totals"])  # drop nested struct
        if "WinPct" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("WinPct").is_null())
                  .then(pl.col("WinPctFromTotals"))
                  .otherwise(pl.col("WinPct"))
                  .alias("WinPct")
            ).drop(["WinPctFromTotals"])
        else:
            df = df.rename({"WinPctFromTotals": "WinPct"})

    # playoff_seed to Int64
    if "playoff_seed" in df.columns:
        df = df.with_columns(pl.col("playoff_seed").cast(
            pl.Int64, strict=False).alias("playoff_seed"))

    # Sorting preference
    if "Rank" in df.columns:
        df = df.with_columns(pl.col("Rank").cast(
            pl.Int64, strict=False).alias("_RankNum"))
        df = df.sort("_RankNum", descending=False,
                     nulls_last=True).drop("_RankNum")
    elif "WinPct" in df.columns:
        df = df.with_columns(pl.col("WinPct").cast(
            pl.Float64).alias("_WinPctNum"))
        extra_keys = [c for c in ["W", "wins"] if c in df.columns]
        sort_cols = ["_WinPctNum"] + extra_keys
        reverse = [True] * len(sort_cols)
        df = df.sort(sort_cols, descending=reverse,
                     nulls_last=True).drop("_WinPctNum")

    return df


def display_standings(df: pl.DataFrame) -> None:
    """Nicely print the standings subset to console."""
    print("\n=== Standings ===")
    cols = [c for c in ["Rank", "Team", "W", "L", "T",
                        "WinPct", "PF", "PA", "Streak"] if c in df.columns]
    df_display = df
    if "WinPct" in df_display.columns:
        try:
            df_display = df_display.with_columns(
                pl.col("WinPct").cast(pl.Float64).round(3))
        except Exception:
            pass
    if cols:
        print(df_display.select(cols))
    else:
        print(df_display)


def save_parquet(df: pl.DataFrame, path: Path) -> None:
    # Delegate to shared helper in common
    _save_parquet(df, path)


def get_standings() -> None:
    sc = get_session()
    league_key = select_league(sc, sport="nfl")
    if not league_key:
        print("No NFL leagues found for this account/season.")
        return
    print(f"Using league: {league_key}")

    standings = fetch_standings(sc, league_key)
    if not standings:
        print("No standings returned (season/league may not be active yet).")
        return

    df = transform_standings(standings)
    display_standings(df)
    save_parquet(df, OUTPUT_PARQUET)
