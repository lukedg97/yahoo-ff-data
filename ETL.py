#!/usr/bin/env python3

import json
from pathlib import Path
from typing import List, Dict

import pandas as pd
import yahoo_fantasy_api as yfa
from yahoo_oauth import OAuth2

OAUTH_FILE = "oauth2.json"
OUTPUT_JSON = "standings.json"
OUTPUT_CSV = "standings.csv"


def get_session() -> OAuth2:
    """
    Creates or refreshes an OAuth2 session using credentials in oauth2.json.
    On first run, this will open a browser window to log in and authorize.
    The yahoo_oauth library will also write/refresh token info back to oauth2.json.
    """
    oauth_path = Path(OAUTH_FILE)
    if not oauth_path.exists():
        raise FileNotFoundError(
            f"Missing {OAUTH_FILE}. Create it with your Yahoo client_id/client_secret/redirect_uri."
        )
    sc = OAuth2(None, None, from_file=str(oauth_path))
    if not sc.token_is_valid():
        sc.refresh_access_token()
    return sc


def find_league_ids(sc: OAuth2, sport: str = "nfl", year: int | None = None) -> List[str]:
    """
    Returns a list of league_keys for the given sport/year visible to the authorized user.
    Example league_key: '449.l.12345'
    """
    gm = yfa.Game(sc, sport)
    league_ids = gm.league_ids(year=year) if year else gm.league_ids()
    return league_ids


def fetch_standings(sc: OAuth2, league_key: str) -> List[Dict]:
    """
    Fetch standings for a specific league_key.
    """
    lg = yfa.League(sc, league_key)
    # This returns a list of dicts with keys like: name, team_key, wins, losses, ties, pct, points_for, points_against, etc.
    return lg.standings()


def main():
    sc = get_session()

    # Pick a league:
    # change to "nhl", "nba", "mlb" as needed
    league_ids = find_league_ids(sc, sport="nfl")
    if not league_ids:
        print("No NFL leagues found for this account/season.")
        return

    # If you have multiple leagues, pick the first by default.
    # You can also hardcode a specific league key once you know it.
    league_key = league_ids[0]
    print(f"Using league: {league_key}")

    standings = fetch_standings(sc, league_key)
    if not standings:
        print("No standings returned (season/league may not be active yet).")
        return

    # Pretty print + save
    df = pd.DataFrame(standings)
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
    df = df.rename(
        columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Sort safely (coerce to numeric to avoid str/int comparison issues)
    # Prefer Rank (asc). If missing/NaN, fall back to WinPct (desc), then Wins (desc).
    if "Rank" in df.columns:
        df["_RankNum"] = pd.to_numeric(df["Rank"], errors="coerce")
        df = df.sort_values(["_RankNum"], ascending=[True], na_position="last")
        df = df.drop(columns=["_RankNum"])
    elif "WinPct" in df.columns:
        df["_WinPctNum"] = pd.to_numeric(df["WinPct"], errors="coerce")
        # If wins/losses exist, include them as tie-breakers
        extra_keys = [c for c in ["W", "wins"] if c in df.columns]
        asc = [False] + [False] * len(extra_keys)
        df = df.sort_values(["_WinPctNum"] + extra_keys,
                            ascending=asc, na_position="last")
        df = df.drop(columns=["_WinPctNum"])
    else:
        # No rank/winpct—leave order as-is
        pass

    # Display to console
    print("\n=== Standings ===")
    cols = [c for c in ["Rank", "Team", "W", "L", "T",
                        "WinPct", "PF", "PA", "Streak"] if c in df.columns]
    # Format WinPct if present
    if "WinPct" in df.columns:
        try:
            df["WinPct"] = pd.to_numeric(df["WinPct"], errors="coerce").map(
                lambda x: f"{x:.3f}" if pd.notna(x) else ""
            )
        except Exception:
            pass
    print(df[cols].to_string(index=False)
          if cols else df.to_string(index=False))

    # Save files
    Path(OUTPUT_JSON).write_text(json.dumps(standings, indent=2))
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved: {OUTPUT_JSON} and {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
