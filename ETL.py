#!/usr/bin/env python3

from ff.common import get_session, select_league, save_parquet, DATA_DIR
import polars as pl
import logging

# Reduce verbosity from third-party libraries (only show warnings/errors)
logging.getLogger("yahoo_oauth").setLevel(logging.WARNING)
logging.getLogger("yahoo_oauth.oauth").setLevel(logging.WARNING)


def main():
    # CLI: allow running specific parts of the ETL
    import argparse
    parser = argparse.ArgumentParser(description="Yahoo FF ETL runner")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug prints prefixed with [DEBUG]")
    parser.add_argument("--standings", action="store_true",
                        help="Fetch standings")
    parser.add_argument("--teams", action="store_true",
                        help="Fetch teams from standings and save per-team files")
    parser.add_argument("--test", action="store_true",
                        help="Run in test mode: process only one team (first) and truncate outputs before run")
    parser.add_argument("--players", action="store_true",
                        help="Fetch players for teams (requires --teams or existing teams) and save per-player files")
    parser.add_argument("--all", action="store_true",
                        help="Run standings, teams, and players")
    parser.add_argument("--inspect", action="store_true",
                        help="Inspect all Parquet files under Data/ and print a small sample and schema")
    import sys

    args = parser.parse_args()

    # Enable debug messages globally when requested
    if args.debug:
        import ff.common as _common
        _common.DEBUG = True

    # If the user invoked the script with no args (e.g. `python ETL.py`), default to --all
    # This is detected by checking sys.argv length == 1 (only the script name)
    if len(sys.argv) == 1:
        args.all = True
    else:
        # Otherwise preserve explicit flags; if none of the specific flags were set,
        # keep args.all False so the user must opt-in
        pass

    if args.all:
        args.standings = args.teams = args.players = True

    # Inspect mode: print sample + schema for every parquet under Data/ then exit
    if args.inspect:
        from pathlib import Path

        data_dir = DATA_DIR
        files = list(data_dir.glob("*.parquet"))
        players_dir = data_dir / "players"
        if players_dir.exists():
            files += list(players_dir.glob("*.parquet"))

        if not files:
            print("No parquet files found under Data/")
            return

        for p in files:
            try:
                df = pl.read_parquet(p)
                print(f"\nFile: {p} — shape={df.shape}")
                print("Schema:")
                for n, t in df.schema.items():
                    print(f" - {n}: {t}")
                print("Sample:")
                print(df.head(5))
            except Exception as e:
                print(f"Failed to read {p}: {e}")
        return

    if args.standings:
        from ff.get_standings import get_standings

        get_standings()

    # If teams or players, try to read the standings file for team keys
    if args.teams or args.players:
        from ff.get_team import get_team
        from ff.get_player import get_player
        from pathlib import Path

        OUTPUT_PARQUET = DATA_DIR / "standings.parquet"

        # Read cached standings parquet to extract team_key column
        try:
            df = pl.read_parquet(OUTPUT_PARQUET)
        except Exception:
            print(f"Could not read {OUTPUT_PARQUET}; run --standings first")
            return

        team_keys = []
        if "team_key" in df.columns:
            team_keys = df.select("team_key").to_series().to_list()
        elif "teamKey" in df.columns:
            team_keys = df.select("teamKey").to_series().to_list()
        else:
            print("No team key column found in standings parquet")

        # For each team, fetch and optionally fetch players
        # If test mode, truncate the canonical team_players parquet so we can profile a clean run
        if args.test:
            tp = DATA_DIR / "team_players.parquet"
            if tp.exists():
                try:
                    tp.unlink()
                    print(f"Test mode: removed existing {tp} to start fresh")
                except Exception as e:
                    print(f"Test mode: failed to remove {tp}: {e}")

        # If test mode, only process the first team key (if present)
        if args.test and team_keys:
            team_keys = team_keys[:1]

        for tk in team_keys:
            print(f"Processing team: {tk}")
            try:
                player_keys = get_team(tk) if args.teams else []
            except Exception as e:
                print(f"Failed to fetch team {tk}: {e}")
                player_keys = []

            if args.players:
                # If get_team returned player keys, fetch each player
                if player_keys:
                    for pk in player_keys:
                        try:
                            get_player(pk)
                        except Exception as e:
                            print(f"Failed to fetch player {pk}: {e}")


if __name__ == "__main__":
    main()
