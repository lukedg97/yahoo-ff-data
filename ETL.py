#!/usr/bin/env python3

from common import get_session, select_league, save_parquet
import polars as pl
import logging

# Reduce verbosity from third-party libraries (only show warnings/errors)
logging.getLogger("yahoo_oauth").setLevel(logging.WARNING)
logging.getLogger("yahoo_oauth.oauth").setLevel(logging.WARNING)


def main():
    # CLI: allow running specific parts of the ETL
    import argparse
    parser = argparse.ArgumentParser(description="Yahoo FF ETL runner")
    parser.add_argument("--standings", action="store_true",
                        help="Fetch standings")
    parser.add_argument("--teams", action="store_true",
                        help="Fetch teams from standings and save per-team files")
    parser.add_argument("--players", action="store_true",
                        help="Fetch players for teams (requires --teams or existing teams) and save per-player files")
    parser.add_argument("--all", action="store_true",
                        help="Run standings, teams, and players")
    args = parser.parse_args()

    # If no flags provided, behave like --all
    if not any([args.standings, args.teams, args.players, args.all]):
        args.all = True

    if args.all:
        args.standings = args.teams = args.players = True

    if args.standings:
        from get_standings import get_standings

        get_standings()

    # If teams or players, try to read the standings file for team keys
    if args.teams or args.players:
        from get_standings import OUTPUT_PARQUET
        from get_team import get_team
        from get_player import get_player

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
        for tk in team_keys:
            print(f"Processing team: {tk}")
            try:
                player_keys = get_team(tk) if args.teams else []
            except Exception as e:
                print(f"Failed to fetch team {tk}: {e}")
                player_keys = []

            if args.players:
                # If get_team returned player keys, use them; otherwise attempt to derive none
                if player_keys:
                    for pk in player_keys:
                        try:
                            get_player(pk)
                        except Exception as e:
                            print(f"Failed to fetch player {pk}: {e}")


if __name__ == "__main__":
    main()
