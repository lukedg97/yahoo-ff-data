#!/usr/bin/env python3
"""Improved API inspector.

This script fetches one example each: league_id, a team summary, and a player
detail/stat sample. It uses the documented Game/League/Team APIs and prints
small JSON-serializable summaries. Use --debug to show tracebacks.
"""

from __future__ import annotations

import argparse
import json
import traceback
from typing import Any

from ff.common import get_session, DEBUG, debug_print
import yahoo_fantasy_api as yfa


def safe_print(obj: Any) -> None:
    try:
        print(json.dumps(obj, default=str, indent=2))
    except Exception:
        print(repr(obj))


def main():
    p = argparse.ArgumentParser(description="Minimal Yahoo API inspector")
    p.add_argument("--game", default="nfl",
                   help="game code (mlb, nfl, nba, etc.)")
    p.add_argument("--debug", action="store_true", help="show tracebacks")
    args = p.parse_args()

    # honor debug flag in common DEBUG too for consistency with ETL
    if args.debug:
        try:
            # set module-level flag if present
            globals()["DEBUG"] = True
            debug_print("debug enabled")
        except Exception:
            pass

    sc = get_session()

    # 1) Game -> league ids
    try:
        gm = yfa.Game(sc, args.game)
        league_ids = gm.league_ids(is_available=True) or []
        if not league_ids:
            print("No leagues found for the authenticated user.")
            return
        league_id = league_ids[0]
        print("League sample:")
        safe_print({"league_id": league_id,
                   "available_league_count": len(league_ids)})
    except Exception as e:
        print("Failed to fetch leagues:", str(e))
        if args.debug:
            traceback.print_exc()
        return

    # 2) League -> teams / standings -> sample team
    teams_dict = {}
    try:
        lg = yfa.League(sc, league_id)
        try:
            teams_dict = lg.teams() or {}
        except Exception:
            # some versions may return weird types; fallback to empty
            teams_dict = {}

        if teams_dict:
            # teams() returns dict keyed by team_key
            sample_team_key = next(iter(teams_dict))
            sample_team = teams_dict[sample_team_key]
            print("Team sample (from League.teams):")
            safe_print({"team_key": sample_team_key,
                       "name": sample_team.get("name")})
            # try to fetch roster via Team object
            try:
                tm = lg.to_team(sample_team_key)
                roster = tm.roster() or []
                print("Roster sample (first 5):")
                safe_print(roster[:5])
            except Exception as e:
                print("Failed to instantiate/inspect Team roster:", str(e))
                if args.debug:
                    traceback.print_exc()
        else:
            # fallback to standings
            try:
                standings = lg.standings() or []
                if standings:
                    s0 = standings[0]
                    print("Team sample (from standings):")
                    safe_print({"team_key": s0.get("team_key"),
                               "name": s0.get("name")})
                    # attempt to construct a Team from the team_key
                    tk = s0.get("team_key")
                    if tk:
                        try:
                            tm = lg.to_team(tk)
                            roster = tm.roster() or []
                            print("Roster sample (first 5):")
                            safe_print(roster[:5])
                        except Exception as e:
                            print(
                                "Failed to instantiate/inspect Team roster:", str(e))
                            if args.debug:
                                traceback.print_exc()
                else:
                    print("No teams or standings returned for league", league_id)
            except Exception as e:
                print("Failed to fetch standings:", str(e))
                if args.debug:
                    traceback.print_exc()
    except Exception as e:
        print("Failed to fetch teams for league:", str(e))
        if args.debug:
            traceback.print_exc()

    # 3) Player inspect: prefer league.player_details/player_stats
    try:
        sample_player_id = None
        # extract from roster if available
        if teams_dict:
            # use the first team's roster pulled earlier if present
            try:
                tm_key = next(iter(teams_dict))
                tm = lg.to_team(tm_key)
                roster = tm.roster() or []
                if roster:
                    first = roster[0]
                    # roster entries are dicts with player_id usually int or str
                    sample_player_id = first.get("player_id") or first.get(
                        "playerKey") or first.get("player_key")
            except Exception:
                sample_player_id = None

        # fallback: use player search on a common name (try 'Smith') if no player found
        if not sample_player_id:
            # Try to search by name using player_details (string search)
            try:
                found = lg.player_details("Smith")
                if found:
                    sample_player_id = found[0].get(
                        "player_id") or found[0].get("playerId")
            except Exception:
                sample_player_id = None

        if not sample_player_id:
            print("No player id found for inspection")
            return

        # coerce numeric string -> int where appropriate
        try:
            pid_int = int(sample_player_id)
        except Exception:
            pid_int = None

        # prefer player_details (can accept name or int list)
        try:
            if pid_int is not None:
                pd = lg.player_details([pid_int])
            else:
                pd = lg.player_details(str(sample_player_id))
            if not pd:
                print("player_details returned empty for", sample_player_id)
                return
            print("Player details sample:")
            # print a small selection of fields
            p0 = pd[0]
            safe_print({
                "player_id": p0.get("player_id") or p0.get("playerId"),
                "player_key": p0.get("player_key") or p0.get("playerKey") or p0.get("player_key"),
                "full_name": (p0.get("name") or {}).get("full") if isinstance(p0.get("name"), dict) else p0.get("name"),
                "primary_position": p0.get("primary_position") or p0.get("display_position")
            })

            # attempt to fetch stats for the player (season sample)
            try:
                pid_for_stats = pid_int if pid_int is not None else int(
                    p0.get("player_id"))
                stats = lg.player_stats([pid_for_stats], "season")
                if stats:
                    print("Player stats (season) sample:")
                    safe_print(stats[0])
            except Exception as e:
                print("Failed to fetch player stats:", str(e))
                if args.debug:
                    traceback.print_exc()

        except Exception as e:
            print("player_details failed:", str(e))
            if args.debug:
                traceback.print_exc()

    except Exception as e:
        print("Player inspection failed:", str(e))
        if args.debug:
            traceback.print_exc()


if __name__ == "__main__":
    main()
