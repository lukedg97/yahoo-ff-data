#!/usr/bin/env python3
"""Small diagnostic helper: inspect Data/team_players.parquet for nulls in player/stat columns.

Usage: python3 inspect_team_players.py
"""
from pathlib import Path
import polars as pl

DATA_DIR = Path(__file__).parent / "Data"
TP_PATH = DATA_DIR / "team_players.parquet"

if not TP_PATH.exists():
    print(f"Missing {TP_PATH}")
    raise SystemExit(1)

print(f"Reading {TP_PATH}...")
df = pl.read_parquet(TP_PATH)
print(f"Rows: {df.height}, Columns: {df.width}\n")

# Candidate player/stat columns we expect
player_cols = [
    'player_id', 'player_name', 'player_status', 'position_type', 'eligible_positions', 'selected_position',
    'player_full_name', 'primary_position'
]
stat_cols = [
    'pass_yds', 'pass_td', 'interceptions', 'rush_att', 'rush_yds', 'rush_td',
    'rec', 'rec_yds', 'rec_td', 'targets', 'fum_lost', 'total_points'
]

existing_player_cols = [c for c in player_cols if c in df.columns]
existing_stat_cols = [c for c in stat_cols if c in df.columns]

print('Player metadata columns found:', existing_player_cols)
print('Stat columns found:', existing_stat_cols)

# Null counts
nulls = {}
for c in existing_player_cols + existing_stat_cols:
    nulls[c] = int(df[c].null_count())

print('\nNull counts (column: nulls / rows):')
for c in sorted(nulls.keys()):
    print(f" - {c}: {nulls[c]} / {df.height}")

# Show a few non-null samples per stat column (first 3 non-null rows)
print('\nSample non-null rows for stat columns:')
for c in existing_stat_cols:
    non_null = df.filter(pl.col(c).is_not_null()).select(
        ['team_key', 'player_name', c])
    print(f"\nColumn: {c} - non-null rows: {non_null.height}")
    if non_null.height > 0:
        print(non_null.head(3).to_pandas())

# Show rows where all stat columns are null.
# Polars doesn't like concat_list of boolean Series for this use in some versions,
# so build the predicate by chaining '&' or fall back to pandas for the final check.
if existing_stat_cols:
    try:
        # Build chained predicate: col1.is_null() & col2.is_null() & ...
        predicate = None
        for c in existing_stat_cols:
            expr = pl.col(c).is_null()
            predicate = expr if predicate is None else (predicate & expr)
        all_null_stats = df.filter(predicate).select(
            ['team_key', 'player_name'] + existing_stat_cols)
        print(
            f"\nRows with all stat columns null: {all_null_stats.height} / {df.height}")
        if all_null_stats.height > 0:
            print(all_null_stats.head(5).to_pandas())
    except Exception:
        # Last-resort: convert relevant columns to pandas and compute.
        print(
            '\nFalling back to pandas for all-null-stats check due to Polars limitation...')
        pdf = df.select(['team_key', 'player_name'] +
                        existing_stat_cols).to_pandas()
        all_null_mask = pdf[existing_stat_cols].isnull().all(axis=1)
        all_null_stats_pdf = pdf.loc[all_null_mask]
        print(
            f"Rows with all stat columns null: {len(all_null_stats_pdf)} / {df.height}")
        if len(all_null_stats_pdf) > 0:
            print(all_null_stats_pdf.head(5))

print('\nDone.')
