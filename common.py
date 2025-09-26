from pathlib import Path
from typing import List

import polars as pl
import yahoo_fantasy_api as yfa
from yahoo_oauth import OAuth2

# Shared configuration used by multiple modules
DATA_DIR = Path("Data")
OAUTH_FILE = "oauth2.json"

# Global debug flag controlled by ETL.py
DEBUG = False


def debug_print(msg: str) -> None:
    """Print debug messages only when DEBUG is enabled."""
    if DEBUG:
        print(msg)


def get_session() -> OAuth2:
    """
    Create or refresh an OAuth2 session using credentials in `oauth2.json`.
    Shared by multiple fetchers (standings, team, player).
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
    Return a list of league_keys for the given sport/year visible to the authorized user.
    Example league_key: '449.l.12345'
    """
    gm = yfa.Game(sc, sport)
    league_ids = gm.league_ids(year=year) if year else gm.league_ids()
    return league_ids


def select_league(sc: OAuth2, sport: str = "nfl") -> str | None:
    """Return a league_key for the given sport or None if none are found."""
    league_ids = find_league_ids(sc, sport=sport)
    if not league_ids:
        return None
    return league_ids[0]


def validate_parquet(path: Path) -> None:
    """Validate that a Parquet file can be read and print a short schema preview."""
    try:
        df_check = pl.read_parquet(path)
    except Exception as e:
        print(f"\n❌ Parquet validation failed for {path}:", e)
        return

    debug_print("Schema (column: dtype):")
    for name, dtype in df_check.schema.items():
        debug_print(f" - {name}: {dtype}")

    try:
        import duckdb  # type: ignore
        con = duckdb.connect(":memory:")
        con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(path)])
        print("DuckDB validation: ✅ read_parquet() succeeded")
    except Exception as e:
        print("DuckDB validation: ⚠️", e)


def save_parquet(df: pl.DataFrame, path: Path) -> None:
    """Ensure directory exists, write Parquet (snappy), and run validation."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="snappy")
    print(f"\nSaved: {path}")
    validate_parquet(path)
