# yahoo-ff-data

Minimal Python tooling to authenticate with the **Yahoo Fantasy Sports API** (OAuth2) and export league **standings** to JSON/CSV.

## What this does
- Uses `yahoo_oauth` for the 3‑legged OAuth2 flow (opens a browser on first run).
- Uses `yahoo_fantasy_api` to enumerate your NFL league(s) and fetch standings.
- Writes outputs to `standings.json` and `standings.csv`.

---

## Prerequisites
- Python 3.9+ recommended
- A Yahoo Developer app with **Fantasy Sports** permission enabled

Create an app in the Yahoo Developer Dashboard and note your **Client ID** and **Client Secret**. Add a redirect URI such as:
```
https://localhost/
```

---

## Setup

1) **Clone & create a virtualenv**
```bash
git clone https://github.com/yourusername/yahoo-ff-data.git
cd yahoo-ff-data
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

2) **Install dependencies**
```bash
pip install -r requirements.txt
```

3) **Create `oauth2.json` (local, contains your secrets)**
Create a file `oauth2.json` in the project root with:
```json
{
  "consumer_key": "YOUR_CLIENT_ID",
  "consumer_secret": "YOUR_CLIENT_SECRET",
  "redirect_uri": "https://localhost/",
  "token_time": 0
}
```

> ℹ️ On first successful login, this file will be updated with `access_token` and `refresh_token` fields so you won’t have to re‑authorize each run.

## Run

This repo expects a script named **`ETL.py`** that:
- performs the OAuth flow,
- auto-detects your NFL league(s),
- prints standings and saves outputs.

Run it with:
```bash
python ETL.py
```

On first run it will open a browser to sign in and authorize the app.

Outputs:
- `standings.json` — raw standings list/dicts
- `standings.csv` — tidy table (Rank, Team, W, L, T, WinPct, PF, PA, …)