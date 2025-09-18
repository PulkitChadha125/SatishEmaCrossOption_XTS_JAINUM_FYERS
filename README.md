# Satish EMA Cross Option - XTS + JAINAM + FYERS

A trading automation project that integrates:
- Fyers API (authentication, quotes, history, WebSocket)
- XTS Connect (interactive + market data)
- Strategy based on EMA crossovers using Polars + polars-ta

## Prerequisites
- Python 3.12 (recommended)
- Git
- Fyers API credentials and TOTP
- XTS Connect credentials (Interactive and Market Data)

## Setup
1) Clone the repo
```bash
git clone https://github.com/PulkitChadha125/SatishEmaCrossOption_XTS_JAINUM_FYERS.git
cd SatishEmaCrossOption_XTS_JAINUM_FYERS
```

2) Create and activate a virtual environment
```powershell
python -m venv .venv
. .venv/Scripts/Activate.ps1
```

3) Install dependencies
```bash
pip install -r requirements.txt
```

## Configuration
- `Credentials.csv`: XTS keys for Interactive and Market Data
- `FyersCredentials.csv`: Fyers client_id, secret, redirect_uri, FY_ID, PIN, TOTP key, etc.
- `TradeSettings.csv`: Symbols, MA1, MA2, quantity, timeframe, timing, placement type, strike step, etc.

Note: `config.ini` is ignored by git. Runtime copies may be required in project root for XTS SDK reads.

## Run
```powershell
python main.py
```
This will:
- Login to Fyers automatically (OTP via TOTP + PIN flow)
- Login to XTS Market Data and Interactive
- Subscribe to websockets for live prices
- Execute the EMA cross strategy per `TradeSettings.csv`

## Key Files
- `main.py`: Core strategy loop and order routing
- `FyresIntegration.py`: Fyers auth, quotes, history, websocket helpers
- `xtspythonclientapisdk/`: XTS REST + Socket client (vendored)

## Notes
- `.gitignore` excludes `.venv`, `main.exe`, logs, and local configs
- The strategy writes logs to `OrderLog.txt`
- EMA calculation uses Polars (`polars`) and TA helpers (`polars-ta`)

## Build (optional)
If you plan to bundle:
```powershell
pip install pyinstaller
pyinstaller --onefile main.py
```
Avoid committing `dist/`, `build/`, and the generated `main.exe`.

## License
Proprietary - internal project.
