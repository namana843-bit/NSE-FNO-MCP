# NSE-FNO-MCP

A Python Model Context Protocol (MCP) server for querying NSE India end-of-day Futures & Options (F&O) bhavcopy history.

**Supported instrument classes**
- `FUTSTK` — stock futures
- `FUTIDX` — index futures
- `OPTSTK` — stock options
- `OPTIDX` — index options

The server downloads NSE UDiFF F&O daily archive ZIP files, normalizes the data and stores it in DuckDB. ChatGPT or another MCP client can query the local database using MCP tools.

> **Data limitations:** This project targets end-of-day bhavcopy records, not intraday candles, tick data or a live option chain. NSE controls archive access and may change URL patterns, schemas, availability or automated-download rules. Verify sample records against NSE before using the data for research or trading.

## MCP tools

| Tool | Description |
|---|---|
| `sync_fno_history` | Download a bounded date range of daily archives and store them |
| `get_fno_history` | Query contract-wise OHLC, volume, OI and change in OI |
| `list_fno_contracts` | List symbols, expiries, instrument types and option sides |
| `get_oi_movers` | Rank contracts by daily change in OI |
| `get_database_status` | Inspect local database coverage |

## Quick start (local)

Requires Python 3.11+.

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
python server.py
```

The Streamable HTTP endpoint is:

```
http://localhost:8000/mcp
```

Use the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) to test the endpoint:

```bash
npx -y @modelcontextprotocol/inspector
```

## Sync a small test range first

Call `sync_fno_history` with known NSE trading dates. Example:

```json
{
  "start_date": "2026-11-02",
  "end_date": "2026-11-06"
}
```

Then query stock options:

```json
{
  "start_date": "2026-11-02",
  "end_date": "2026-11-06",
  "symbol": "TVSMOTOR",
  "instrument_type": "OPTSTK",
  "option_type": "CE",
  "limit": 500
}
```

For futures, use `FUTSTK` or `FUTIDX`. For index options, use `OPTIDX`.

The default sync limit is 31 calendar days, configurable with `MAX_SYNC_DAYS`. Sync larger periods in small chunks and check `get_database_status` after each run.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DATA_DIR` | `./data` | Persistent raw archives and DuckDB location |
| `PORT` | `8000` | HTTP port |
| `MAX_SYNC_DAYS` | `31` | Maximum calendar days per sync call |
| `MAX_QUERY_ROWS` | `2000` | Maximum rows returned per query |
| `REQUEST_DELAY_SECONDS` | `0.6` | Delay between archive downloads |

## Docker

Build and run with a persistent volume:

```bash
docker build -t nse-fno-mcp .
docker run --rm -p 8000:8000 \
  -e DATA_DIR=/app/data \
  -v nse-fno-data:/app/data \
  nse-fno-mcp
```

Endpoint: `http://localhost:8000/mcp`.

## Deploying a remote MCP endpoint

Deploy this Docker image to a Docker-compatible host. Configure:
- Persistent volume mounted at `/app/data`
- Port `8000` (or inject the platform's `PORT`)
- HTTPS ingress
- Authentication at the gateway or platform layer
- Request rate limits and restricted access to the sync tool

**Security:** This starter server does not implement OAuth or bearer-token validation itself. Do not expose it publicly without a secure authentication gateway. The `sync_fno_history` tool writes to the database and triggers external downloads; restrict it to trusted users. Read tools may be separately exposed with read-only credentials.

ChatGPT custom MCP availability and supported authentication modes vary by plan and workspace. Follow OpenAI's current documentation: https://developers.openai.com/

## Data schema

The normalized DuckDB table includes:
- Trade date, segment, instrument type and symbol
- Contract expiry and actual expiry (where available)
- Strike and option type
- Open, high, low, close, settlement and underlying price
- Traded volume, turnover and number of trades
- Open interest and change in open interest

## Data integrity checklist

Before research use:
1. Confirm the archive was an actual ZIP, not an NSE HTML access-denied page.
2. Confirm the UDiFF field mapping against the downloaded CSV header.
3. Compare a selection of contracts against the NSE report for the same date.
4. Check that all expected instrument types and option sides were loaded.
5. Record missing trading days and retry them if the exchange archive was temporarily unavailable.

No market data is bundled in this repository. Downloaded archives and databases are excluded by `.gitignore`.

## Disclaimer

This is an independent research utility and is not affiliated with or endorsed by NSE. It is not investment advice or a trading recommendation. Respect NSE website terms, data licensing and rate limits. Verify any data used for financial decisions.
