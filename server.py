"""Read-only query and controlled sync MCP for NSE UDiFF F&O bhavcopy.

Important:
- End-of-day bhavcopy only; not intraday/tick data.
- NSE archives may throttle/block automated downloads.
- Validate the current archive URL and column schema against NSE before production.
- Protect this service with MCP bearer authentication at the deployment gateway.
"""
from __future__ import annotations

import io
import os
import re
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import requests
from mcp.server.fastmcp import FastMCP

APP_NAME = "NSE F&O Market Data MCP"
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "nse_fno.duckdb"
PORT = int(os.getenv("PORT", "8000"))
MAX_SYNC_DAYS = int(os.getenv("MAX_SYNC_DAYS", "31"))
MAX_QUERY_ROWS = int(os.getenv("MAX_QUERY_ROWS", "2000"))
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "0.6"))

DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

mcp = FastMCP(APP_NAME)

# NSE UDiFF daily F&O Common Bhavcopy archive pattern.
ARCHIVE_URL = (
    "https://nsearchives.nseindia.com/content/fo/"
    "BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/131.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/zip,*/*",
    "Referer": "https://www.nseindia.com/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

VALID_INSTRUMENTS = {"FUTSTK", "FUTIDX", "OPTSTK", "OPTIDX"}

# UDiFF field names. Some archive revisions may vary; unexpected schemas
# are rejected and surfaced rather than silently generating incorrect data.
FIELD_ALIASES = {
    "trade_date": ["BizDt", "TradDt"],
    "segment": ["Sgmt"],
    "instrument_type": ["FinInstrmTp"],
    "symbol": ["TckrSymb"],
    "expiry": ["XpryDt"],
    "actual_expiry": ["FinInstrmActlXpryDt"],
    "strike": ["StrkPric"],
    "option_type": ["OptnTp"],
    "contract_name": ["FinInstrmNm"],
    "open": ["OpnPric"],
    "high": ["HghPric"],
    "low": ["LwPric"],
    "close": ["ClsPric"],
    "settlement": ["SttlmPric"],
    "underlying_price": ["UndrlygPric"],
    "volume": ["TtlTradgVol"],
    "turnover": ["TtlTrfVal"],
    "trades": ["TtlNbOfTxsExctd"],
    "oi": ["OpnIntrst"],
    "change_oi": ["ChngInOpnIntrst"],
}

NUMERIC_COLUMNS = [
    "strike", "open", "high", "low", "close", "settlement",
    "underlying_price", "volume", "turnover", "trades", "oi",
    "change_oi",
]

DB_COLUMNS = [
    "trade_date", "segment", "instrument_type", "symbol", "expiry",
    "actual_expiry", "strike", "option_type", "contract_name", "open",
    "high", "low", "close", "settlement", "underlying_price", "volume",
    "turnover", "trades", "oi", "change_oi",
]


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ValueError("Dates must use YYYY-MM-DD format") from exc


def _validate_symbol(symbol: str | None) -> str | None:
    if symbol is None or not symbol.strip():
        return None
    value = symbol.strip().upper()
    if not re.fullmatch(r"[A-Z0-9_-]{1,30}", value):
        raise ValueError("Invalid NSE symbol")
    return value


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute("""
        CREATE TABLE IF NOT EXISTS fno_bhavcopy (
            trade_date DATE,
            segment VARCHAR,
            instrument_type VARCHAR,
            symbol VARCHAR,
            expiry DATE,
            actual_expiry DATE,
            strike DOUBLE,
            option_type VARCHAR,
            contract_name VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            settlement DOUBLE,
            underlying_price DOUBLE,
            volume BIGINT,
            turnover DOUBLE,
            trades BIGINT,
            oi BIGINT,
            change_oi BIGINT
        )
    """)
    return con


def _find_column(columns: list[str], aliases: list[str]) -> str | None:
    lookup = {str(col).strip().lstrip("\ufeff").lower(): col for col in columns}
    for alias in aliases:
        if alias.lower() in lookup:
            return lookup[alias.lower()]
    return None


def _normalize(raw: pd.DataFrame, expected_date: date) -> pd.DataFrame:
    raw.columns = [str(c).strip().lstrip("\ufeff") for c in raw.columns]
    result = pd.DataFrame(index=raw.index)

    for target, candidates in FIELD_ALIASES.items():
        source = _find_column(list(raw.columns), candidates)
        result[target] = raw[source] if source else pd.NA

    if result["symbol"].isna().all():
        raise ValueError(
            "UDiFF symbol column not found. Archive schema may have changed. "
            f"Received columns: {list(raw.columns)}"
        )

    result["trade_date"] = pd.to_datetime(
        result["trade_date"], errors="coerce"
    ).dt.date
    result["trade_date"] = result["trade_date"].fillna(expected_date)

    for col in ("expiry", "actual_expiry"):
        result[col] = pd.to_datetime(result[col], errors="coerce").dt.date

    for col in NUMERIC_COLUMNS:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    for col in ("volume", "trades", "oi", "change_oi"):
        result[col] = result[col].round().astype("Int64")

    for col in ("segment", "instrument_type", "symbol", "option_type", "contract_name"):
        result[col] = result[col].astype("string").str.strip()

    result["symbol"] = result["symbol"].str.upper()
    result["instrument_type"] = result["instrument_type"].str.upper()
    result["option_type"] = result["option_type"].str.upper()

    result = result[result["instrument_type"].isin(VALID_INSTRUMENTS)].copy()
    return result[DB_COLUMNS]


def _download_day(trading_date: date) -> tuple[pd.DataFrame | None, str | None]:
    date_key = trading_date.strftime("%Y%m%d")
    cache_path = RAW_DIR / f"{date_key}.zip"

    if cache_path.exists():
        payload = cache_path.read_bytes()
    else:
        url = ARCHIVE_URL.format(yyyymmdd=date_key)
        # Warm NSE homepage session; this is not a guarantee of access.
        try:
            SESSION.get("https://www.nseindia.com/", timeout=15)
        except requests.RequestException:
            pass

        response = SESSION.get(url, timeout=45)
        if response.status_code == 404:
            return None, "Archive not found (likely non-trading day)"
        response.raise_for_status()
        payload = response.content

        if not payload.startswith(b"PK"):
            sample = payload[:160].decode("utf-8", errors="replace")
            raise ValueError(
                "NSE returned a non-ZIP response; possible access restriction "
                f"or changed URL. Response sample: {sample!r}"
            )
        cache_path.write_bytes(payload)
        time.sleep(REQUEST_DELAY_SECONDS)

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError("No CSV found inside NSE ZIP")
        with archive.open(csv_names[0]) as handle:
            raw = pd.read_csv(handle)

    return _normalize(raw, trading_date), None


def _store_day(trading_date: date, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0

    con = _connect()
    try:
        con.register("incoming_frame", frame)
        con.execute("DELETE FROM fno_bhavcopy WHERE trade_date = ?", [trading_date])
        con.execute(f"INSERT INTO fno_bhavcopy ({','.join(DB_COLUMNS)}) SELECT {','.join(DB_COLUMNS)} FROM incoming_frame")
        return len(frame)
    finally:
        con.close()


def _frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    # JSON round trip converts pandas NA/Timestamp values into JSON-safe values.
    return frame.to_json(orient="records", date_format="iso") and __import__("json").loads(
        frame.to_json(orient="records", date_format="iso")
    )


@mcp.tool()
def sync_fno_history(start_date: str, end_date: str) -> dict:
    """Download and locally cache daily NSE F&O UDiFF bhavcopies.

    Use YYYY-MM-DD. Maximum window is controlled by MAX_SYNC_DAYS (default 31).
    This tool writes to the local database; call it for small date batches.
    """
    start, end = _parse_date(start_date), _parse_date(end_date)
    if end < start:
        return {"error": "end_date must not be before start_date"}
    if (end - start).days + 1 > MAX_SYNC_DAYS:
        return {"error": f"Maximum sync window is {MAX_SYNC_DAYS} calendar days"}

    loaded_days = 0
    rows_loaded = 0
    missing_days: list[str] = []
    failed_days: list[dict[str, str]] = []

    current = start
    while current <= end:
        try:
            frame, message = _download_day(current)
            if frame is None:
                missing_days.append(current.isoformat())
            else:
                rows_loaded += _store_day(current, frame)
                loaded_days += 1
        except Exception as exc:
            failed_days.append({"date": current.isoformat(), "error": str(exc)[:400]})
        current += timedelta(days=1)

    return {
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "loaded_days": loaded_days,
        "rows_loaded": rows_loaded,
        "missing_or_non_trading_days": missing_days,
        "failed_days": failed_days,
        "database_path": str(DB_PATH),
        "note": "Verify a sample against NSE. End-of-day bhavcopy only.",
    }


@mcp.tool()
def get_fno_history(
    start_date: str,
    end_date: str,
    symbol: str = "",
    instrument_type: str = "ALL",
    expiry: str = "",
    option_type: str = "",
    limit: int = 500,
) -> dict:
    """Query contract-wise daily F&O history, including OHLC, volume and OI.

    instrument_type: ALL, FUTSTK, FUTIDX, OPTSTK, OPTIDX.
    option_type: CE, PE, or blank for futures/both option sides.
    """
    start, end = _parse_date(start_date), _parse_date(end_date)
    if end < start:
        return {"error": "end_date must not be before start_date"}

    symbol_value = _validate_symbol(symbol)
    instrument = instrument_type.upper().strip()
    opt = option_type.upper().strip()
    if instrument not in VALID_INSTRUMENTS | {"ALL"}:
        return {"error": "Invalid instrument_type"}
    if opt not in {"", "CE", "PE"}:
        return {"error": "option_type must be CE, PE or blank"}
    if expiry:
        expiry_value = _parse_date(expiry)
    else:
        expiry_value = None

    limit = max(1, min(int(limit), MAX_QUERY_ROWS))
    filters = ["trade_date BETWEEN ? AND ?"]
    params: list[Any] = [start, end]

    if symbol_value:
        filters.append("symbol = ?")
        params.append(symbol_value)
    if instrument != "ALL":
        filters.append("instrument_type = ?")
        params.append(instrument)
    if expiry_value:
        filters.append("expiry = ?")
        params.append(expiry_value)
    if opt:
        filters.append("option_type = ?")
        params.append(opt)

    where = " AND ".join(filters)
    con = _connect()
    try:
        total = con.execute(
            f"SELECT COUNT(*) FROM fno_bhavcopy WHERE {where}", params
        ).fetchone()[0]
        frame = con.execute(
            f"""SELECT * FROM fno_bhavcopy WHERE {where}
                ORDER BY trade_date, symbol, expiry, strike, option_type
                LIMIT ?""",
            params + [limit],
        ).df()
    finally:
        con.close()

    return {
        "total_matching_rows": total,
        "returned_rows": len(frame),
        "truncated": total > len(frame),
        "records": _frame_records(frame),
    }


@mcp.tool()
def list_fno_contracts(
    trade_date: str = "",
    symbol: str = "",
    instrument_type: str = "ALL",
) -> dict:
    """List symbols, instrument classes, expiries and option sides in local data."""
    symbol_value = _validate_symbol(symbol)
    instrument = instrument_type.upper().strip()
    if instrument not in VALID_INSTRUMENTS | {"ALL"}:
        return {"error": "Invalid instrument_type"}

    filters = ["1=1"]
    params: list[Any] = []
    if trade_date:
        filters.append("trade_date = ?")
        params.append(_parse_date(trade_date))
    if symbol_value:
        filters.append("symbol = ?")
        params.append(symbol_value)
    if instrument != "ALL":
        filters.append("instrument_type = ?")
        params.append(instrument)

    con = _connect()
    try:
        frame = con.execute(
            f"""SELECT symbol, instrument_type, expiry, option_type,
                       COUNT(*) AS rows
                FROM fno_bhavcopy
                WHERE {' AND '.join(filters)}
                GROUP BY ALL
                ORDER BY symbol, expiry, option_type
                LIMIT 2000""",
            params,
        ).df()
    finally:
        con.close()

    return {"groups_returned": len(frame), "contracts": _frame_records(frame)}


@mcp.tool()
def get_oi_movers(
    trade_date: str,
    symbol: str = "",
    instrument_type: str = "ALL",
    option_type: str = "",
    top_n: int = 30,
) -> dict:
    """Rank stored F&O contracts by daily change in open interest."""
    day = _parse_date(trade_date)
    symbol_value = _validate_symbol(symbol)
    instrument = instrument_type.upper().strip()
    opt = option_type.upper().strip()
    if instrument not in VALID_INSTRUMENTS | {"ALL"}:
        return {"error": "Invalid instrument_type"}
    if opt not in {"", "CE", "PE"}:
        return {"error": "option_type must be CE, PE or blank"}

    top_n = max(1, min(int(top_n), 100))
    filters = ["trade_date = ?"]
    params: list[Any] = [day]
    if symbol_value:
        filters.append("symbol = ?")
        params.append(symbol_value)
    if instrument != "ALL":
        filters.append("instrument_type = ?")
        params.append(instrument)
    if opt:
        filters.append("option_type = ?")
        params.append(opt)

    con = _connect()
    try:
        frame = con.execute(
            f"""SELECT trade_date, symbol, instrument_type, expiry, strike,
                       option_type, close, volume, oi, change_oi
                FROM fno_bhavcopy
                WHERE {' AND '.join(filters)}
                ORDER BY change_oi DESC NULLS LAST
                LIMIT ?""",
            params + [top_n],
        ).df()
    finally:
        con.close()

    return {"trade_date": day.isoformat(), "top_contracts": _frame_records(frame)}


@mcp.tool()
def get_database_status() -> dict:
    """Return local database coverage and row counts."""
    con = _connect()
    try:
        count, first_day, last_day = con.execute(
            "SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM fno_bhavcopy"
        ).fetchone()
        by_type = con.execute(
            "SELECT instrument_type, COUNT(*) FROM fno_bhavcopy GROUP BY 1 ORDER BY 1"
        ).fetchall()
    finally:
        con.close()
    return {
        "database": str(DB_PATH),
        "rows": count,
        "first_trade_date": str(first_day) if first_day else None,
        "last_trade_date": str(last_day) if last_day else None,
        "rows_by_instrument": {k: v for k, v in by_type},
        "data_is_end_of_day": True,
    }


if __name__ == "__main__":
    # Streamable HTTP MCP endpoint: /mcp
    mcp.run(transport="streamable-http", host="0.0.0.0", port=PORT)
