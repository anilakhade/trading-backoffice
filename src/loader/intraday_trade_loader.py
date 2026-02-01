import logging
import re
from datetime import datetime
from typing import Dict, List

import pandas as pd


class IntradayTradeLoadError(Exception):
    pass


class IntradayTradeLoader:
    """
    Intraday execution loader.

    Design:
    - INSERT ONLY
    - Immutable execution ledger
    - Accepts raw broker data
    """

    DATE_REGEX = re.compile(r"^\d{2}-[A-Za-z]{3}-\d{4}$")

    ALLOWED_EXCHANGES = {"NSE", "BSE"}
    EQ_ALIASES = {"EQ", "EQUITY", "CASH"}

    ALLOWED_INSTRUMENTS = {
        "EQ",
        "FUT",
        "FUTIDX",
        "FUTSTK",
        "OPT",
        "OPTIDX",
        "OPTSTK",
    }

    BSE_SYMBOL_MAP = {
        "SENSEX": {"BSX", "BSE", "BSXOPT", "SENSEX"},
        "BANKEX": {"BKX", "BKXOPT", "BANKEX"},
    }

    CSV_TO_DB_COLS = {
        "Broker_Id": "broker_id",
        "Sheet": "sheet",
        "Strategy": "strategy",
        "Exchange": "exchange",
        "Instrument": "instrument_type",
        "Symbol": "symbol",
        "Expiry": "expiry",
        "Strike": "strike",
        "Opt_Type": "opt_type",
        "Buy_Qty": "buy_qty",
        "Buy_Rate": "buy_rate",
        "Sell_Qty": "sell_qty",
        "Sell_Rate": "sell_rate",
        "Net_Qty": "net_qty",
        "Trade_Date": "trade_date",
    }

    REQUIRED_COLUMNS = set(CSV_TO_DB_COLS.keys())
    NULL_STRINGS = {"", "nan", "none", "null"}

    def __init__(self, supabase_client, config: Dict):
        self.supabase = supabase_client
        self.table = config["intraday_trades_table"]
        self.logger = logging.getLogger(
            "trading_backoffice.intraday_trade_loader"
        )

    # =====================================================
    # PUBLIC
    # =====================================================

    def load(self, csv_path: str) -> None:
        self.logger.info(f"File received: {csv_path}")

        df = self._read_csv(csv_path)
        self._validate_required_columns(df)
        self._basic_normalization(df)

        trade_date = self._validate_trade_date(df)
        self._validate_exchange(df)

        self._canonicalize_bse_symbols(df)
        self._canonicalize_equity_instruments(df)
        self._validate_expiry_strike_opt_type(df)
        self._validate_quantities_and_rates(df)

        self.logger.info(
            f"Inserting {len(df)} intraday rows for Trade_Date={trade_date}"
        )

        self._insert_to_db(self._to_db_records(df))

        self.logger.info(
            f"Intraday trades loaded successfully for Trade_Date={trade_date}"
        )

    # =====================================================
    # CSV
    # =====================================================

    def _read_csv(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path, dtype=str)

    def _validate_required_columns(self, df: pd.DataFrame) -> None:
        missing = self.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise IntradayTradeLoadError(
                f"Missing required columns: {sorted(missing)}"
            )

    def _basic_normalization(self, df: pd.DataFrame) -> None:
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()

        for col in [
            "Broker_Id", "Sheet", "Strategy", "Exchange",
            "Instrument", "Symbol", "Opt_Type"
        ]:
            df[col] = df[col].str.upper()

    # =====================================================
    # DATE / EXCHANGE
    # =====================================================

    def _validate_trade_date(self, df: pd.DataFrame) -> str:
        dates = df["Trade_Date"].unique()
        if len(dates) != 1:
            raise IntradayTradeLoadError("Trade_Date must be unique per file.")
        self._parse_date(dates[0], "Trade_Date")
        return dates[0]

    def _validate_exchange(self, df: pd.DataFrame) -> None:
        bad = set(df["Exchange"]) - self.ALLOWED_EXCHANGES
        if bad:
            raise IntradayTradeLoadError(f"Invalid exchange(s): {bad}")

    def _parse_date(self, value: str, col: str) -> None:
        if not self.DATE_REGEX.match(value):
            raise IntradayTradeLoadError(
                f"{col}: invalid date '{value}', expected DD-MMM-YYYY"
            )
        datetime.strptime(value.upper(), "%d-%b-%Y")

    # =====================================================
    # CANONICALIZATION
    # =====================================================

    def _canonicalize_bse_symbols(self, df: pd.DataFrame) -> None:
        for canonical, aliases in self.BSE_SYMBOL_MAP.items():
            mask = (df["Exchange"] == "BSE") & df["Symbol"].isin(aliases)
            df.loc[mask, "Symbol"] = canonical

            opt = mask & df["Instrument"].isin({"IO", "OPT", "OPTIDX"})
            fut = mask & df["Instrument"].isin({"FUT", "FUTIDX"})

            df.loc[opt, "Instrument"] = "OPTIDX"
            df.loc[fut, "Instrument"] = "FUTIDX"

    def _canonicalize_equity_instruments(self, df: pd.DataFrame) -> None:
        eq = df["Instrument"].isin(self.EQ_ALIASES)
        df.loc[eq, "Instrument"] = "EQ"
        df.loc[eq, ["Expiry", "Strike", "Opt_Type"]] = None

    # =====================================================
    # CONTRACT VALIDATION
    # =====================================================

    def _validate_expiry_strike_opt_type(self, df: pd.DataFrame) -> None:
        for i, r in df.iterrows():
            inst = r["Instrument"]

            if inst == "EQ":
                continue

            if inst.startswith("FUT"):
                if not r["Expiry"]:
                    raise IntradayTradeLoadError(f"Row {i+1}: FUT needs expiry")
                continue

            if inst.startswith("OPT"):
                if not (r["Expiry"] and r["Strike"] and r["Opt_Type"]):
                    raise IntradayTradeLoadError(
                        f"Row {i+1}: OPT needs expiry/strike/opt_type"
                    )

    # =====================================================
    # QUANTITIES / RATES (RELAXED & CORRECT)
    # =====================================================

    def _validate_quantities_and_rates(self, df: pd.DataFrame) -> None:
        for i, r in df.iterrows():

            def to_int(v):
                if pd.isna(v) or str(v).lower() in self.NULL_STRINGS:
                    return 0
                return int(float(v))

            def to_float(v):
                if pd.isna(v) or str(v).lower() in self.NULL_STRINGS:
                    return None
                return float(v)

            buy = to_int(r["Buy_Qty"])
            sell = to_int(r["Sell_Qty"])
            net = to_int(r["Net_Qty"])

            if buy == 0 and sell == 0:
                raise IntradayTradeLoadError(
                    f"Row {i+1}: both Buy_Qty and Sell_Qty are zero"
                )

            if net != buy - sell:
                raise IntradayTradeLoadError(
                    f"Row {i+1}: Net_Qty mismatch"
                )

            br = to_float(r["Buy_Rate"])
            sr = to_float(r["Sell_Rate"])

            if br is not None and br < 0:
                raise IntradayTradeLoadError(f"Row {i+1}: Buy_Rate < 0")

            if sr is not None and sr < 0:
                raise IntradayTradeLoadError(f"Row {i+1}: Sell_Rate < 0")

    # =====================================================
    # DB
    # =====================================================

    def _to_db_records(self, df: pd.DataFrame) -> List[dict]:
        db_df = df.rename(columns=self.CSV_TO_DB_COLS)

        records = []
        for rec in db_df.to_dict("records"):
            clean = {}
            for k, v in rec.items():
                if pd.isna(v):
                    clean[k] = None
                else:
                    clean[k] = v
            records.append(clean)

        return records



    def _insert_to_db(self, records: List[dict]) -> None:
        self.supabase.table(self.table).insert(records).execute()

