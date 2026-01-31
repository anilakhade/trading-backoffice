import logging
import re
from datetime import datetime
from typing import Dict, List

import pandas as pd


class NetPositionLoadError(Exception):
    pass


class NetPositionSnapshotLoader:
    """
    Day-0 / snapshot net position loader.

    Strict rules:
    - Binary behavior: entire file passes or fails
    - Snapshot semantics (one row per instrument after merge)
    - DB upsert using business key
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
        "Net_Qty": "net_qty",
        "Avg_Price": "avg_price",
        "Carry_Date": "carry_date",
    }

    REQUIRED_COLUMNS = set(CSV_TO_DB_COLS.keys())

    def __init__(self, supabase_client, config: Dict):
        self.supabase = supabase_client
        self.table = config["net_positions_table"]
        self.logger = logging.getLogger(
            "trading_backoffice.net_position_loader"
        )

    # =====================================================
    # PUBLIC API
    # =====================================================

    def load(self, csv_path: str) -> None:
        self.logger.info(f"File received: {csv_path}")

        df = self._read_csv(csv_path)
        self._validate_required_columns(df)

        self.logger.info("Validating file structure and formats...")
        self._basic_normalization(df)

        carry_date = self._validate_carry_date(df)
        self._validate_exchange(df)
        self._validate_expiry_format(df)

        self.logger.info("File passed structural validation.")
        self.logger.info("Normalizing symbols and instruments...")

        self._canonicalize_bse_symbols(df)
        self._canonicalize_equity_instruments(df)

        self.logger.info("Validating numeric fields...")
        self._validate_numeric_fields(df)

        self.logger.info("Resolving duplicate positions using snapshot rules...")
        df = self._merge_duplicates(df)

        self.logger.info("Final validation against position invariants...")
        self._final_shape_validation(df)

        self.logger.info("Uploading snapshot to database (transaction started)...")
        records = self._to_db_records(df)
        self._upsert_to_db(records)

        self.logger.info(
            f"Net position snapshot loaded successfully for carry_date={carry_date}"
        )

    # =====================================================
    # CSV / STRUCTURE
    # =====================================================

    def _read_csv(self, path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(path, dtype=str)
        except Exception as exc:
            raise NetPositionLoadError(f"Failed to read CSV: {exc}") from exc

    def _validate_required_columns(self, df: pd.DataFrame) -> None:
        missing = self.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise NetPositionLoadError(
                f"Missing required column(s): {sorted(missing)}"
            )

    def _basic_normalization(self, df: pd.DataFrame) -> None:
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()

        for col in [
            "Broker_Id",
            "Sheet",
            "Strategy",
            "Exchange",
            "Instrument",
            "Symbol",
            "Opt_Type",
        ]:
            df[col] = df[col].str.upper()

    # =====================================================
    # DATE / EXCHANGE VALIDATION
    # =====================================================

    def _validate_carry_date(self, df: pd.DataFrame) -> str:
        vals = df["Carry_Date"].unique()
        if len(vals) != 1:
            raise NetPositionLoadError(
                "Carry_Date must be single-valued for entire file."
            )

        date_str = vals[0]
        self._parse_date(date_str, "Carry_Date")
        return date_str

    def _validate_exchange(self, df: pd.DataFrame) -> None:
        bad = set(df["Exchange"]) - self.ALLOWED_EXCHANGES
        if bad:
            raise NetPositionLoadError(
                f"Invalid exchange(s): {bad}. Allowed: {self.ALLOWED_EXCHANGES}"
            )

    def _validate_expiry_format(self, df: pd.DataFrame) -> None:
        for idx, row in df.iterrows():
            inst = row["Instrument"]
            expiry = row["Expiry"]

            if inst in self.EQ_ALIASES:
                if expiry and expiry != "nan":
                    raise NetPositionLoadError(
                        f"Row {idx+1}: EQ must not have expiry."
                    )
                continue

            if not expiry or expiry == "nan":
                raise NetPositionLoadError(
                    f"Row {idx+1}: Missing expiry."
                )

            self._parse_date(expiry, "Expiry", idx)

    def _parse_date(
        self, value: str, col: str, idx: int | None = None
    ) -> None:
        if not self.DATE_REGEX.match(value):
            prefix = f"Row {idx+1} | " if idx is not None else ""
            raise NetPositionLoadError(
                f"{prefix}{col}: invalid date '{value}'. Expected DD-MMM-YYYY."
            )
        datetime.strptime(value.upper(), "%d-%b-%Y")

    # =====================================================
    # CANONICALIZATION
    # =====================================================

    def _canonicalize_bse_symbols(self, df: pd.DataFrame) -> None:
        """
        BSE-only canonicalization.

        Symbol:
        - BSX/BSE/BSXOPT -> SENSEX
        - BKX/BKXOPT    -> BANKEX

        Instrument:
        - IO / OPT / OPTIDX  -> OPTIDX
        - FUT / FUTIDX      -> FUTIDX
        """

        for canonical, aliases in self.BSE_SYMBOL_MAP.items():
            mask = (df["Exchange"] == "BSE") & df["Symbol"].isin(aliases)

            if not mask.any():
                continue

            df.loc[mask, "Symbol"] = canonical

            opt_mask = mask & df["Instrument"].isin({"IO", "OPT", "OPTIDX"})
            fut_mask = mask & df["Instrument"].isin({"FUT", "FUTIDX"})

            df.loc[opt_mask, "Instrument"] = "OPTIDX"
            df.loc[fut_mask, "Instrument"] = "FUTIDX"

            bad_mask = mask & ~df["Instrument"].isin(
                {"IO", "OPT", "OPTIDX", "FUT", "FUTIDX"}
            )

            if bad_mask.any():
                bad_vals = df.loc[bad_mask, "Instrument"].unique()
                raise NetPositionLoadError(
                    f"BSE index instruments must be OPTIDX or FUTIDX. Found: {bad_vals}"
                )

    def _canonicalize_equity_instruments(self, df: pd.DataFrame) -> None:
        eq_mask = df["Instrument"].isin(self.EQ_ALIASES)
        df.loc[eq_mask, "Instrument"] = "EQ"
        df.loc[eq_mask, "Sheet"] = "PORTFOLIO"
        df.loc[eq_mask, ["Expiry", "Strike", "Opt_Type"]] = None

        bad = ~df["Instrument"].isin(self.ALLOWED_INSTRUMENTS)
        if bad.any():
            raise NetPositionLoadError(
                f"Unknown instrument(s): {df.loc[bad, 'Instrument'].unique()}"
            )

    # =====================================================
    # NUMERIC VALIDATION
    # =====================================================

    def _validate_numeric_fields(self, df: pd.DataFrame) -> None:
        for idx, row in df.iterrows():
            try:
                qty = int(row["Net_Qty"])
            except Exception:
                raise NetPositionLoadError(
                    f"Row {idx+1}: Net_Qty must be integer."
                )

            price = float(row["Avg_Price"])
            if price < 0 or round(price, 3) != price:
                raise NetPositionLoadError(
                    f"Row {idx+1}: Avg_Price must be >=0 with 3 decimals."
                )

            strike = row["Strike"]
            if strike and strike != "nan":
                val = float(strike)
                if round(val, 3) != val:
                    raise NetPositionLoadError(
                        f"Row {idx+1}: Strike must be int or <=3 decimals."
                    )

    # =====================================================
    # DUPLICATE SNAPSHOT MERGE
    # =====================================================

    def _merge_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        keys = [
            "Broker_Id",
            "Sheet",
            "Strategy",
            "Exchange",
            "Instrument",
            "Symbol",
            "Expiry",
            "Strike",
            "Opt_Type",
        ]

        out: List[dict] = []
        merged = 0

        for _, g in df.groupby(keys, dropna=False):
            if len(g) == 1:
                out.append(g.iloc[0].to_dict())
                continue

            qty = g["Net_Qty"].astype(int).sum()
            if qty == 0:
                out.extend(g.to_dict("records"))
                continue

            vwap = (
                (g["Net_Qty"].astype(int) * g["Avg_Price"].astype(float)).sum()
                / qty
            )

            row = g.iloc[0].to_dict()
            row["Net_Qty"] = qty
            row["Avg_Price"] = round(vwap, 3)
            out.append(row)
            merged += 1

        if merged:
            self.logger.info(
                f"Merged {merged} duplicate snapshot groups using VWAP."
            )
        else:
            self.logger.info("No duplicate positions detected.")

        return pd.DataFrame(out)

    # =====================================================
    # FINAL INVARIANTS
    # =====================================================

    def _final_shape_validation(self, df: pd.DataFrame) -> None:
        for idx, row in df.iterrows():
            inst = row["Instrument"]

            if inst == "EQ":
                if any([row["Expiry"], row["Strike"], row["Opt_Type"]]):
                    raise NetPositionLoadError(
                        f"Row {idx+1}: EQ must not have expiry/strike/opt_type."
                    )

            if inst in {"FUT", "FUTIDX", "FUTSTK"}:
                if not row["Expiry"]:
                    raise NetPositionLoadError(
                        f"Row {idx+1}: FUT requires expiry."
                    )

            if inst in {"OPT", "OPTIDX", "OPTSTK"}:
                if not (row["Expiry"] and row["Strike"] and row["Opt_Type"]):
                    raise NetPositionLoadError(
                        f"Row {idx+1}: OPT requires expiry, strike, opt_type."
                    )

    # =====================================================
    # DB
    # =====================================================

    def _to_db_records(self, df: pd.DataFrame) -> List[dict]:
        db_df = df.rename(columns=self.CSV_TO_DB_COLS)
        return db_df.to_dict(orient="records")

    def _upsert_to_db(self, records: List[dict]) -> None:
        resp = (
            self.supabase
            .table(self.table)
            .upsert(
                records,
                on_conflict=(
                    "broker_id,strategy,sheet,exchange,"
                    "instrument_type,symbol,expiry,strike,opt_type,carry_date"
                ),
            )
            .execute()
        )

        if getattr(resp, "error", None):
            raise NetPositionLoadError(resp.error)

