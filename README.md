

# trading-backoffice

Backend system for aggregating multi-broker trading positions.

Handles intraday positions, net positions, booked P&L, and risk data.  
Uses Supabase as the single source of truth for all state.  
Data ingestion, cleaning, and accounting are done via Python and SQL.  
Primary interface is the terminal; outputs are generated as CSV / Excel.  
UI and dashboards are out of scope for now.

---

## Design Philosophy

- Correctness over convenience
- Abort-on-error, no partial writes
- Explicit validation at ingestion boundaries
- Clear separation of responsibilities
- Predictable and repeatable terminal commands

This system is designed for real trading environments where silent data corruption is unacceptable.

---

## Core Concepts

### Net Positions
- Represent end-of-day or day-zero portfolio state
- Loaded as snapshots
- Idempotent upserts using a composite business key
- Used as the base state for accounting and simulations

### Intraday Trades
- Represent raw execution data
- Append-only trade ledger
- NULL values are preserved intentionally
- Cleared and processed downstream by accounting logic

---

## Project Structure

```

trading-backoffice/
├── trading_backoffice/        # Python package
│   ├── cli.py                 # CLI entry point
│   └── loader/                # Net & intraday loaders
├── data/                      # CSV inputs (gitignored)
├── sql/                       # Database schema and engine logic
├── run.py                     # Development harness
├── pyproject.toml             # Packaging and CLI definition
├── requirements.txt
└── README.md

````

- All core logic lives in `trading_backoffice/`
- CLI is intentionally thin
- `run.py` is for development and testing only

---

## Prerequisites

- Git
- Python 3.10 or higher
- Supabase project with required tables
- Internet access

Check Python version:
```bash
python --version
````

---

## Installation (One Time per Machine)

### 1. Clone the Repository

```bash
git clone https://github.com/anilakhade/trading-backoffice.git
cd trading-backoffice
```

---

### 2. Create and Activate Virtual Environment

#### Linux / macOS

```bash
python -m venv .venv
source .venv/bin/activate
```

#### Windows (PowerShell)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

---

### 3. Configure Supabase Credentials

Create a `.env` file in the project root:

```
SUPABASE_URL=https://<your-project>.supabase.co
SUPABASE_KEY=<your-secret-or-service-role-key>
```

Notes:

* `.env` is not committed to Git
* Each machine maintains its own credentials

---

### 4. Install Backoffice CLI

```bash
pip install -e .
```

Verify installation:

```bash
backoffice --help
```

---

## Daily Usage (Operations)

### Load Net Position Snapshot

Used for day-zero initialization or full resets.

```bash
backoffice load net data/net_pos/net_pos.csv
```

Behavior:

* Full structural and logical validation
* Symbol and instrument normalization
* Idempotent upsert
* Safe to re-run

---

### Load Intraday Trades

Used for daily execution ingestion.

```bash
backoffice load intraday data/intraday/intraday_pos.csv
```

Behavior:

* Strict validation
* Preserves NULLs and raw execution data
* Append-only ledger semantics
* Entire file is rejected on any error

---

## Development vs Operations

### Development / Debugging

```bash
python run.py
```

Used for:

* Rapid iteration
* Debugging loaders
* Testing new logic

---

### Operations / Office Usage

```bash
backoffice ...
```

Used for:

* Daily ingestion
* Predictable, repeatable execution
* No code modification required

---

## Updating Code on Any Machine

When new changes are pushed:

```bash
git pull
pip install -e .
```

Commands remain unchanged.

---

## Operational Rules

* Never commit `.env`
* Do not modify loader logic on office machines
* Validate CSVs before ingestion
* If a command fails, no data is written
* Accounting and reconciliation are handled downstream in SQL

---

## Status

* Net position loader: Stable
* Intraday trade loader: Stable
* CLI interface: Production-ready

This repository forms the foundation for accounting, risk, and reporting layers.

```


