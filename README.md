trading-backoffice

Backend system for aggregating multi-broker trading positions.
Handles intraday positions, net positions, booked P&L, and risk data.
Uses Supabase as the single source of truth for all state.
Data ingestion, cleaning, and accounting are done via Python and SQL.
Primary interface is the terminal; outputs are generated as CSV/Excel.
UI and dashboards are out of scope for now.

