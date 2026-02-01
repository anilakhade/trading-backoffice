import logging
import os
import sys

from dotenv import load_dotenv
from supabase import create_client

from trading_backoffice.loader.intraday_trade_loader import IntradayTradeLoader
from trading_backoffice.loader.net_position_loader import NetPositionSnapshotLoader


def create_supabase_client():
    load_dotenv()

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        raise RuntimeError(
            "SUPABASE_URL or SUPABASE_KEY not set. "
            "Check your .env file."
        )

    return create_client(supabase_url, supabase_key)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    )

    if len(sys.argv) != 3:
        print(
            "Usage:\n"
            "  python run.py load_net <csv_path>\n"
            "  python run.py load_intraday <csv_path>"
        )
        sys.exit(1)

    command = sys.argv[1]
    csv_path = sys.argv[2]

    supabase = create_supabase_client()

    if command == "load_net":
        loader = NetPositionSnapshotLoader(
            supabase_client=supabase,
            config={"net_positions_table": "net_positions"},
        )
        loader.load(csv_path)

    elif command == "load_intraday":
        loader = IntradayTradeLoader(
            supabase_client=supabase,
            config={"intraday_trades_table": "intraday_trades"},
        )
        loader.load(csv_path)

    else:
        raise ValueError(
            f"Unknown command '{command}'. "
            "Use 'load_net' or 'load_intraday'."
        )


if __name__ == "__main__":
    main()

