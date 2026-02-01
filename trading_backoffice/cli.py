import argparse
import logging
import os

from dotenv import load_dotenv
from supabase import create_client

from trading_backoffice.loader.net_position_loader import NetPositionSnapshotLoader
from trading_backoffice.loader.intraday_trade_loader import IntradayTradeLoader


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    )


def get_supabase_client():
    load_dotenv()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL or SUPABASE_KEY not set. Check .env file."
        )

    return create_client(url, key)


def main():
    setup_logging()

    parser = argparse.ArgumentParser(prog="backoffice")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # backoffice load ...
    load_parser = subparsers.add_parser("load", help="Load data into backoffice")
    load_sub = load_parser.add_subparsers(dest="target", required=True)

    # backoffice load net <file>
    net_parser = load_sub.add_parser("net", help="Load net position snapshot")
    net_parser.add_argument("csv_path")

    # backoffice load intraday <file>
    intra_parser = load_sub.add_parser(
        "intraday", help="Load intraday trades"
    )
    intra_parser.add_argument("csv_path")

    args = parser.parse_args()

    supabase = get_supabase_client()

    if args.command == "load" and args.target == "net":
        loader = NetPositionSnapshotLoader(
            supabase_client=supabase,
            config={"net_positions_table": "net_positions"},
        )
        loader.load(args.csv_path)

    elif args.command == "load" and args.target == "intraday":
        loader = IntradayTradeLoader(
            supabase_client=supabase,
            config={"intraday_trades_table": "intraday_trades"},
        )
        loader.load(args.csv_path)


if __name__ == "__main__":
    main()

