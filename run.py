import logging
import os

from dotenv import load_dotenv
from supabase import create_client

from src.loader.net_position_loader import NetPositionSnapshotLoader


def main() -> None:
    # Load environment variables from .env
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    )

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        raise RuntimeError(
            "SUPABASE_URL or SUPABASE_KEY not set. "
            "Check your .env file."
        )

    supabase = create_client(supabase_url, supabase_key)

    loader = NetPositionSnapshotLoader(
        supabase_client=supabase,
        config={
            "net_positions_table": "net_positions",
        },
    )

    loader.load("data/net_pos/net_pos.csv")


if __name__ == "__main__":
    main()

