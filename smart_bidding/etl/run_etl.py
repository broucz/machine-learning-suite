"""ETL process for Clickhouse events."""

import os
import time
from argparse import Namespace, ArgumentParser
from shared.utils import ClickHouseDbClient, date, db_ops, log, validators
from shared.utils.storage_ops import LocalStorage, S3Storage
from .dictionary import Dictionary
from .processor import Processor

# Module Config
logger = log.get_logger()
NAMESPACE = "smart_bidding"
DB_DIR = os.path.join(os.getcwd(), ".db", NAMESPACE)
LOCAL_DATASET_DIR = os.path.join(DB_DIR, "dataset")
REMOTE_DATASET_DIR = "my-bucket-name"
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
COLLECTIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collections")


def main(args: Namespace) -> None:
    """Main function to orchestrate the ETL process.

    Args:
        args: Command line arguments.
    """

    logger.info("Extracting events")

    dictionary = Dictionary(COLLECTIONS_DIR)
    storage = LocalStorage(LOCAL_DATASET_DIR) if args.storage_type == "local" else S3Storage(REMOTE_DATASET_DIR)
    query = db_ops.read_query_from_file(os.path.join(SQL_DIR, "extract_events.sql"))

    execution_start_time = time.time()

    etl_processor = Processor(args, logger, ClickHouseDbClient, dictionary, storage, query)
    etl_processor.run()

    execution_time = time.time() - execution_start_time
    logger.info(f"Execution time: {round(execution_time, 3)} seconds")


# CLI


DEFAULT_DATE_RANGE = 7
DEFAULT_START_DATE, DEFAULT_END_DATE = date.get_date_range_for_past_days(days=DEFAULT_DATE_RANGE)
DEFAULT_DOWN_SAMPLING = 0.01
DEFAULT_MAX_WORKERS = 8


def parse_arguments() -> Namespace:
    """Parse command-line arguments."""
    parser = ArgumentParser(description="Extract events.")

    parser.add_argument(
        "--start_date",
        type=validators.validate_datetime_arg,
        default=DEFAULT_START_DATE,
        help=(
            f"Start date for fetching dataset. Defaults to {DEFAULT_DATE_RANGE} days ago at 00:00:00."
            "Format: 'YYYY-MM-DD HH:MM:SS'"
        ),
    )

    parser.add_argument(
        "--end_date",
        type=validators.validate_datetime_arg,
        default=DEFAULT_END_DATE,
        help="End date for fetching dataset. Defaults to yesterday at 23:59:59. Format: 'YYYY-MM-DD HH:MM:SS'",
    )

    parser.add_argument(
        "--down_sampling_percentage",
        type=validators.validate_percentage_arg,
        default=DEFAULT_DOWN_SAMPLING,
        help=f"Data sampling percentage (0-1). Defaults to {DEFAULT_DOWN_SAMPLING}.",
    )

    parser.add_argument(
        "--max_workers",
        type=validators.validate_positive_integer_arg,
        default=DEFAULT_MAX_WORKERS,
        help=f"Max number of worker threads. Defaults to {DEFAULT_MAX_WORKERS}.",
    )

    parser.add_argument(
        "--storage_type",
        choices=["local", "remote"],
        default="remote",
        help="Type of storage to use. Options are 'local' and 'remote'. Default is 'remote'.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
