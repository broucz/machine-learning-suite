"""ETL process for Clickhouse events."""

import argparse
import os
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict

from shared.utils import ClickHouseDbClient, date, db_ops, log, validators

# Module Config
logger = log.get_logger()
NAMESPACE = "smart_bidding"
DB_DIR = os.path.join(os.getcwd(), ".db", NAMESPACE)
RAW_DATASET_DIR = os.path.join(DB_DIR, "raw_dataset")
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")

os.makedirs(DB_DIR, exist_ok=True)
os.makedirs(RAW_DATASET_DIR, exist_ok=True)


def submit_futures(
    executor: ThreadPoolExecutor, query: str, args: argparse.Namespace
) -> Dict[Future, Dict[str, datetime]]:
    """
    Submit tasks to the ThreadPoolExecutor.

    Args:
        executor: The ThreadPoolExecutor object.
        query: The SQL query string.
        args: Command line arguments.

    Returns:
        Dictionary of futures along with associated metadata.
    """

    futures = {}
    hour_intervals = date.get_hour_intervals(args.start_date, args.end_date)
    for start_time, end_time in hour_intervals:
        destination_folder = os.path.join(RAW_DATASET_DIR, f"{start_time.strftime('%Y-%m-%d_%H')}")
        if not os.path.exists(destination_folder):
            params = {
                "start_time": start_time,
                "end_time": end_time,
                "down_sampling_percentage": args.down_sampling_percentage,
            }
            client = ClickHouseDbClient().get_client()
            future = executor.submit(db_ops.execute_query_to_dask_df, client, query, params)
            futures[future] = {"start_time": start_time}
    return futures


def process_future_results(futures: Dict[Future, Dict[str, datetime]]) -> None:
    """
    Process the results of completed futures.

    Args:
        futures: Dictionary of futures along with associated metadata.
    """

    for future in as_completed(futures):
        metadata = futures[future]
        try:
            dask_df = future.result()
            destination_folder = os.path.join(RAW_DATASET_DIR, f"{metadata['start_time'].strftime('%Y-%m-%d_%H')}")
            try:
                dask_df.to_parquet(destination_folder)
            except Exception as e:
                raise db_ops.DiskWriteError(f"Failed to write to disk: {e}")
        except Exception as e:
            raise db_ops.QueryExecutionError(f"An exception occurred for {metadata['start_time']}: {e}")


def main(args: argparse.Namespace) -> None:
    """
    Main function to orchestrate the ETL process.

    Args:
        args: Command line arguments.
    """

    logger.info("Extracting events")
    query = db_ops.read_query_from_file(os.path.join(SQL_DIR, "extract_events.sql"))
    execution_start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = submit_futures(executor, query, args)
        process_future_results(futures)

    execution_time = time.time() - execution_start_time
    logger.info(f"Execution time: {round(execution_time, 3)} seconds")


# CLI


DEFAULT_DATE_RANGE = 7
DEFAULT_START_DATE, DEFAULT_END_DATE = date.get_date_range_for_past_days(days=DEFAULT_DATE_RANGE)
DEFAULT_DOWN_SAMPLING = 0.01
DEFAULT_MAX_WORKERS = 8


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Extract events.")

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
