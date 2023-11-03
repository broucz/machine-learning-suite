from argparse import Namespace
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime
from logging import Logger
from typing import Dict

from shared.utils import date, db_ops, etl_ops, storage_ops
from shared.utils.db_ops import AbstractDbClient
from shared.utils.etl_ops import AbstractDictionary, AbstractProcessor
from shared.utils.storage_ops import AbstractStorage

from .transform import Transformer


class Processor(AbstractProcessor):
    """Encapsulates the ETL process for extracting, transforming,
    and loading data using a multi-threaded approach.
    """

    def __init__(
        self,
        args: Namespace,
        logger: Logger,
        db_client: AbstractDbClient,
        dictionary: AbstractDictionary,
        storage: AbstractStorage,
        query: str,
    ):
        super().__init__(args, logger)

        self.dictionary = dictionary
        self.storage = storage
        self.query = query

        self.db_client_pool = db_client()
        self.transformer = Transformer(self.dictionary, self.logger)
        self.hour_intervals = date.get_hour_intervals(self.args.start_date, self.args.end_date)
        self.down_sampling_percentage = args.down_sampling_percentage

    def _submit_futures(self, executor: ThreadPoolExecutor) -> Dict[Future, Dict[str, datetime]]:
        """Submit tasks to the ThreadPoolExecutor.

        Args:
            executor: The ThreadPoolExecutor object.

        Returns:
            Dictionary of futures along with associated metadata.
        """

        futures = {}
        for interval_start_time, interval_end_time in self.hour_intervals:
            params = {
                "start_time": interval_start_time,
                "end_time": interval_end_time,
                "down_sampling_percentage": self.down_sampling_percentage,
            }
            client = self.db_client_pool.get_client()
            future = executor.submit(db_ops.execute_query_to_dask_df, client, self.query, params)
            futures[future] = {"interval_start_time": interval_start_time}
        return futures

    def _process_future_results(self, futures: Dict[Future, Dict[str, datetime]]) -> None:
        """Process the results of completed futures.

        Args:
            futures: Dictionary of futures along with associated metadata.
        """

        for future in as_completed(futures):
            metadata = futures[future]
            try:
                df = future.result()
            except Exception as e:
                raise db_ops.QueryExecutionError(f"An exception occurred for {metadata['interval_start_time']}: {e}")

            try:
                transformed_df = self.transformer.transform_dask_df(df)
            except Exception as e:
                raise etl_ops.TransformError(f"Failed to transform dataset: {e}")

            try:
                file_path = f"{metadata['interval_start_time'].strftime('%Y-%m-%d_%H')}"
                self.storage.write_dask_df(transformed_df, file_path)
            except Exception as e:
                raise storage_ops.StorageWriteError(f"Failed to write: {e}")

    def run(self):
        """Executes the ETL process."""
        self.logger.info("Starting ETL Process.")

        with ThreadPoolExecutor(max_workers=self.args.max_workers) as executor:
            futures = self._submit_futures(executor)
            self._process_future_results(futures)

        self.logger.info("ETL Process completed successfully.")
