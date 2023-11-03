import os
from abc import ABC, abstractmethod
from typing import List

import dask.dataframe as dd


class AbstractStorage(ABC):
    @abstractmethod
    def read_dask_df(self, file_paths: List[str]) -> dd.DataFrame:
        """Read data from multiple file paths into a Dask DataFrame."""

        pass

    @abstractmethod
    def write_dask_df(self, df: dd.DataFrame, file_path: str) -> None:
        """Write a Dask DataFrame to a given file path."""

        pass


class StorageWriteError(Exception):
    """Custom exception to indicate errors during write operations.

    Raised when an attempt to write data fails, allowing for
    more specific error handling related to storage operations.
    """

    pass


class LocalStorage(AbstractStorage):
    def __init__(self, root_dir: str):
        """
        Initialize the LocalStorage with a given root directory.

        Args:
            root_dir (str): The path to the root directory to use.
        """

        self.root_dir = root_dir
        # Ensure root directory exists
        os.makedirs(self.root_dir, exist_ok=True)

    def _get_full_path(self, file_path: str) -> str:
        """Construct the full path for a given file path."""

        return os.path.join(self.root_dir, file_path)

    def read_dask_df(self, file_paths: List[str]) -> dd.DataFrame:
        """Read local parquet files into a Dask DataFrame."""

        full_paths = [self._get_full_path(path) for path in file_paths]
        return dd.concat([dd.read_parquet(path) for path in full_paths])

    def write_dask_df(self, df: dd.DataFrame, file_path: str) -> None:
        """Write a Dask DataFrame to a local parquet file."""

        full_path = self._get_full_path(file_path)
        df.to_parquet(full_path)


class S3Storage(AbstractStorage):
    def __init__(self, bucket_name: str):
        """
        Initialize the S3Storage with a given bucket name.

        Args:
            bucket_name (str): Name of the S3 bucket to use.
        """

        self.bucket_name = bucket_name

        raise NotImplementedError("This feature has not been implemented yet.")

    def read_dask_df(self, file_paths: List[str]) -> dd.DataFrame:
        """Read parquet files from S3 into a Dask DataFrame."""

        full_paths = [f"s3://{self.bucket_name}/{path}" for path in file_paths]
        return dd.concat([dd.read_parquet(path) for path in full_paths])

    def write_dask_df(self, df: dd.DataFrame, file_path: str) -> None:
        """Write a Dask DataFrame to a parquet file in S3."""

        full_path = f"s3://{self.bucket_name}/{file_path}"
        df.to_parquet(full_path)
