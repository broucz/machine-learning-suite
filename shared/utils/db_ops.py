from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Optional, Protocol, Union

import dask.dataframe as dd
import pandas as pd
from pandas import DataFrame


class SupportsQueryDFStream(Protocol):
    """Protocol that defines the contract for a database client supporting a query_df_stream method."""

    def query_df_stream(self, query: str) -> Iterable[DataFrame]:
        """Execute a query and return an iterable of DataFrames.

        Args:
            query: SQL query as a string.

        Returns:
            An iterable containing DataFrames.
        """

        pass


class DbClient(ABC):
    """Abstract Base Class for database client classes.

    This class defines the interface for database clients.
    """

    @abstractmethod
    def get_client(self, **kwargs: Any) -> SupportsQueryDFStream:
        """Retrieve a database client.

        Args:
            kwargs: Optional keyword arguments for client initialization.

        Returns:
            A database client object that supports query_df_stream method.
        """

        pass


class DiskWriteError(Exception):
    """Custom exception to indicate errors during disk write operations.

    Raised when an attempt to write data to disk fails, allowing for
    more specific error handling related to disk operations.
    """

    pass


class QueryExecutionError(Exception):
    """Custom exception to indicate errors during query execution.

    Raised when a SQL query fails to execute properly, allowing for
    more specific error handling related to database queries.
    """

    pass


def execute_query_to_dask_df(
    client: SupportsQueryDFStream,
    query: str,
    params: Optional[Dict[str, Union[str, int, float]]] = None,
    npartitions: int = 1,
) -> dd.DataFrame:
    """
    Execute a query and append the result to a Dask DataFrame.

    Args:
        client (SupportsQueryDFStream): A database client class implementing the SupportsQueryDFStream protocol.
        query (str): SQL query as a string.
        params (Dict[str, Union[str, int, float]], optional): Parameters to format the query.
        npartitions (int, optional): Number of partitions for the Dask DataFrame.

    Returns:
        dd.DataFrame: The DataFrame with the query results.

    Raises:
        QueryExecutionError: If an error occurs while executing the query.
    """

    if params:
        query = query.format(**params)

    try:
        df_stream = client.query_df_stream(query)
    except Exception as e:
        raise QueryExecutionError(f"Error executing query: {e}")

    df = dd.from_pandas(pd.DataFrame(), npartitions=1)

    with df_stream:
        first_chunk = True
        for _, df_chunk in enumerate(df_stream):
            dask_df_chunk = dd.from_pandas(df_chunk, npartitions=npartitions)
            if first_chunk:
                df = dask_df_chunk
                first_chunk = False
            else:
                df = dd.concat([df, dask_df_chunk], interleave_partitions=True)

    return df


def read_query_from_file(path: str) -> str:
    """
    Read SQL query from a file.

    Args:
        path (str): Path to the SQL query file.

    Returns:
        str: SQL query as a string.

    Raises:
        FileNotFoundError: If the specified file is not found.
        IOError: For other I/O related errors.
    """

    try:
        with open(path, "r") as file:
            query = file.read()
        return query
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except IOError as e:
        raise IOError(f"An I/O error occurred while reading the file: {path}. Error: {e}")
