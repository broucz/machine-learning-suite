from unittest.mock import MagicMock, mock_open, patch

import dask.dataframe as dd
import pandas as pd
import pytest
from shared.utils.db_ops import (
    QueryExecutionError,
    SupportsQueryDFStream,
    execute_query_to_dask_df,
    read_query_from_file,
)

# execute_query_to_dask_df


class MockClient(SupportsQueryDFStream):
    def query_df_stream(self, query: str):
        mock_stream = MagicMock()
        if query == "INVALID QUERY":
            mock_stream.__iter__.side_effect = QueryExecutionError("Some database error message")
        else:
            mock_stream.__iter__.return_value = iter(
                [pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]}), pd.DataFrame({"col1": [3, 4], "col2": ["c", "d"]})]
            )
        return mock_stream


def test_execute_query_basic():
    mock_client = MockClient()
    result = execute_query_to_dask_df(client=mock_client, query="SELECT * FROM table")
    assert isinstance(result, dd.DataFrame)
    assert len(result.compute()) == 4


def test_execute_query_with_params():
    mock_client = MockClient()
    result = execute_query_to_dask_df(
        client=mock_client, query="SELECT * FROM table LIMIT {value}", params={"value": 10}
    )
    assert isinstance(result, dd.DataFrame)
    assert len(result.compute()) == 4


def test_execute_query_invalid_query():
    mock_client = MockClient()
    with pytest.raises(QueryExecutionError):
        execute_query_to_dask_df(client=mock_client, query="INVALID QUERY")


# read_query_from_file


def test_read_query_from_file_success():
    mock_file_content = "SELECT * FROM table;"
    with patch("builtins.open", mock_open(read_data=mock_file_content)) as mock_file:
        result = read_query_from_file("fake_path")
        mock_file.assert_called_once_with("fake_path", "r")
        assert result == mock_file_content


def test_read_query_from_file_file_not_found():
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = FileNotFoundError
        with pytest.raises(FileNotFoundError):
            read_query_from_file("nonexistent_path")


def test_read_query_from_file_io_error():
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = IOError
        with pytest.raises(IOError):
            read_query_from_file("some_path")
