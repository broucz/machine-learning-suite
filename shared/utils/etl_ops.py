from argparse import Namespace
from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor
from logging import Logger
from typing import Any, Dict, Set, Tuple
import dask.dataframe as dd


class TransformError(Exception):
    """Custom exception to indicate errors during the ETL transform process.

    Raised when a transformation on a dataset fails during the ETL process,
    allowing for more specific error handling related to data transformations.
    """

    ...


class AbstractDictionary(ABC):
    """Abstract base class for dictionaries used in data transformation.

    This class should be extended with specific implementations that provide
    a method for retrieving dictionary mappings.
    """

    @abstractmethod
    def get_dictionary(self, dictionary_name: str) -> Dict[int, Tuple[int, int]]:
        """Retrieves a dictionary of mappings based on the provided name.

        Args:
            dictionary_name: The name of the dictionary to retrieve.

        Returns:
            A dictionary with keys and values corresponding to the mapping.

        Raises:
            NotImplementedError: If the method is not implemented in the derived class.
        """
        raise NotImplementedError("Subclasses must implement this method")


class AbstractTransformer(ABC):
    """Abstract base class for transformers that apply data transformation logic.

    This class should be extended with specific implementations that apply
    transformations to dataframes.
    """

    def __init__(self, dictionary: AbstractDictionary, logger: Logger):
        """Initializes the AbstractTransformer with a dictionary and a logger.

        Args:
            dictionary: An instance of AbstractDictionary to be used for mapping values.
            logger: An instance of Logger to log information during the transformation process.
        """
        self.dictionary = dictionary
        self.logger = logger

    def get_mapped_value(self, x: int, dictionary_name: str, logged_keys: Set) -> Tuple[int, int]:
        """Utility function to get the mapped value and log if a key is missing.

        Args:
            x: The value to map.
            dictionary_name: The name of the dictionary to use for mapping.
            logged_keys: A set of keys to log if missing from the dictionary.

        Returns:
            The mapped value as a tuple. Returns a default value if the key is missing.
        """
        mapping_dict = self.dictionary.get_dictionary(dictionary_name)

        if x not in mapping_dict and x not in logged_keys:
            self.logger.info(f"Missing entry for key {x} in {dictionary_name}")
            logged_keys.add(x)

        return mapping_dict.get(x, (0, 0))

    @abstractmethod
    def transform_dask_df(self, df: dd.DataFrame) -> dd.DataFrame:
        """Abstract method to transform the input Dask dataframe.

        This method should be implemented by subclasses to define specific transformation logic.

        Args:
            df: The input Dask dataframe to be transformed.

        Returns:
            The transformed Dask dataframe.

        Raises:
            NotImplementedError: If the method is not implemented in the derived class.
        """
        raise NotImplementedError("Subclasses must implement this method")


class AbstractProcessor(ABC):
    """Abstract base class for data processors that defines the standard ETL operations.

    Derived classes should implement the methods `_submit_futures` and `_process_future_results`
    to execute the ETL process using a multi-threaded approach.
    """

    def __init__(self, args: Namespace, logger: Logger):
        """Initializes the AbstractProcessor with command line arguments and a logger.

        Args:
            args: Command line arguments that provide configuration settings.
            logger: A logging object to record the process's progress.
        """
        self.args = args
        self.logger = logger

    @abstractmethod
    def _submit_futures(self, executor: ThreadPoolExecutor) -> Dict[Future, Dict[str, Any]]:
        """Submits tasks to the executor and returns a dictionary of futures with associated metadata.

        Args:
            executor: The ThreadPoolExecutor object used for submitting tasks.

        Returns:
            A dictionary where keys are `Future` objects and values are metadata dictionaries.

        Raises:
            NotImplementedError: If the method is not implemented in the derived class.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def _process_future_results(self, futures: Dict[Future, Dict[str, Any]]) -> None:
        """Processes the results of the completed futures and handles exceptions.

        Args:
            futures: A dictionary of futures with associated metadata.

        Raises:
            NotImplementedError: If the method is not implemented in the derived class.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def run(self) -> None:
        """The main method to execute the processor's tasks, typically invoking the submission of
        futures and processing of results.

        Raises:
            NotImplementedError: If the method is not implemented in the derived class.
        """
        raise NotImplementedError("Subclasses must implement this method")
