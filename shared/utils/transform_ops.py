from abc import ABC, abstractmethod
from logging import Logger
import dask.dataframe as dd
from typing import Dict, Tuple, Set


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
