import json
import os
from shared.utils.transform_ops import AbstractDictionary
from typing import Dict, Tuple


class Dictionary(AbstractDictionary):
    def __init__(self, root_dir: str):
        self.root_dir = root_dir

        self.dictionaries: Dict[str, Dict[int, Tuple[int, int]]] = {
            "device_dict": {},
            "product_category_dict": {},
            "content_category_dict": {},
        }

        self._load_devices()
        self._load_product_categories()
        self._load_content_categories()

    def _load_devices(self) -> None:
        with open(os.path.join(self.root_dir, "devices.json"), "r") as file:
            devices_data = json.load(file)

            device_brand = {}
            for device in devices_data:
                brand_name = device["name"].lower().replace(" ", "_")
                if brand_name not in device_brand:
                    device_brand[brand_name] = len(device_brand) + 1

                self.dictionaries["device_dict"][device["id"]] = (
                    device["device_type"]["id"],
                    device_brand[brand_name],
                )

    def _load_product_categories(self) -> None:
        with open(os.path.join(self.root_dir, "product_categories.json"), "r") as file:
            product_categories_data = json.load(file)

            for product_category in product_categories_data:
                self.dictionaries["product_category_dict"][product_category["id"]] = (
                    product_category["id"],
                    product_category["parent"],
                )

    def _load_content_categories(self) -> None:
        with open(os.path.join(self.root_dir, "content_categories.json"), "r") as file:
            content_categories_data = json.load(file)

            for content_category in content_categories_data:
                self.dictionaries["content_category_dict"][content_category["id"]] = (
                    content_category["id"],
                    content_category["parent"],
                )

    def get_dictionary(self, dictionary_name: str) -> Dict[int, Tuple[int, int]]:
        """Retrieves a dictionary of mappings based on the provided name.

        Args:
            dictionary_name: The name of the dictionary to retrieve.

        Returns:
            A dictionary with keys and values corresponding to the mapping.

        Raises:
            ValueError: If the dictionary name is not valid.
        """
        if dictionary_name not in self.dictionaries:
            raise ValueError(f"{dictionary_name} is not a valid dictionary name.")

        return self.dictionaries[dictionary_name]
