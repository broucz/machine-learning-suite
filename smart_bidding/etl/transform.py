from logging import Logger
from typing import Set

import dask.dataframe as dd
from shared.utils.transform_ops import AbstractDictionary, AbstractTransformer


class Transformer(AbstractTransformer):
    def __init__(self, dictionary: AbstractDictionary, logger: Logger):
        super().__init__(dictionary, logger)

        self.logged_device_keys: Set[int] = set()
        self.logged_browser_keys: Set[int] = set()
        self.logged_product_category_keys: Set[int] = set()
        self.logged_content_category_keys: Set[int] = set()
        self.logged_ad_format_keys: Set[int] = set()

    def transform_dask_df(self, df: dd.DataFrame) -> dd.DataFrame:
        """Transforms the input dataframe.

        Args:
            df: Input Dask dataframe to be transformed.

        Returns:
            Transformed Dask dataframe.
        """

        # Rename columns.
        rename_dict = {
            "idlanguage": "browser_language",
            "region_geoname_id": "region",
            "city_geoname_id": "city",
            "idos": "os",
            "idproxy": "proxy",
            "idadvertiser": "advertiser_id",
            "idcampaign": "campaign_id",
            "idvariation": "variation_id",
            "idadvertiser_ad_type": "campaign_type",
            "ad_type": "zone_type",
            "idpublisher": "publisher_id",
            "idsite": "site_id",
            "idzone": "zone_id",
            "sub": "sub_id",
            "idtraffic_type": "traffic_type",
            "goal": "conversion_status",
        }
        df = df.rename(columns=rename_dict)

        # Maps values to extract features. Drops the original ones.
        df["device_type"] = df["iddevice"].map(
            lambda x: self.get_mapped_value(x, "device_dict", self.logged_device_keys)[0]
        )
        df["device_brand"] = df["iddevice"].map(
            lambda x: self.get_mapped_value(x, "device_dict", self.logged_device_keys)[1]
        )
        df["ad_category"] = df["idproduct_category"].map(
            lambda x: self.get_mapped_value(x, "product_category_dict", self.logged_product_category_keys)[0]
        )
        df["ad_sub_category"] = df["idproduct_category"].map(
            lambda x: self.get_mapped_value(x, "product_category_dict", self.logged_product_category_keys)[1]
        )
        df["content_category"] = df["idcategory"].map(
            lambda x: self.get_mapped_value(x, "content_category_dict", self.logged_content_category_keys)[0]
        )
        df["content_sub_category"] = df["idcategory"].map(
            lambda x: self.get_mapped_value(x, "content_category_dict", self.logged_content_category_keys)[1]
        )
        df = df.drop(["iddevice", "idproduct_category", "idcategory"], axis=1)

        # Extract hour and day from date_time.
        df["hour_of_day"] = df["date_time"].dt.hour
        df["day_of_week"] = df["date_time"].dt.dayofweek

        return df
