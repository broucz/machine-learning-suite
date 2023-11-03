import os
from typing import Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from .db_ops import AbstractDbClient


class ClickHouseDbClient(AbstractDbClient):
    """ClickHouse database client that abstracts the creation of a ClickHouse connection
    and falls back on environment variables or default values if specific configurations
    are not provided.
    """

    DEFAULT_HOST = "chp.ovh.0x3e.net"
    DEFAULT_PORT = "9090"
    DEFAULT_USER = "generic-raw"

    def get_client(self, host: Optional[str] = None, port: Optional[str] = None, user: Optional[str] = None) -> Client:
        """Returns a ClickHouse client using the provided configurations or
        environment variables if not provided.

        Args:
            host: The host address for ClickHouse.
            port: The port for ClickHouse.
            user: The user for ClickHouse.

        Returns:
            An instance of the ClickHouse client.
        """

        host = host or os.environ.get("CH_HOST", self.DEFAULT_HOST)
        port = port or os.environ.get("CH_PORT", self.DEFAULT_PORT)
        user = user or os.environ.get("CH_USER", self.DEFAULT_USER)

        return clickhouse_connect.get_client(host=host, port=port, user=user)
