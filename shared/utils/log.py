import logging
from logging import Logger

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = "%(asctime)s UTC %(levelname)s M:%(module)s P:%(process)d %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str = __name__, level: int = DEFAULT_LOG_LEVEL) -> Logger:
    """Configures and returns a logger with the specified name and level.

    Args:
        name: The name of the logger. Defaults to the name of the current module (`__name__`).
        level: The logging level. Defaults to logging.INFO.

    Returns:
        A Logger instance configured with the specified name and settings.
    """
    logger = logging.getLogger(name)

    # Check if the logger already has handlers configured
    if not logger.handlers:
        logging.basicConfig(
            level=level,
            format=DEFAULT_LOG_FORMAT,
            datefmt=DEFAULT_DATE_FORMAT,
        )
        logger.setLevel(level)

    return logger
