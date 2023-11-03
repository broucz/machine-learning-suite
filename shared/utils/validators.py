from argparse import ArgumentTypeError
from datetime import datetime

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def validate_datetime_arg(datetime_arg: str) -> datetime:
    """Custom argparse type for user datetime values given from the command line.

    Args:
        datetime_arg: The string datetime to validate.

    Returns:
        The parsed datetime.

    Raises:
        ArgumentTypeError: If datetime_arg is invalid.
    """
    try:
        return datetime.strptime(datetime_arg, DATETIME_FORMAT)
    except ValueError:
        msg = f"Given datetime ({datetime_arg}) is not valid. Expected format: '{DATETIME_FORMAT}'"
        raise ArgumentTypeError(msg)


def validate_percentage_arg(value: str) -> float:
    """Validates if the given value is a percentage between 0 and 1.

    Args:
        value: The string representation of the value to validate.

    Returns:
        The validated value as a float.

    Raises:
        ArgumentTypeError: If the value is not between 0 and 1.
    """
    try:
        value = float(value)
    except ValueError:
        raise ArgumentTypeError("Must be a valid number.")

    if 0 <= value <= 1:
        return value
    raise ArgumentTypeError("Percentage must be between 0 and 1.")


def validate_positive_integer_arg(value: str) -> int:
    """Validates if the given value is a positive integer.

    Args:
        value: The string representation of the value to validate.

    Returns:
        The validated value as an integer.

    Raises:
        ArgumentTypeError: If the value is not a positive integer.
    """
    try:
        value = int(value)
    except ValueError:
        raise ArgumentTypeError("Must be a valid integer.")

    if value > 0:
        return value
    raise ArgumentTypeError("Must be a positive integer.")
