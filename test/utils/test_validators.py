from argparse import ArgumentTypeError
from datetime import datetime

import pytest
from shared.utils.validators import validate_datetime_arg, validate_percentage_arg, validate_positive_integer_arg

# validate_datetime_arg


def test_validate_datetime_arg_valid():
    datetime_str = "2023-11-01 12:34:56"
    expected_datetime = datetime(2023, 11, 1, 12, 34, 56)
    assert validate_datetime_arg(datetime_str) == expected_datetime


def test_validate_datetime_arg_invalid():
    datetime_str = "invalid-datetime-string"
    with pytest.raises(ArgumentTypeError):
        validate_datetime_arg(datetime_str)


def test_validate_datetime_arg_invalid_format():
    datetime_str = "2023-11-01"
    with pytest.raises(ArgumentTypeError):
        validate_datetime_arg(datetime_str)


# validate_percentage_arg


def test_validate_percentage_valid():
    assert validate_percentage_arg("0") == 0
    assert validate_percentage_arg("0.5") == 0.5
    assert validate_percentage_arg("1") == 1


def test_validate_percentage_invalid():
    with pytest.raises(ArgumentTypeError):
        validate_percentage_arg("-0.1")
    with pytest.raises(ArgumentTypeError):
        validate_percentage_arg("1.1")
    with pytest.raises(ArgumentTypeError):
        validate_percentage_arg("invalid")


# validate_positive_integer_arg


def test_validate_positive_integer_valid():
    assert validate_positive_integer_arg("1") == 1
    assert validate_positive_integer_arg("10") == 10


def test_validate_positive_integer_invalid():
    with pytest.raises(ArgumentTypeError):
        validate_positive_integer_arg("0")
    with pytest.raises(ArgumentTypeError):
        validate_positive_integer_arg("-1")
    with pytest.raises(ArgumentTypeError):
        validate_positive_integer_arg("invalid")
