from datetime import datetime

import pytest
from shared.utils.date import get_date_range_for_past_days, get_hour_intervals

# get_date_range_for_past_days


def test_get_date_range_for_past_days():
    current_time = datetime(2023, 11, 1, 12, 34, 56)
    start_date, end_date = get_date_range_for_past_days(7, current_time)

    expected_start_date = datetime(2023, 10, 25, 0, 0, 0)
    expected_end_date = datetime(2023, 10, 31, 23, 59, 59)

    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_date_range_for_one_day():
    current_time = datetime(2023, 11, 1, 12, 34, 56)
    start_date, end_date = get_date_range_for_past_days(1, current_time)

    expected_start_date = datetime(2023, 10, 31, 0, 0, 0)
    expected_end_date = datetime(2023, 10, 31, 23, 59, 59)

    assert start_date == expected_start_date
    assert end_date == expected_end_date


# get_hour_intervals


def test_get_hour_intervals_normal_case():
    start_date = datetime(2022, 12, 31, 23, 30)
    end_date = datetime(2023, 1, 1, 2, 15)
    expected = [
        (datetime(2022, 12, 31, 23, 30), datetime(2023, 1, 1, 0, 29, 59)),
        (datetime(2023, 1, 1, 0, 30), datetime(2023, 1, 1, 1, 29, 59)),
        (datetime(2023, 1, 1, 1, 30), datetime(2023, 1, 1, 2, 29, 59)),
    ]
    assert get_hour_intervals(start_date, end_date) == expected


def test_get_hour_intervals_boundary_case():
    with pytest.raises(ValueError):
        get_hour_intervals(datetime(2022, 1, 1, 1, 0), datetime(2022, 1, 1, 1, 0))


def test_get_hour_intervals_invalid_date_order():
    with pytest.raises(ValueError):
        get_hour_intervals(datetime(2022, 1, 1, 2, 0), datetime(2022, 1, 1, 1, 0))


def test_get_hour_intervals_none_input():
    with pytest.raises(ValueError):
        get_hour_intervals(None, datetime(2022, 1, 1, 1, 0))

    with pytest.raises(ValueError):
        get_hour_intervals(datetime(2022, 1, 1, 1, 0), None)

    with pytest.raises(ValueError):
        get_hour_intervals(None, None)


def test_get_hour_intervals_last_hour():
    start_date = datetime(2023, 1, 1, 0, 0)
    end_date = datetime(2023, 1, 1, 1, 30)  # 1.5 hours later
    expected = [
        (datetime(2023, 1, 1, 0, 0), datetime(2023, 1, 1, 0, 59, 59)),
        (datetime(2023, 1, 1, 1, 0), datetime(2023, 1, 1, 1, 59, 59)),
    ]
    assert get_hour_intervals(start_date, end_date) == expected
