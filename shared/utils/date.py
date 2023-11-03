from datetime import datetime, timedelta
from typing import List, Tuple

END_OF_DAY = (23, 59, 59)
START_OF_DAY = (0, 0, 0)


def get_date_range_for_past_days(days: int, current_time: datetime = datetime.now()) -> Tuple[datetime, datetime]:
    """Generate a date range for the past specified number of days, inclusive.

    Args:
        days: Number of past days to generate the date range for.
        current_time: The current time, useful for testing. Defaults to datetime.now().

    Returns:
        Start and end datetime objects for the date range.
    """

    end_date = datetime(current_time.year, current_time.month, current_time.day, *END_OF_DAY) - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)
    start_date = datetime(start_date.year, start_date.month, start_date.day, *START_OF_DAY)

    return start_date, end_date


def get_hour_intervals(start_date: datetime, end_date: datetime) -> List[Tuple[datetime, datetime]]:
    """Generate a list of hourly intervals between two dates.

    Args:
        start_date: The start date and time.
        end_date: The end date and time.

    Returns:
        A list of tuples, each containing the start and end of an hourly interval.

    Raises:
        ValueError: If end_date is before start_date or if any date is None.
    """

    if start_date is None or end_date is None:
        raise ValueError("Both start_date and end_date must be provided")
    if start_date >= end_date:
        raise ValueError("start_date must be before end_date")

    seconds_per_hour = 3600
    one_second = timedelta(seconds=1)
    one_hour = timedelta(hours=1)

    total_seconds = (end_date - start_date).total_seconds()
    total_hours = total_seconds // seconds_per_hour
    if total_seconds % seconds_per_hour > 0:
        total_hours += 1

    hour_ranges = []
    start = start_date
    for _ in range(int(total_hours)):
        end = start + one_hour - one_second
        hour_ranges.append((start, end))
        start = end + one_second

    return hour_ranges
