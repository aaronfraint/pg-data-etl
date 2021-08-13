from __future__ import annotations
from datetime import datetime


def timestamp_for_filepath(dt: datetime | None = None) -> str:
    """
    - Make a datetime string formatted like: 'on_2021_02_09_at_09_29_58'

    Arguments:
        dt (datetime | None): Use the provided `datetime`, or the current datetime if None

    Returns:
        str: text representation of date and time to use as suffix for database backup
    """
    if not dt:
        dt = datetime.now()

    return dt.strftime("on_%Y_%m_%d_at_%H_%M_%S")
