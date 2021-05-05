from datetime import datetime


def timestamp_for_filepath(dt: datetime = None) -> str:
    """
    Make a datetime string formatted like: 'on_2021_02_09_at_09_29_58'

        TODO: docstring
    """
    if not dt:
        dt = datetime.now()

    return dt.strftime("on_%Y_%m_%d_at_%H_%M_%S")
