
from datetime import datetime


def validate_quarter_date(value):

    if (
        not isinstance(value, str)
        or len(value) != 8
        or not value.isdigit()
    ):
        raise ValueError(
            "Report date must be in YYYYMMDD format"
        )

    try:
        parsed = datetime.strptime(
            value, "%Y%m%d"
        )

    except ValueError as error:
        raise ValueError(
            "Invalid calendar date"
        ) from error

    valid_quarter_ends = {
        (3, 31),
        (6, 30),
        (9, 30),
        (12, 31),
    }

    if (parsed.month, parsed.day) not in valid_quarter_ends:
        raise ValueError(
            "Date must be a valid quarter-end date"
        )

    return value
