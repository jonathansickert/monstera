import datetime
import locale
import re
from typing import Any, Sequence, TypeGuard, TypeVar

import pandas as pd
from sqlalchemy import Table


def get_current_datetime() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def validate_columns(df: pd.DataFrame, table: Table) -> pd.DataFrame:
    for column in df.columns:
        if column not in table.columns:
            df = df.drop(column)
    return df


def parse_float_from(string: str) -> float:
    locale.setlocale(locale.LC_NUMERIC, "de_DE")
    pattern = r"\d+(?:[.,]?\d+)*(?:[.,]\d+)\b"
    match: str = re.findall(pattern=pattern, string=string)[0]
    match = match.replace(".", "")
    return locale.atof(match)


def parse_date_from(string: str) -> datetime.date:
    date_str: str = string.replace(".", "-")
    # return datetime.datetime.strptime(date_str, "%d-%m-%y").date()
    return datetime.date.fromisoformat(date_str)


def filter_existing_isin(new: pd.DataFrame, old: pd.DataFrame, on: str) -> pd.DataFrame:
    return new[~new[on].isin(old[on])]


def is_sequence(val: Any) -> TypeGuard[Sequence]:
    return isinstance(val, Sequence)


T = TypeVar("T")


def ensure_sequence(scalar_or_sequence: T | Sequence[T]) -> Sequence[T]:
    return (
        scalar_or_sequence if is_sequence(scalar_or_sequence) else [scalar_or_sequence]
    )
