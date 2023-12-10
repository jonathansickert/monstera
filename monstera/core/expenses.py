import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import CursorResult, Table, delete, select
from sqlalchemy.exc import IntegrityError

from monstera.core.gid import GID
from monstera.db.db import DBResource
from monstera.db.schema import ExpensesTable
from monstera.helper import (
    filter_existing_isin,
    get_current_datetime,
    parse_date_from,
    parse_float_from,
    validate_columns,
)

expense_table: Table = ExpensesTable


@dataclass
class Expense:
    name: str
    category_name: str
    amount: float
    date: datetime.date

    @property
    def gid(self) -> str:
        return GID.EXPENSES.create(
            self.name, self.category_name, self.amount, self.date
        )

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame.from_records(
            [
                {
                    "expense_gid": self.gid,
                    "expense_name": self.name,
                    "category_name": self.category_name,
                    "amount": self.amount,
                    "date": self.date,
                }
            ]
        )


def get_expenses_from_csv(
    csv_df: pd.DataFrame,
    category_column_name: str,
    expense_column_name: str,
    amount_column_name: str,
    date_column_name: str,
) -> pd.DataFrame:
    expenses: list[Expense] = [
        Expense(
            name=row[expense_column_name],
            category_name=row[category_column_name],
            amount=parse_float_from(row[amount_column_name]),
            date=parse_date_from(row[date_column_name]),
        )
        for _, row in csv_df.iterrows()
    ]

    df: pd.DataFrame = pd.concat([expense.to_df() for expense in expenses])
    df["load_date"] = get_current_datetime()
    df = validate_columns(df=df, table=expense_table)
    return df


def get_all_expenses(
    db: DBResource,
) -> pd.DataFrame:
    return db.query_df(stmt=select(expense_table.c))


def insert_expense_df(db: DBResource, df: pd.DataFrame, ensure_unique) -> int:
    if ensure_unique is True:
        existing_df: pd.DataFrame = get_all_expenses(db=db)
        df = filter_existing_isin(
            new=df, old=existing_df, on="expense_gid"
        ).drop_duplicates(subset="expense_gid")
    records_written: int = 0
    try:
        records_written = db.write_df(table=expense_table, df=df)
    except IntegrityError:
        # TODO: handle integrity error
        pass
    except Exception:
        # TODO: handle general errors
        pass
    return records_written


def add_expense(
    db: DBResource,
    expense_name: str,
    category_name: str,
    amount: float,
    date: datetime.date,
    ensure_unique: bool,
) -> int:
    expense: Expense = Expense(
        name=expense_name, category_name=category_name, amount=amount, date=date
    )
    df: pd.DataFrame = expense.to_df()
    df["load_date"] = get_current_datetime()
    return insert_expense_df(db=db, df=df, ensure_unique=ensure_unique)


def add_expenses_from_csv(
    db: DBResource,
    csv_path: Path,
    sep: str,
    category_column_name: str,
    expense_column_name: str,
    amount_column_name: str,
    date_column_name: str,
    ensure_unique: bool,
) -> int:
    csv_df: pd.DataFrame = pd.read_csv(csv_path, sep=sep)
    df: pd.DataFrame = get_expenses_from_csv(
        csv_df=csv_df,
        category_column_name=category_column_name,
        expense_column_name=expense_column_name,
        amount_column_name=amount_column_name,
        date_column_name=date_column_name,
    )
    return insert_expense_df(db=db, df=df, ensure_unique=ensure_unique)


def delete_expense_from_gid(
    db: DBResource,
    expense_gid: str,
) -> bool:
    result: CursorResult[Any] = db.execute(
        stmt=delete(expense_table).where(expense_table.c.expense_gid == expense_gid)
    )
    return result.rowcount == 1
