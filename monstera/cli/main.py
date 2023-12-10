import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import typer
from rich import print
from rich.table import Table
from typing_extensions import Annotated

from monstera.core.expenses import (
    add_expense,
    add_expenses_from_csv,
    delete_expense_from_gid,
    get_all_expenses,
)
from monstera.db import MONSTERA_DB
from monstera.db.db import DBResource
from monstera.db.meta import meta

app = typer.Typer()
db = DBResource(url=f"sqlite:///{MONSTERA_DB}")


def print_df(df: pd.DataFrame) -> None:
    table = Table(*df.columns, style="green")
    for _, row in df.iterrows():
        row_str: pd.Series[str] = row.astype(str)
        table.add_row(*row_str)
    print(table)


@app.command("init")
def init() -> None:
    meta.create_all(db.engine)
    print("[b green]Initialized database[/b green]")


@app.command("reset")
def reset() -> None:
    open(MONSTERA_DB, "w").close()
    meta.create_all(db.engine)
    print("[b green]Resetted database[/b green]")


@app.command("add")
def add(
    expense_name: Annotated[str, typer.Argument()],
    category_name: Annotated[str, typer.Argument()],
    amount: Annotated[float, typer.Argument()],
    date: Annotated[Optional[str], typer.Argument()] = None,
    ensure_unique: Annotated[bool, typer.Argument()] = True,
) -> None:
    _date: datetime.date
    if date is None:
        _date = datetime.datetime.now(datetime.UTC).date()
    else:
        _date = datetime.date.fromisoformat(date)

    if (
        add_expense(
            db=db,
            expense_name=expense_name,
            category_name=category_name,
            amount=amount,
            date=_date,
            ensure_unique=ensure_unique,
        )
        == 1
    ):
        print(":gem: [b green]Sucessfully added expense![/b green]")
    else:
        print(
            f":alert: [b red]Warning![/b red] Insertion of expense {expense_name} failed. Please check if the expense already exists."
        )


@app.command("load-csv")
def load_csv(
    csv_path: Annotated[str, typer.Argument()],
    sep: Annotated[str, typer.Argument()],
    category_column_name: Annotated[str, typer.Argument()],
    expense_column_name: Annotated[str, typer.Argument()],
    amount_column_name: Annotated[str, typer.Argument()],
    date_column_name: Annotated[str, typer.Argument()],
    ensure_unique: Annotated[bool, typer.Argument()] = True,
) -> None:
    expenses_added: int = add_expenses_from_csv(
        db=db,
        csv_path=Path(csv_path),
        sep=sep,
        category_column_name=category_column_name,
        expense_column_name=expense_column_name,
        amount_column_name=amount_column_name,
        date_column_name=date_column_name,
        ensure_unique=ensure_unique,
    )
    print(f"[b green]Added '{expenses_added}' new expenses[/b green]")


@app.command("list")
def list_expenses() -> None:
    df: pd.DataFrame = get_all_expenses(db=db)
    print_df(df=df)


@app.command("delete")
def delete_expenses(expense_gid: Annotated[str, typer.Argument()]) -> None:
    delete_expense_from_gid(db=db, expense_gid=expense_gid)
