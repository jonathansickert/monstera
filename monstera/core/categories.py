import datetime
from typing import Any

import pandas as pd
from sqlalchemy import CursorResult, Table, delete, select
from sqlalchemy.exc import IntegrityError

from monstera.db.db import DBResource
from monstera.core.gid import GID
from monstera.db.schema import CategoriesTable

categories_table: Table = CategoriesTable


def get_category_df(category_name: str) -> pd.DataFrame:
    return pd.DataFrame.from_records(
        [
            {
                "category_gid": GID.CATEGRORIES.create(category_name),
                "category_name": category_name,
                "load_date": datetime.datetime.now(datetime.UTC),
            }
        ]
    )


def add_category(db: DBResource, category_name: str) -> bool:
    df: pd.DataFrame = get_category_df(category_name=category_name)
    try:
        db.write_df(table=CategoriesTable, df=df)
        return True
    except IntegrityError:
        print(
            f"Warning: insertion of category {category_name} failed. Please check if the category already exists."
        )
        return False


def delete_category(db: DBResource, category_name: str) -> bool:
    result: CursorResult[Any] = db.execute(
        stmt=delete(categories_table).where(
            categories_table.c.category_name == category_name
        )
    )
    return result.rowcount == 1


def get_all_categories(
    db: DBResource,
) -> list[str]:
    return db.query_df(stmt=select(categories_table.c))["category_name"].to_list()


def find_category(db: DBResource, category_name: str) -> bool:
    result: CursorResult[Any] = db.execute(
        stmt=select(categories_table.c).where(
            categories_table.c.category_name == category_name
        )
    )
    return len(result.fetchall()) == 1
