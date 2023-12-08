from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import CursorResult, Table, delete, select
from sqlalchemy.exc import IntegrityError

from monstera.core.db import DBResource
from monstera.core.gid import GID
from monstera.core.schema import CategoriesTable

categories_table: Table = CategoriesTable


def get_category_df(category: str) -> pd.DataFrame:
    return pd.DataFrame.from_records(
        [
            {
                "category_gid": GID.CATEGRORIES.create(category),
                "category_name": category,
                "load_date": datetime.now(tz=timezone.utc),
            }
        ]
    )


def add_category(db: DBResource, category: str) -> bool:
    df: pd.DataFrame = get_category_df(category=category)
    try:
        db.write_df(table=CategoriesTable, df=df)
        return True
    except IntegrityError:
        print(
            f"Warning: insertion of category {category} failed. Please check if the category already exists."
        )
        return False


def delete_category(db: DBResource, category: str) -> bool:
    result: CursorResult[Any] = db.execute(
        stmt=delete(categories_table).where(
            categories_table.c.category_name == category
        )
    )
    return result.rowcount == 1


def get_all_categories(
    db: DBResource,
) -> list[str]:
    return db.query_df(stmt=select(categories_table.c))["category_name"].to_list()


def find_category(db: DBResource, category: str) -> bool:
    result: CursorResult[Any] = db.execute(
        stmt=select(categories_table.c).where(
            categories_table.c.category_name == category
        )
    )
    return len(result.fetchall()) == 1
