import pandas as pd
from sqlalchemy import Table, select

from monstera.core.categories import (
    add_category,
    delete_category,
    find_category,
    get_all_categories,
    get_category_df,
)
from monstera.core.db import DBResource
from monstera.core.schema import CategoriesTable

categories_table: Table = CategoriesTable

TEST_CATEGORY_NAME: str = "test_category"
TEST_CATEGROY_GID: str = "657e9289-8ea0-3599-87eb-e8e53b52b6d6"


def test_get_category_df() -> None:
    df: pd.DataFrame = get_category_df(TEST_CATEGORY_NAME)

    assert df.index.size == 1
    assert df.loc[0]["category_gid"] == TEST_CATEGROY_GID
    assert df.loc[0]["category_name"] == TEST_CATEGORY_NAME


def test_get_category_df_is_deterministic() -> None:
    df1: pd.DataFrame = get_category_df(TEST_CATEGORY_NAME)
    df2: pd.DataFrame = get_category_df(TEST_CATEGORY_NAME)

    pd.testing.assert_frame_equal(
        df1.drop("load_date", axis=1), df2.drop("load_date", axis=1)
    )


def test_add_category_returns_true_for_new_category(db_resource: DBResource) -> None:
    assert add_category(db=db_resource, category=TEST_CATEGORY_NAME) is True
    assert len(db_resource.execute(select(CategoriesTable.c)).fetchall()) == 1


def test_add_category_returns_false_for_duplicate_categories(
    db_resource: DBResource,
) -> None:
    assert add_category(db=db_resource, category=TEST_CATEGORY_NAME) is True
    assert add_category(db=db_resource, category=TEST_CATEGORY_NAME) is False
    assert len(db_resource.execute(select(CategoriesTable.c)).fetchall()) == 1


def test_delete_category(
    db_resource: DBResource,
) -> None:
    assert add_category(db=db_resource, category=TEST_CATEGORY_NAME) is True
    assert delete_category(db=db_resource, category=TEST_CATEGORY_NAME) is True
    assert len(db_resource.execute(select(CategoriesTable.c)).fetchall()) == 0


def test_delete_category_fails_for_non_existent_category(
    db_resource: DBResource,
) -> None:
    assert delete_category(db=db_resource, category=TEST_CATEGORY_NAME) is False


def test_get_all_categories(
    db_resource: DBResource,
) -> None:
    assert add_category(db=db_resource, category=TEST_CATEGORY_NAME + "1") is True
    assert add_category(db=db_resource, category=TEST_CATEGORY_NAME + "2") is True
    assert get_all_categories(db=db_resource) == [
        TEST_CATEGORY_NAME + "1",
        TEST_CATEGORY_NAME + "2",
    ]


def test_find_category(
    db_resource: DBResource,
) -> None:
    assert add_category(db=db_resource, category=TEST_CATEGORY_NAME) is True
    assert find_category(db=db_resource, category=TEST_CATEGORY_NAME) is True
    assert find_category(db=db_resource, category=TEST_CATEGORY_NAME + "1") is False
