from sqlalchemy import Column, Date, DateTime, Float, String, Table

from monstera.db.meta import meta

CategoriesTable = Table(
    "categories",
    meta,
    Column("category_name", String, primary_key=True),
    Column("load_date", DateTime, nullable=False, index=True),
)


ExpensesTable = Table(
    "expenses",
    meta,
    Column("expense_gid", String, primary_key=True),
    Column("category_name", String, nullable=False),
    Column("expense_name", String, nullable=False, index=True),
    Column("amount", Float, nullable=False, index=True),
    Column("date", Date, nullable=False, index=True),
    Column("load_date", DateTime, nullable=False, index=True),
)
