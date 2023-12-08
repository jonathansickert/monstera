from sqlalchemy import Column, DateTime, Float, ForeignKey, MetaData, String, Table

meta = MetaData()

CategoriesTable = Table(
    "categories",
    meta,
    Column("category_gid", String, primary_key=True),
    Column("category_name", String, unique=True),
    Column("load_date", DateTime, nullable=False, index=True),
)


ExpensesTable = Table(
    "expenses",
    meta,
    Column("expense_gid", String, primary_key=True),
    Column(
        "category_gid",
        String,
        ForeignKey(CategoriesTable.c.category_gid, ondelete="cascade"),
        index=True,
        nullable=False,
    ),
    Column("expense_name", String, nullable=False, index=True),
    Column("amount", Float, nullable=False, index=True),
    Column("date", String, nullable=False, index=True),
    Column("load_date", DateTime, nullable=False, index=True),
)
