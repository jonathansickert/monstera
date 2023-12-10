from dataclasses import dataclass
from typing import Any

import pandas as pd
from sqlalchemy import CursorResult, Engine, Executable, Select, Table, create_engine

__all__: list[str] = ["DBResource"]


@dataclass
class DBResource:
    url: str

    @property
    def engine(self) -> Engine:
        return create_engine(self.url)

    def query_df(self, stmt: Select) -> pd.DataFrame:
        with self.engine.begin() as conn:
            return pd.read_sql(sql=stmt, con=conn)

    def write_df(self, table: Table, df: pd.DataFrame) -> int:
        records_written: int | None = 0
        if df.empty is True:
            return records_written
        with self.engine.begin() as conn:
            records_written = df.to_sql(
                name=table.name,
                con=conn,
                if_exists="append",
                index=False,
            )
        return 0 if records_written is None else records_written

    def purge(self, table: Table) -> None:
        with self.engine.begin() as conn:
            conn.execute(table.delete())

    def execute(self, stmt: Executable) -> CursorResult[Any]:
        with self.engine.begin() as conn:
            return conn.execute(statement=stmt)
