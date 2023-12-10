from pathlib import Path
from typing import Iterator

import pytest

from monstera.db.db import DBResource
from monstera.db.schema import meta


@pytest.fixture
def db_resource(tmp_path: Path) -> Iterator[DBResource]:
    test_db: Path = tmp_path / "test.db"
    with open(test_db, "w"):
        pass
    db = DBResource(url=f"sqlite:///{test_db}")
    meta.create_all(db.engine)

    yield db
