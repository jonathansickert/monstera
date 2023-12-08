from enum import Enum
from uuid import UUID, uuid3

__all__: list[str] = ["GID"]


class GID(Enum):
    CATEGRORIES = "91edbbf6-2ecb-4f49-a08d-94888ce3735d"

    def create(self, *args) -> str:
        name: str = "".join(str(a) for a in args)
        return str(uuid3(UUID(self.value), name=name))
