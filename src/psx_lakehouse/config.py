"""Names and locations. Override catalog/schema with job parameters."""
from __future__ import annotations

from dataclasses import dataclass

SOURCE_REPO = "shessols/psx-market-data-platform"
SOURCE_BASE = f"https://raw.githubusercontent.com/{SOURCE_REPO}/main/dataset"


@dataclass(frozen=True)
class Names:
    catalog: str = "workspace"   # Databricks Free Edition's default catalog
    schema: str = "psx"

    @property
    def volume(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/raw"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    @property
    def checkpoints(self) -> str:
        return f"{self.volume}/_checkpoints"
