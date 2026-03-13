"""Job configuration — deserialized from job.conf JSON files."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass
class JobConf:
    job_name: str = ""
    modules: list[dict] = field(default_factory=list)
    first_effective_date: date | None = None

    @classmethod
    def from_file(cls, path: str | Path) -> JobConf:
        with open(path) as f:
            raw = json.load(f)
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: dict) -> JobConf:
        fed = None
        if "firstEffectiveDate" in raw and raw["firstEffectiveDate"]:
            fed = date.fromisoformat(raw["firstEffectiveDate"])

        return cls(
            job_name=raw.get("jobName", raw.get("JobName", "")),
            modules=raw.get("modules", raw.get("Modules", [])),
            first_effective_date=fed,
        )
