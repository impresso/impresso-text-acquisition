"""Data models for Chronicling America bulk downloads."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date


@dataclass(frozen=True)
class TitleSpec:
    lccn: str
    alias: str
    start_date: date | None = None
    end_date: date | None = None


@dataclass(frozen=True)
class TarballInfo:
    batch: str
    url: str
    sha1: str
    size: int
    lccns: tuple[str, ...] = ()
    issue_count: int = 0


@dataclass(frozen=True)
class IssueInfo:
    batch: str
    lccn: str
    alias: str
    date: str
    edition: str
    issue_dir_name: str
    url: str


@dataclass
class DownloadPlan:
    titles: list[TitleSpec]
    batches: list[str]
    tarballs: list[TarballInfo]
    issues: list[IssueInfo]
    total_tarball_bytes: int = 0
    estimated_issues: int | None = None


@dataclass
class DownloadState:
    completed_tarballs: dict[str, str] = field(default_factory=dict)
    completed_issues: set[str] = field(default_factory=set)

    @staticmethod
    def load(path: str) -> DownloadState:
        if not os.path.exists(path):
            return DownloadState()
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return DownloadState(
            completed_tarballs=data.get("completed_tarballs", {}),
            completed_issues=set(data.get("completed_issues", [])),
        )

    def save(self, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "completed_tarballs": self.completed_tarballs,
                    "completed_issues": sorted(self.completed_issues),
                },
                f,
                indent=2,
            )
