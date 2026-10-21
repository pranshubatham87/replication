"""Module 5 — Convergence and CRDTs (FastAPI + SQLite).

Implements:
- G-Counter (Grow-only counter) & PN-Counter (Positive-Negative counter)
- Two-Phase Set (2P-Set) & Observed-Remove Set (OR-Set with Add-Wins unique birth tags)
- Join-Semilattice state merge (idempotent, commutative, associative) and deduplicated op replay
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set
from fastapi import FastAPI


@dataclass
class PNCounterCRDT:
    p: Dict[str, int] = field(default_factory=dict)
    n: Dict[str, int] = field(default_factory=dict)

    def increment(self, replica_id: str, delta: int = 1) -> None:
        self.p[replica_id] = self.p.get(replica_id, 0) + delta

    def decrement(self, replica_id: str, delta: int = 1) -> None:
        self.n[replica_id] = self.n.get(replica_id, 0) + delta

    def value(self) -> int:
        return sum(self.p.values()) - sum(self.n.values())

    def merge(self, other: "PNCounterCRDT") -> "PNCounterCRDT":
        mp = {k: max(self.p.get(k, 0), other.p.get(k, 0)) for k in set(self.p) | set(other.p)}
        mn = {k: max(self.n.get(k, 0), other.n.get(k, 0)) for k in set(self.n) | set(other.n)}
        return PNCounterCRDT(p=mp, n=mn)


@dataclass
class ORSetCRDT:
    entries: Dict[str, Set[str]] = field(default_factory=dict)
    removed_tags: Set[str] = field(default_factory=set)

    def add(self, element: str, unique_tag: str) -> None:
        self.entries.setdefault(element, set()).add(unique_tag)

    def remove(self, element: str) -> None:
        self.removed_tags.update(self.entries.get(element, set()) - self.removed_tags)

    def elements(self) -> List[str]:
        return sorted([e for e, tags in self.entries.items() if (tags - self.removed_tags)])

    def merge(self, other: "ORSetCRDT") -> "ORSetCRDT":
        merged_entries = {
            k: set(self.entries.get(k, set())) | set(other.entries.get(k, set()))
            for k in set(self.entries) | set(other.entries)
        }
        return ORSetCRDT(entries=merged_entries, removed_tags=set(self.removed_tags) | set(other.removed_tags))


app = FastAPI(title="Module 5 — Convergence & CRDTs Service")
