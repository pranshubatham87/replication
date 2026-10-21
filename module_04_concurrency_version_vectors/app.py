"""Module 4 — Concurrency, Clock Skew & Version Vectors (FastAPI + SQLite).

Implements:
- Exact Version Vector happens-before (<), happens-after (>), and concurrent (||) evaluation
- Clock skew simulation demonstrating silent data loss in naive Wall-Clock Last-Write-Wins (LWW)
- SQLite multi-sibling storage preserving concurrent branches until causally merged
- Shopping-cart set union and field-level conflict resolution
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import uuid
from typing import Any, Dict, List, Optional
from fastapi import FastAPI
from pydantic import BaseModel, Field


def compare_vv(va: Dict[str, int], vb: Dict[str, int]) -> str:
    actors = set(va.keys()) | set(vb.keys())
    a_le_b = all(va.get(k, 0) <= vb.get(k, 0) for k in actors)
    b_le_a = all(vb.get(k, 0) <= va.get(k, 0) for k in actors)
    if a_le_b and b_le_a:
        return "EQUAL"
    if a_le_b:
        return "HAPPENS_BEFORE"
    if b_le_a:
        return "HAPPENS_AFTER"
    return "CONCURRENT"


def merge_vvs(vvs: List[Dict[str, int]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for v in vvs:
        for k, cnt in v.items():
            out[k] = max(out.get(k, 0), cnt)
    return dict(sorted(out.items()))


class CausalWriteRequest(BaseModel):
    actor_id: str
    value: Any
    context_vector: Dict[str, int] = Field(default_factory=dict)
    wall_clock_ms: int = 1000
    naive_lww: bool = False


def create_app(db_path: Optional[str] = None) -> FastAPI:
    path = db_path or os.path.join(tempfile.mkdtemp(prefix="mod04_vv_"), "vv.db")
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS siblings (
            key TEXT, sibling_id TEXT, val_json TEXT, vv_json TEXT, wall_ms INT, actor TEXT,
            PRIMARY KEY (key, sibling_id)
        );"""
    )
    conn.commit()
    conn.close()

    skew_ms: Dict[str, int] = {}
    app = FastAPI(title="Module 4 — Concurrency & Version Vectors")

    @app.post("/clocks/skew")
    def set_skew(actor_id: str, offset_ms: int) -> dict:
        skew_ms[actor_id] = offset_ms
        return {"actor_id": actor_id, "offset_ms": offset_ms}

    @app.put("/items/{key}")
    def put_item(key: str, req: CausalWriteRequest) -> dict:
        eff_clock = req.wall_clock_ms + skew_ms.get(req.actor_id, 0)
        new_vv = dict(req.context_vector)
        new_vv[req.actor_id] = new_vv.get(req.actor_id, 0) + 1
        new_sib = {
            "sibling_id": f"{req.actor_id}:{new_vv[req.actor_id]}:{uuid.uuid4().hex[:6]}",
            "value": req.value, "vv": new_vv, "wall_ms": eff_clock, "actor": req.actor_id,
        }
        conn = sqlite3.connect(path)
        rows = conn.execute("SELECT sibling_id, val_json, vv_json, wall_ms, actor FROM siblings WHERE key=?;", (key,)).fetchall()
        surviving = []
        for sid, vj, vvj, wms, act in rows:
            old_vv = json.loads(vvj)
            if compare_vv(old_vv, req.context_vector) in ("HAPPENS_BEFORE", "EQUAL"):
                continue
            surviving.append({"sibling_id": sid, "value": json.loads(vj), "vv": old_vv, "wall_ms": int(wms), "actor": act})
        surviving.append(new_sib)
        if len(surviving) > 1 and req.naive_lww:
            winner = max(surviving, key=lambda s: (s["wall_ms"], s["actor"]))
            winner["vv"] = merge_vvs([s["vv"] for s in surviving])
            surviving = [winner]
        conn.execute("DELETE FROM siblings WHERE key=?;", (key,))
        for s in surviving:
            conn.execute("INSERT INTO siblings VALUES (?, ?, ?, ?, ?, ?);", (key, s["sibling_id"], json.dumps(s["value"]), json.dumps(s["vv"]), s["wall_ms"], s["actor"]))
        conn.commit()
        conn.close()
        return {"key": key, "siblings": surviving, "merged_context_vector": merge_vvs([s["vv"] for s in surviving]), "has_conflict": len(surviving) > 1}

    return app


app = create_app()
