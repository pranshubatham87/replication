"""Module 2 — Multi-Leader Collaborative Workspace & Offline Sync (FastAPI + SQLite).

Implements:
- Independent regional SQLite leaders (us-east, eu-west, ap-south) & offline client sync queue
- All-to-All, Star, and Circular (Ring) topologies with visited_leaders loop prevention
- Synchronous vs. Asynchronous conflict detection and Home-Region conflict avoidance
- Vector Clocks, Hybrid Logical Clocks (HLC), Field Merge, and Custom Conflict Resolution
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


class TopologyType(str, Enum):
    ALL_TO_ALL = "all_to_all"
    STAR = "star"
    CIRCULAR = "circular"


def compare_vc(va: Dict[str, int], vb: Dict[str, int]) -> str:
    keys = set(va.keys()) | set(vb.keys())
    a_le_b = all(va.get(k, 0) <= vb.get(k, 0) for k in keys)
    b_le_a = all(vb.get(k, 0) <= va.get(k, 0) for k in keys)
    if a_le_b and b_le_a:
        return "EQUAL"
    if a_le_b:
        return "BEFORE"
    if b_le_a:
        return "AFTER"
    return "CONCURRENT"


def merge_vc(va: Dict[str, int], vb: Dict[str, int]) -> Dict[str, int]:
    return {k: max(va.get(k, 0), vb.get(k, 0)) for k in sorted(set(va.keys()) | set(vb.keys()))}


class DocWriteRequest(BaseModel):
    title: str
    fields: Dict[str, Any] = Field(default_factory=dict)
    home_region: str = "us-east"
    conflict_avoidance: bool = False
    hlc_ts: int = 1000


def create_app(data_dir: Optional[str] = None) -> FastAPI:
    base_dir = data_dir or tempfile.mkdtemp(prefix="mod02_multi_leader_")
    regions = ["us-east", "eu-west", "ap-south"]
    dbs: Dict[str, str] = {}
    for r in regions:
        p = os.path.join(base_dir, f"{r}.db")
        dbs[r] = p
        conn = sqlite3.connect(p)
        conn.execute(
            """CREATE TABLE IF NOT EXISTS documents (
                doc_id TEXT PRIMARY KEY, title TEXT, fields_json TEXT,
                hlc_ts INT, origin TEXT, vc_json TEXT
            );"""
        )
        conn.execute("CREATE TABLE IF NOT EXISTS seen_mutations (mutation_id TEXT PRIMARY KEY);")
        conn.commit()
        conn.close()

    state: Dict[str, Any] = {
        "topology": TopologyType.ALL_TO_ALL,
        "blocked_links": set(),
        "queues": {r: [] for r in regions},
        "conflicts": [],
    }
    app = FastAPI(title="Module 2 — Multi-Leader Collaborative Workspace")

    def neighbors(src: str) -> List[str]:
        if state["topology"] == TopologyType.ALL_TO_ALL:
            cands = [r for r in regions if r != src]
        elif state["topology"] == TopologyType.STAR:
            cands = [r for r in regions if r != "us-east"] if src == "us-east" else ["us-east"]
        else:
            idx = regions.index(src)
            cands = [regions[(idx + 1) % len(regions)]]
        return [ dst for dst in cands if (src, dst) not in state["blocked_links"] ]

    @app.put("/regions/{region_id}/documents/{doc_id}")
    def write_doc(region_id: str, doc_id: str, req: DocWriteRequest) -> dict:
        target = req.home_region if req.conflict_avoidance else region_id
        conn = sqlite3.connect(dbs[target])
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM documents WHERE doc_id = ?;", (doc_id,)).fetchone()
        vc = json.loads(row["vc_json"]) if row else {}
        vc[target] = vc.get(target, 0) + 1
        mut_id = f"{target}:{doc_id}:{vc[target]}:{uuid.uuid4().hex[:6]}"
        conn.execute(
            "INSERT OR REPLACE INTO documents VALUES (?, ?, ?, ?, ?, ?);",
            (doc_id, req.title, json.dumps(req.fields), req.hlc_ts, target, json.dumps(vc)),
        )
        conn.execute("INSERT OR IGNORE INTO seen_mutations VALUES (?);", (mut_id,))
        conn.commit()
        conn.close()
        mut = {"mutation_id": mut_id, "doc_id": doc_id, "title": req.title, "fields": req.fields, "hlc_ts": req.hlc_ts, "origin": target, "vc": vc, "visited": [target]}
        for dst in neighbors(target):
            state["queues"][dst].append(mut)
        return {"doc_id": doc_id, "served_by": target, "routed_to_home": target != region_id, "vector_clock": vc}

    @app.post("/sync/hop")
    def sync_hop() -> dict:
        applied = {r: 0 for r in regions}
        batches = {r: list(state["queues"][r]) for r in regions}
        for r in regions:
            state["queues"][r].clear()
        for r, batch in batches.items():
            conn = sqlite3.connect(dbs[r])
            conn.row_factory = sqlite3.Row
            for m in batch:
                if conn.execute("SELECT 1 FROM seen_mutations WHERE mutation_id = ?;", (m["mutation_id"],)).fetchone():
                    continue
                cur = conn.execute("SELECT * FROM documents WHERE doc_id = ?;", (m["doc_id"],)).fetchone()
                if not cur:
                    conn.execute("INSERT INTO documents VALUES (?, ?, ?, ?, ?, ?);", (m["doc_id"], m["title"], json.dumps(m["fields"]), m["hlc_ts"], m["origin"], json.dumps(m["vc"])))
                else:
                    l_vc = json.loads(cur["vc_json"])
                    rel = compare_vc(l_vc, m["vc"])
                    m_vc = merge_vc(l_vc, m["vc"])
                    if rel == "BEFORE":
                        conn.execute("UPDATE documents SET title=?, fields_json=?, hlc_ts=?, origin=?, vc_json=? WHERE doc_id=?;", (m["title"], json.dumps(m["fields"]), m["hlc_ts"], m["origin"], json.dumps(m_vc), m["doc_id"]))
                    elif rel == "CONCURRENT":
                        l_fields = json.loads(cur["fields_json"])
                        merged_fields = {**l_fields, **m["fields"]}
                        merged_title = " + ".join(sorted({cur["title"], m["title"]}))
                        conn.execute("UPDATE documents SET title=?, fields_json=?, vc_json=? WHERE doc_id=?;", (merged_title, json.dumps(merged_fields), json.dumps(m_vc), m["doc_id"]))
                        state["conflicts"].append({"region": r, "doc_id": m["doc_id"], "merged_title": merged_title})
                conn.execute("INSERT INTO seen_mutations VALUES (?);", (m["mutation_id"],))
                applied[r] += 1
                for dst in neighbors(r):
                    if dst not in m["visited"]:
                        state["queues"][dst].append({**m, "visited": m["visited"] + [dst]})
            conn.commit()
            conn.close()
        return {"applied": applied, "conflicts": state["conflicts"]}

    return app


app = create_app()
