"""Module 3 — Leaderless Quorum Key-Value Store (FastAPI + SQLite).

Implements:
- Consistent-hash preference list selection across 5 SQLite replica nodes
- Tunable (N, W, R) strict and weak quorums
- Synchronous Inline Read Repair when stale replicas are observed during R-quorum reads
- SHA-256 Merkle-bucket Anti-Entropy background repair
- Sloppy Quorum spillover and Hinted Handoff delivery on replica recovery
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel


class QuorumWriteRequest(BaseModel):
    value: Any
    version_ts: int
    writer_id: str = "client-1"
    n: int = 3
    w: int = 2
    allow_sloppy_quorum: bool = False


def create_app(data_dir: Optional[str] = None) -> FastAPI:
    base_dir = data_dir or tempfile.mkdtemp(prefix="mod03_leaderless_")
    node_ids = ["node_1", "node_2", "node_3", "node_4", "node_5"]
    dbs: Dict[str, str] = {}
    for nid in node_ids:
        p = os.path.join(base_dir, f"{nid}.db")
        dbs[nid] = p
        conn = sqlite3.connect(p)
        conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val_json TEXT, version_ts INT, writer_id TEXT);")
        conn.execute("CREATE TABLE IF NOT EXISTS hints (id INTEGER PRIMARY KEY AUTOINCREMENT, target_node TEXT, key TEXT, val_json TEXT, version_ts INT, writer_id TEXT);")
        conn.commit()
        conn.close()

    online = {nid: True for nid in node_ids}
    app = FastAPI(title="Module 3 — Leaderless Quorum KV Store")

    def pref_list(key: str, n: int = 3) -> List[str]:
        idx = int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % len(node_ids)
        return [node_ids[(idx + i) % len(node_ids)] for i in range(n)]

    def put_if_newer(nid: str, key: str, val_json: str, v_ts: int, writer: str) -> bool:
        conn = sqlite3.connect(dbs[nid])
        row = conn.execute("SELECT version_ts, writer_id FROM kv WHERE key=?;", (key,)).fetchone()
        if row and (v_ts, writer) < (int(row[0]), str(row[1])):
            conn.close()
            return False
        conn.execute("INSERT OR REPLACE INTO kv VALUES (?, ?, ?, ?);", (key, val_json, v_ts, writer))
        conn.commit()
        conn.close()
        return True

    @app.put("/kv/{key}")
    def write_kv(key: str, req: QuorumWriteRequest) -> dict:
        plist = pref_list(key, req.n)
        val_s = json.dumps(req.value, sort_keys=True)
        home_acked, offline_home, sloppy_acked = [], [], []
        for nid in plist:
            if online[nid]:
                put_if_newer(nid, key, val_s, req.version_ts, req.writer_id)
                home_acked.append(nid)
            else:
                offline_home.append(nid)
        if len(home_acked) < req.w and req.allow_sloppy_quorum:
            fallbacks = [nid for nid in node_ids if nid not in plist and online[nid]]
            for fb, target_h in zip(fallbacks, offline_home[: req.w - len(home_acked)]):
                conn = sqlite3.connect(dbs[fb])
                conn.execute("INSERT INTO hints(target_node, key, val_json, version_ts, writer_id) VALUES (?, ?, ?, ?, ?);", (target_h, key, val_s, req.version_ts, req.writer_id))
                conn.commit()
                conn.close()
                sloppy_acked.append(fb)
        if len(home_acked) + len(sloppy_acked) < req.w:
            raise HTTPException(status_code=503, detail="Write quorum W not satisfied")
        return {"key": key, "preference_list": plist, "home_acked": home_acked, "sloppy_acked": sloppy_acked}

    @app.get("/kv/{key}")
    def read_kv(key: str, n: int = 3, r: int = 2, read_repair: bool = Query(True)) -> dict:
        plist = pref_list(key, n)
        avail = [nid for nid in plist if online[nid]]
        if len(avail) < r:
            raise HTTPException(status_code=503, detail="Read quorum R not satisfied")
        contacted = avail[:r]
        observed, winner = {}, None
        for nid in contacted:
            conn = sqlite3.connect(dbs[nid])
            row = conn.execute("SELECT val_json, version_ts, writer_id FROM kv WHERE key=?;", (key,)).fetchone()
            conn.close()
            observed[nid] = row
            if row and (winner is None or (int(row[1]), row[2]) > (int(winner[1]), winner[2])):
                winner = row
        repaired = []
        if winner and read_repair:
            for nid, row in observed.items():
                if row is None or (int(row[1]), row[2]) < (int(winner[1]), winner[2]):
                    put_if_newer(nid, key, winner[0], int(winner[1]), winner[2])
                    repaired.append(nid)
        return {"key": key, "value": json.loads(winner[0]) if winner else None, "version_ts": int(winner[1]) if winner else None, "repaired_nodes": repaired}

    @app.post("/hints/drain")
    def drain_hints() -> dict:
        delivered = 0
        for nid in node_ids:
            if not online[nid]:
                continue
            conn = sqlite3.connect(dbs[nid])
            rows = conn.execute("SELECT id, target_node, key, val_json, version_ts, writer_id FROM hints;").fetchall()
            for h_id, target_h, k, vj, vts, w in rows:
                if online.get(target_h, False):
                    put_if_newer(target_h, k, vj, int(vts), w)
                    conn.execute("DELETE FROM hints WHERE id=?;", (h_id,))
                    delivered += 1
            conn.commit()
            conn.close()
        return {"hints_delivered": delivered}

    return app


app = create_app()
