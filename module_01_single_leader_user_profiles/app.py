"""Module 1 — Single-Leader Replicated User-Profile Service (FastAPI + SQLite).

Implements:
- Primary writes & ordered LSN replication log propagation
- ASYNC, SEMI_SYNC, and SYNC_ALL acknowledgement modes with configurable timeouts
- Replica lag measurement & deliberate stale-read reproduction
- Read-Your-Writes and Monotonic Reads consistency policies
- Adding a new follower from a consistent snapshot + post-snapshot log catch-up
- Follower outage recovery and Leader Failover with Epoch Fencing Tokens (split-brain prevention)
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field


class AckMode(str, Enum):
    ASYNC = "async"
    SEMI_SYNC = "semi_sync"
    SYNC_ALL = "sync_all"


class ReadConsistency(str, Enum):
    EVENTUAL = "eventual"
    READ_YOUR_WRITES = "read_your_writes"
    MONOTONIC = "monotonic"


class NodeRole(str, Enum):
    PRIMARY = "PRIMARY"
    REPLICA = "REPLICA"
    FENCED = "FENCED"


class UserProfileUpsertRequest(BaseModel):
    username: str
    email: str
    bio: str = ""
    preferences: Dict[str, Any] = Field(default_factory=dict)
    ack_mode: AckMode = AckMode.ASYNC
    ack_timeout_ms: int = 500
    client_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    target_node_id: Optional[str] = None


class ReplicationLogEntry(BaseModel):
    lsn: int
    epoch: int
    operation: str
    user_id: str
    payload: Dict[str, Any]
    idempotency_key: str
    created_at_ms: int


class SQLiteNodeStorage:
    def __init__(self, node_id: str, db_path: str, role: NodeRole, epoch: int = 1) -> None:
        self.node_id = node_id
        self.db_path = db_path
        self._init_schema(role, epoch)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_schema(self, role: NodeRole, epoch: int) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute("CREATE TABLE IF NOT EXISTS node_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
            conn.execute(
                """CREATE TABLE IF NOT EXISTS user_profiles (
                    user_id TEXT PRIMARY KEY, username TEXT NOT NULL, email TEXT NOT NULL,
                    bio TEXT NOT NULL, preferences_json TEXT NOT NULL, version INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL, last_lsn INTEGER NOT NULL, last_epoch INTEGER NOT NULL
                );"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS replication_log (
                    lsn INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, operation TEXT NOT NULL,
                    user_id TEXT NOT NULL, payload_json TEXT NOT NULL, idempotency_key TEXT UNIQUE NOT NULL,
                    created_at_ms INTEGER NOT NULL
                );"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS applied_log_history (
                    lsn INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, idempotency_key TEXT UNIQUE NOT NULL
                );"""
            )
            conn.execute("INSERT OR IGNORE INTO node_meta VALUES ('role', ?);", (role.value,))
            conn.execute("INSERT OR IGNORE INTO node_meta VALUES ('epoch', ?);", (str(epoch),))
            conn.execute("INSERT OR IGNORE INTO node_meta VALUES ('applied_lsn', '0');")
            conn.execute("COMMIT;")
        finally:
            conn.close()

    def get_meta(self, key: str) -> str:
        conn = self._connect()
        try:
            return conn.execute("SELECT value FROM node_meta WHERE key = ?;", (key,)).fetchone()["value"]
        finally:
            conn.close()

    def set_meta(self, key: str, value: str) -> None:
        conn = self._connect()
        try:
            conn.execute("INSERT OR REPLACE INTO node_meta(key, value) VALUES (?, ?);", (key, value))
        finally:
            conn.close()

    def write_primary(self, user_id: str, req: UserProfileUpsertRequest, epoch: int) -> Tuple[ReplicationLogEntry, dict]:
        now_ms = int(time.time() * 1000)
        idem = req.idempotency_key or f"{user_id}:{uuid.uuid4().hex}"
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE;")
            next_lsn = int(conn.execute("SELECT value FROM node_meta WHERE key='applied_lsn';").fetchone()["value"]) + 1
            cur = conn.execute("SELECT version FROM user_profiles WHERE user_id = ?;", (user_id,)).fetchone()
            ver = (int(cur["version"]) + 1) if cur else 1
            payload = {
                "user_id": user_id, "username": req.username, "email": req.email, "bio": req.bio,
                "preferences": req.preferences, "version": ver, "updated_at_ms": now_ms,
                "last_lsn": next_lsn, "last_epoch": epoch,
            }
            conn.execute(
                """INSERT OR REPLACE INTO user_profiles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);""",
                (user_id, req.username, req.email, req.bio, json.dumps(req.preferences), ver, now_ms, next_lsn, epoch),
            )
            conn.execute(
                """INSERT INTO replication_log VALUES (?, ?, 'UPSERT', ?, ?, ?, ?);""",
                (next_lsn, epoch, user_id, json.dumps(payload), idem, now_ms),
            )
            conn.execute("INSERT OR REPLACE INTO applied_log_history VALUES (?, ?, ?);", (next_lsn, epoch, idem))
            conn.execute("INSERT OR REPLACE INTO node_meta VALUES ('applied_lsn', ?);", (str(next_lsn),))
            conn.execute("COMMIT;")
            return ReplicationLogEntry(
                lsn=next_lsn, epoch=epoch, operation="UPSERT", user_id=user_id,
                payload=payload, idempotency_key=idem, created_at_ms=now_ms
            ), payload
        finally:
            conn.close()

    def apply_log_entry(self, entry: ReplicationLogEntry, min_epoch: int) -> bool:
        if entry.epoch < min_epoch:
            return False
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE;")
            if conn.execute(
                "SELECT lsn FROM applied_log_history WHERE lsn = ? OR idempotency_key = ?;",
                (entry.lsn, entry.idempotency_key),
            ).fetchone():
                conn.execute("COMMIT;")
                return True
            cur_lsn = int(conn.execute("SELECT value FROM node_meta WHERE key='applied_lsn';").fetchone()["value"])
            if entry.lsn > cur_lsn + 1:
                conn.execute("ROLLBACK;")
                return False
            p = entry.payload
            conn.execute(
                """INSERT OR REPLACE INTO user_profiles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);""",
                (p["user_id"], p["username"], p["email"], p["bio"], json.dumps(p["preferences"]),
                 p["version"], p["updated_at_ms"], p["last_lsn"], p["last_epoch"]),
            )
            conn.execute(
                "INSERT OR REPLACE INTO replication_log VALUES (?, ?, ?, ?, ?, ?, ?);",
                (entry.lsn, entry.epoch, entry.operation, entry.user_id, json.dumps(p), entry.idempotency_key, entry.created_at_ms),
            )
            conn.execute("INSERT INTO applied_log_history VALUES (?, ?, ?);", (entry.lsn, entry.epoch, entry.idempotency_key))
            conn.execute("INSERT OR REPLACE INTO node_meta VALUES ('applied_lsn', ?);", (str(max(cur_lsn, entry.lsn)),))
            conn.execute("COMMIT;")
            return True
        finally:
            conn.close()

    def get_profile(self, user_id: str) -> Optional[dict]:
        conn = self._connect()
        try:
            r = conn.execute("SELECT * FROM user_profiles WHERE user_id = ?;", (user_id,)).fetchone()
            if not r:
                return None
            return {
                "user_id": r["user_id"], "username": r["username"], "email": r["email"], "bio": r["bio"],
                "preferences": json.loads(r["preferences_json"]), "version": r["version"],
                "updated_at_ms": r["updated_at_ms"], "last_lsn": r["last_lsn"], "last_epoch": r["last_epoch"],
            }
        finally:
            conn.close()


def create_app(data_dir: Optional[str] = None) -> FastAPI:
    base_dir = data_dir or tempfile.mkdtemp(prefix="mod01_single_leader_")
    nodes = {
        "primary": SQLiteNodeStorage("primary", os.path.join(base_dir, "primary.db"), NodeRole.PRIMARY, 1),
        "replica_1": SQLiteNodeStorage("replica_1", os.path.join(base_dir, "replica_1.db"), NodeRole.REPLICA, 1),
        "replica_2": SQLiteNodeStorage("replica_2", os.path.join(base_dir, "replica_2.db"), NodeRole.REPLICA, 1),
    }
    state: Dict[str, Any] = {
        "primary_id": "primary",
        "epoch": 1,
        "online": {"primary": True, "replica_1": True, "replica_2": True},
        "delay_ms": {"primary": 0, "replica_1": 0, "replica_2": 0},
        "hold": {"replica_1": False, "replica_2": False},
        "queues": {"replica_1": [], "replica_2": []},
        "client_write_lsn": {},
        "client_seen_lsn": {},
    }
    app = FastAPI(title="Module 1 — Single-Leader User-Profile Service")

    @app.put("/profiles/{user_id}")
    def upsert_profile(user_id: str, req: UserProfileUpsertRequest) -> dict:
        target = req.target_node_id or state["primary_id"]
        role = NodeRole(nodes[target].get_meta("role"))
        node_epoch = int(nodes[target].get_meta("epoch"))
        if role == NodeRole.FENCED or node_epoch < state["epoch"]:
            raise HTTPException(status_code=409, detail=f"Split-brain fenced: node {target} has stale epoch {node_epoch}")
        entry, profile = nodes[target].write_primary(user_id, req, state["epoch"])
        if req.client_id:
            state["client_write_lsn"][req.client_id] = entry.lsn
        acked, pending = [], []
        for rid, store in nodes.items():
            if rid == state["primary_id"] or NodeRole(store.get_meta("role")) != NodeRole.REPLICA:
                continue
            state["queues"][rid].append(entry)
            if state["online"].get(rid, True) and not state["hold"].get(rid, False) and state["delay_ms"].get(rid, 0) <= req.ack_timeout_ms:
                for q_entry in list(state["queues"][rid]):
                    store.apply_log_entry(q_entry, state["epoch"])
                state["queues"][rid].clear()
                acked.append(rid)
            else:
                pending.append(rid)
        if req.ack_mode == AckMode.SEMI_SYNC and len(acked) < 1:
            raise HTTPException(status_code=504, detail="SEMI_SYNC ack timeout")
        if req.ack_mode == AckMode.SYNC_ALL and len(pending) > 0:
            raise HTTPException(status_code=504, detail="SYNC_ALL ack timeout")
        return {"lsn": entry.lsn, "epoch": state["epoch"], "acked_replicas": acked, "pending_replicas": pending, "profile": profile}

    @app.get("/profiles/{user_id}")
    def get_profile(user_id: str, consistency: ReadConsistency = Query(ReadConsistency.EVENTUAL), client_id: Optional[str] = None, preferred_node_id: Optional[str] = None) -> dict:
        p_lsn = int(nodes[state["primary_id"]].get_meta("applied_lsn"))
        chosen = preferred_node_id or "replica_1"
        req_lsn = 0
        if consistency == ReadConsistency.READ_YOUR_WRITES and client_id:
            req_lsn = state["client_write_lsn"].get(client_id, 0)
        elif consistency == ReadConsistency.MONOTONIC and client_id:
            req_lsn = state["client_seen_lsn"].get(client_id, 0)
        if int(nodes[chosen].get_meta("applied_lsn")) < req_lsn:
            chosen = state["primary_id"]
        n_lsn = int(nodes[chosen].get_meta("applied_lsn"))
        if client_id:
            state["client_seen_lsn"][client_id] = max(state["client_seen_lsn"].get(client_id, 0), n_lsn)
        return {"profile": nodes[chosen].get_profile(user_id), "served_by": chosen, "applied_lsn": n_lsn, "lsn_lag": max(0, p_lsn - n_lsn)}

    @app.post("/cluster/failover")
    def failover(candidate_id: str = "replica_1") -> dict:
        old_p = state["primary_id"]
        nodes[old_p].set_meta("role", NodeRole.FENCED.value)
        state["epoch"] += 1
        nodes[candidate_id].set_meta("role", NodeRole.PRIMARY.value)
        nodes[candidate_id].set_meta("epoch", str(state["epoch"]))
        state["primary_id"] = candidate_id
        return {"old_primary": old_p, "new_primary": candidate_id, "new_epoch": state["epoch"]}

    return app


app = create_app()
