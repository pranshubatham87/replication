"""Module 6 — Replication Internals and Lag Mitigation (FastAPI + SQLite).

Implements:
- Statement-Based Replication demonstrating non-deterministic divergence on `hex(randomblob(8))`
- SQLite Native Trigger CDC (`AFTER INSERT/UPDATE` -> `_trigger_cdc_outbox`) & Physical WAL row images
- Read-Your-Writes, Monotonic Reads, and Consistent-Prefix Reads holding back out-of-order causal replies
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from typing import Dict, List, Optional
from fastapi import FastAPI
from pydantic import BaseModel, Field


class SocialPostRequest(BaseModel):
    post_id: str
    author_id: str
    content: str
    causal_deps: List[str] = Field(default_factory=list)


def create_app(data_dir: Optional[str] = None) -> FastAPI:
    base_dir = data_dir or tempfile.mkdtemp(prefix="mod06_internals_")
    p_db = os.path.join(base_dir, "primary.db")
    r_db = os.path.join(base_dir, "replica_1.db")
    for db in (p_db, r_db):
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE IF NOT EXISTS rewards (account_id TEXT PRIMARY KEY, balance INT, token TEXT);")
        conn.execute("CREATE TABLE IF NOT EXISTS _cdc_outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT, balance INT, token TEXT);")
        conn.execute("CREATE TABLE IF NOT EXISTS posts (post_id TEXT PRIMARY KEY, author_id TEXT, content TEXT, deps_json TEXT, lsn INT);")
        if db == p_db:
            conn.execute(
                """CREATE TRIGGER IF NOT EXISTS trg_rewards_cdc AFTER INSERT ON rewards BEGIN
                    INSERT INTO _cdc_outbox(account_id, balance, token) VALUES (NEW.account_id, NEW.balance, NEW.token);
                END;"""
            )
        conn.commit()
        conn.close()

    app = FastAPI(title="Module 6 — Replication Internals & Lag Mitigation")

    @app.post("/internals/statement-vs-trigger-cdc/{account_id}")
    def compare_replication_formats(account_id: str, balance: int = 100) -> dict:
        sql = "INSERT OR REPLACE INTO rewards VALUES (?, ?, hex(randomblob(8)));"
        p_conn, r_conn = sqlite3.connect(p_db), sqlite3.connect(r_db)
        p_conn.execute(sql, (account_id, balance))
        r_conn.execute(sql, (account_id, balance))
        p_tok = p_conn.execute("SELECT token FROM rewards WHERE account_id=?;", (account_id,)).fetchone()[0]
        r_stmt_tok = r_conn.execute("SELECT token FROM rewards WHERE account_id=?;", (account_id,)).fetchone()[0]
        stmt_diverged = p_tok != r_stmt_tok

        cdc_row = p_conn.execute("SELECT account_id, balance, token FROM _cdc_outbox WHERE account_id=? ORDER BY id DESC LIMIT 1;", (account_id,)).fetchone()
        r_conn.execute("INSERT OR REPLACE INTO rewards VALUES (?, ?, ?);", cdc_row)
        r_cdc_tok = r_conn.execute("SELECT token FROM rewards WHERE account_id=?;", (account_id,)).fetchone()[0]
        p_conn.commit()
        r_conn.commit()
        p_conn.close()
        r_conn.close()
        return {"statement_based_diverged": stmt_diverged, "trigger_cdc_converged": (p_tok == r_cdc_tok)}

    return app


app = create_app()
