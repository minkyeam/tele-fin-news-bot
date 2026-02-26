"""
database.py
───────────
SQLite 스키마 생성 및 CRUD 헬퍼 함수 모음.

테이블 구조
 - channels : 텔레그램 채널 메타정보
 - posts     : 채널 게시물
 - links     : URL + Authority Score + 클러스터 귀속
 - signals   : 클러스터링 결과 (마켓 시그널)
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import config

# ─── 스키마 ────────────────────────────────────────────────────────────────────

DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS channels (
    channel_id       TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    subscriber_count INTEGER DEFAULT 0,
    category         TEXT
);

CREATE TABLE IF NOT EXISTS posts (
    post_id    TEXT PRIMARY KEY,            -- "{channel_id}_{msg_id}"
    channel_id TEXT NOT NULL REFERENCES channels(channel_id),
    content    TEXT,
    views      INTEGER DEFAULT 0,
    timestamp  TEXT NOT NULL               -- ISO-8601
);

CREATE TABLE IF NOT EXISTS links (
    url_hash        TEXT PRIMARY KEY,       -- SHA-256(original_url)
    original_url    TEXT NOT NULL UNIQUE,
    title           TEXT,
    description     TEXT,
    authority_score REAL DEFAULT 0.0,
    cluster_id      TEXT REFERENCES signals(cluster_id),
    created_at      TEXT NOT NULL
);

-- post ↔ link 다대다 매핑 (Authority Score 계산에 필요)
CREATE TABLE IF NOT EXISTS post_links (
    post_id  TEXT NOT NULL REFERENCES posts(post_id),
    url_hash TEXT NOT NULL REFERENCES links(url_hash),
    PRIMARY KEY (post_id, url_hash)
);

CREATE TABLE IF NOT EXISTS signals (
    cluster_id            TEXT PRIMARY KEY,
    representative_title  TEXT,
    summary_text          TEXT,
    total_authority_score REAL DEFAULT 0.0,
    category              TEXT DEFAULT '기타',
    generated_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_posts_channel   ON posts(channel_id);
CREATE INDEX IF NOT EXISTS idx_posts_timestamp ON posts(timestamp);
CREATE INDEX IF NOT EXISTS idx_links_score     ON links(authority_score DESC);
CREATE INDEX IF NOT EXISTS idx_links_cluster   ON links(cluster_id);
"""


# ─── 연결 헬퍼 ────────────────────────────────────────────────────────────────

@contextmanager
def get_conn():
    conn = sqlite3.connect(config.DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """스키마를 생성합니다. 이미 존재하면 무시됩니다."""
    with get_conn() as conn:
        conn.executescript(DDL)
        # 기존 DB에 category 컬럼이 없으면 추가 (마이그레이션)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
        if "category" not in cols:
            conn.execute("ALTER TABLE signals ADD COLUMN category TEXT DEFAULT '기타'")
    print(f"[DB] 초기화 완료 → {config.DB_PATH}")


# ─── Channel CRUD ────────────────────────────────────────────────────────────

def upsert_channel(channel_id: str, name: str,
                   subscriber_count: int, category: str = "") -> None:
    sql = """
        INSERT INTO channels (channel_id, name, subscriber_count, category)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(channel_id) DO UPDATE SET
            name             = excluded.name,
            subscriber_count = excluded.subscriber_count,
            category         = excluded.category
    """
    with get_conn() as conn:
        conn.execute(sql, (channel_id, name, subscriber_count, category))


def get_channel(channel_id: str) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM channels WHERE channel_id = ?", (channel_id,)
        ).fetchone()


def get_all_channels() -> list[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM channels").fetchall()


# ─── Post CRUD ───────────────────────────────────────────────────────────────

def upsert_post(post_id: str, channel_id: str,
                content: str, views: int, timestamp: datetime) -> None:
    sql = """
        INSERT INTO posts (post_id, channel_id, content, views, timestamp)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(post_id) DO UPDATE SET
            views     = excluded.views,
            content   = excluded.content
    """
    with get_conn() as conn:
        conn.execute(sql, (
            post_id, channel_id, content, views,
            timestamp.isoformat()
        ))


# ─── Link CRUD ───────────────────────────────────────────────────────────────

def url_to_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def upsert_link(original_url: str, title: str = "",
                description: str = "") -> str:
    """링크를 삽입/무시하고 url_hash를 반환합니다."""
    h = url_to_hash(original_url)
    sql = """
        INSERT OR IGNORE INTO links (url_hash, original_url, title, description, created_at)
        VALUES (?, ?, ?, ?, ?)
    """
    with get_conn() as conn:
        conn.execute(sql, (h, original_url, title, description,
                           datetime.utcnow().isoformat()))
    return h


def get_post_texts_for_links(url_hashes: list[str], limit: int = 5) -> list[str]:
    """
    주어진 url_hash 목록에 연결된 포스트 본문을 조회수 순으로 반환합니다.
    요약 LLM 입력에 실제 텔레그램 메시지 내용을 포함시키기 위해 사용합니다.
    """
    if not url_hashes:
        return []
    placeholders = ",".join("?" * len(url_hashes))
    sql = f"""
        SELECT DISTINCT p.content, p.views
        FROM posts p
        JOIN post_links pl ON p.post_id = pl.post_id
        WHERE pl.url_hash IN ({placeholders})
          AND p.content IS NOT NULL
          AND p.content != ''
        ORDER BY p.views DESC
        LIMIT ?
    """
    with get_conn() as conn:
        rows = conn.execute(sql, (*url_hashes, limit)).fetchall()
    return [r["content"] for r in rows]


def link_post_link(post_id: str, url_hash: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO post_links (post_id, url_hash) VALUES (?, ?)",
            (post_id, url_hash)
        )


def update_link_score(url_hash: str, score: float) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE links SET authority_score = ? WHERE url_hash = ?",
            (score, url_hash)
        )


def update_link_metadata(url_hash: str, title: str, description: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE links SET title = ?, description = ? WHERE url_hash = ?",
            (title, description, url_hash)
        )


def assign_link_to_cluster(url_hash: str, cluster_id: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE links SET cluster_id = ? WHERE url_hash = ?",
            (cluster_id, url_hash)
        )


def get_top_links_by_score(top_percent: int = 20) -> list[sqlite3.Row]:
    """Authority Score 상위 N% 링크를 반환합니다."""
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM links").fetchone()[0]
        limit = max(1, int(total * top_percent / 100))
        return conn.execute(
            """
            SELECT * FROM links
            WHERE title IS NOT NULL AND title != ''
            ORDER BY authority_score DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()


def get_all_links() -> list[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM links").fetchall()


# ─── Signal CRUD ─────────────────────────────────────────────────────────────

def clear_signals() -> None:
    """이전 실행의 시그널을 모두 삭제하고 링크의 cluster_id를 초기화합니다."""
    with get_conn() as conn:
        conn.execute("DELETE FROM signals")
        conn.execute("UPDATE links SET cluster_id = NULL")

def upsert_signal(cluster_id: str, representative_title: str,
                  summary_text: str, total_authority_score: float) -> None:
    sql = """
        INSERT INTO signals
            (cluster_id, representative_title, summary_text,
             total_authority_score, generated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(cluster_id) DO UPDATE SET
            representative_title  = excluded.representative_title,
            summary_text          = excluded.summary_text,
            total_authority_score = excluded.total_authority_score,
            generated_at          = excluded.generated_at
    """
    with get_conn() as conn:
        conn.execute(sql, (
            cluster_id, representative_title, summary_text,
            total_authority_score, datetime.utcnow().isoformat()
        ))


def get_signals_with_links() -> list[dict]:
    """각 시그널과 연결된 링크 목록을 딕셔너리로 반환합니다."""
    with get_conn() as conn:
        signals = conn.execute(
            "SELECT * FROM signals ORDER BY total_authority_score DESC"
        ).fetchall()

        result = []
        for sig in signals:
            links = conn.execute(
                """
                SELECT url_hash, original_url, title, authority_score
                FROM links
                WHERE cluster_id = ?
                ORDER BY authority_score DESC
                """,
                (sig["cluster_id"],)
            ).fetchall()
            result.append({
                "signal": dict(sig),
                "links": [dict(l) for l in links]
            })
        return result
