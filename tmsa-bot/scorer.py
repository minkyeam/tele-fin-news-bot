"""
scorer.py
─────────
PRD 3.1에 정의된 Authority Score를 계산하고 Link 테이블을 업데이트합니다.

공식:
    Authority(L) = Σ_{c∈C_L} ( w1 * log(S_c) + w2 * V_{c,L} / S_c )

변수
 - C_L  : 링크 L을 공유한 채널 집합
 - S_c  : 채널 c의 구독자 수 (0이면 1로 보정)
 - V_c,L: 채널 c에서 해당 링크가 포함된 포스트의 조회수
 - w1   : config.AUTHORITY_W1  (구독자 규모 가중치)
 - w2   : config.AUTHORITY_W2  (조회수 비율 가중치)
"""

from __future__ import annotations

import math
import sqlite3
from typing import NamedTuple

import config
import database as db


class LinkScoreResult(NamedTuple):
    url_hash: str
    original_url: str
    score: float


def _fetch_link_channel_data() -> dict[str, list[tuple[int, int]]]:
    """
    각 url_hash에 대해 (subscriber_count, views) 쌍의 리스트를 반환합니다.
    즉, 해당 링크를 포함한 채널 × 포스트 조합을 모두 가져옵니다.
    """
    sql = """
        SELECT
            pl.url_hash,
            ch.subscriber_count,
            p.views
        FROM post_links pl
        JOIN posts    p  ON pl.post_id    = p.post_id
        JOIN channels ch ON p.channel_id  = ch.channel_id
    """
    result: dict[str, list[tuple[int, int]]] = {}
    with db.get_conn() as conn:
        for row in conn.execute(sql).fetchall():
            url_hash  = row["url_hash"]
            sub_count = max(row["subscriber_count"], 1)  # log(0) 방지
            views     = row["views"] or 0
            result.setdefault(url_hash, []).append((sub_count, views))
    return result


def compute_authority(
    channel_data: list[tuple[int, int]],
    w1: float = config.AUTHORITY_W1,
    w2: float = config.AUTHORITY_W2,
) -> float:
    """
    단일 링크에 대한 Authority Score를 계산합니다.

    channel_data: [(S_c, V_{c,L}), ...]
    """
    total = 0.0
    for S_c, V_c_L in channel_data:
        total += w1 * math.log(S_c) + w2 * (V_c_L / S_c)
    return round(total, 6)


def run_scoring() -> list[LinkScoreResult]:
    """
    DB에서 데이터를 읽어 모든 링크의 Authority Score를 계산하고
    Link 테이블을 업데이트합니다.

    Returns: 스코어링된 링크 목록 (score 내림차순 정렬)
    """
    link_channel_map = _fetch_link_channel_data()

    if not link_channel_map:
        print("[Scorer] 스코어링할 링크가 없습니다.")
        return []

    results: list[LinkScoreResult] = []

    with db.get_conn() as conn:
        all_links = conn.execute(
            "SELECT url_hash, original_url FROM links"
        ).fetchall()

    for link in all_links:
        url_hash = link["url_hash"]
        channel_data = link_channel_map.get(url_hash, [])

        if not channel_data:
            # post_links에 기록이 없는 링크는 0점
            score = 0.0
        else:
            score = compute_authority(channel_data)

        db.update_link_score(url_hash, score)
        results.append(LinkScoreResult(
            url_hash=url_hash,
            original_url=link["original_url"],
            score=score
        ))

    results.sort(key=lambda r: r.score, reverse=True)

    print(f"[Scorer] {len(results)}개 링크 스코어링 완료.")
    if results:
        top3 = results[:3]
        for r in top3:
            print(f"  ▶ {r.score:.4f}  {r.original_url[:80]}")

    return results


def run_post_scoring() -> None:
    """
    포스트 단위 Authority Score를 계산하고 posts 테이블을 업데이트합니다.

    공식:
        Post_Auth(P) = w1·log(Sc) + w2·Vp/Sc   ← 메시지 기본 점수
                     + POST_URL_WEIGHT · max_url_auth(P)  ← URL 어소리티 보조

    - Sc           : 채널 구독자 수
    - Vp           : 포스트 조회수
    - max_url_auth : 포스트에 포함된 URL 중 최고 Authority Score
                     (URL 없으면 0 — URL은 보조값이므로 없어도 무방)
    """
    sql = """
        SELECT
            p.post_id,
            p.views,
            c.subscriber_count,
            COALESCE(MAX(l.authority_score), 0) AS max_url_auth
        FROM posts p
        JOIN channels c ON p.channel_id = c.channel_id
        LEFT JOIN post_links pl ON p.post_id  = pl.post_id
        LEFT JOIN links      l  ON pl.url_hash = l.url_hash
        GROUP BY p.post_id
    """
    with db.get_conn() as conn:
        rows = conn.execute(sql).fetchall()

    if not rows:
        print("[Scorer] 포스트 스코어링할 데이터가 없습니다.")
        return

    for row in rows:
        Sc       = max(row["subscriber_count"] or 1, 1)
        Vp       = row["views"] or 0
        url_auth = row["max_url_auth"] or 0.0

        base  = config.AUTHORITY_W1 * math.log(Sc) + config.AUTHORITY_W2 * Vp / Sc
        score = base + config.POST_URL_WEIGHT * url_auth

        db.update_post_score(row["post_id"], round(score, 6))

    print(f"[Scorer] 포스트 Authority Score 업데이트: {len(rows)}개")
