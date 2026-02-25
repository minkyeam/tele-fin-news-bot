"""
clusterer.py
────────────
Authority Score 상위 N% 링크를 Gemini 임베딩으로 벡터화하고
DBSCAN으로 클러스터링한 뒤 결과를 DB에 반영합니다.

PRD Step 3
 - 임베딩: gemini-embedding-001
 - 클러스터링: DBSCAN (eps, min_samples ← config)
 - 노이즈(-1) 클러스터는 단독 시그널로 처리
 - 결과를 authority 상위 TOP_SIGNALS개로 절단
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

import numpy as np
from google import genai
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import normalize

import config
import database as db

_client = genai.Client(api_key=config.GEMINI_API_KEY)


@dataclass
class Cluster:
    cluster_id: str
    url_hashes: list[str] = field(default_factory=list)
    titles: list[str] = field(default_factory=list)
    descriptions: list[str] = field(default_factory=list)
    post_texts: list[str] = field(default_factory=list)   # 실제 텔레그램 메시지 본문
    total_authority_score: float = 0.0


def _embed_texts(texts: list[str]) -> np.ndarray:
    """배치 임베딩 — Gemini text-embedding-004 (768차원)."""
    batch_size = 100
    all_vecs = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        result = _client.models.embed_content(
            model=config.EMBEDDING_MODEL,
            contents=batch,
        )
        all_vecs.extend([emb.values for emb in result.embeddings])

    return np.array(all_vecs, dtype=np.float32)


def run_clustering(top_n: int | None = None) -> list[Cluster]:
    """
    1. 상위 N% 링크 로드
    2. title + description 임베딩
    3. DBSCAN 클러스터링
    4. DB 업데이트 (link.cluster_id)
    5. authority 상위 top_n개 Cluster 반환 (기본값: config.TOP_SIGNALS)
    """
    if top_n is None:
        top_n = config.TOP_SIGNALS

    links = db.get_top_links_by_score(config.CLUSTER_TOP_PERCENT)

    if not links:
        print("[Clusterer] 클러스터링할 링크가 없습니다.")
        return []

    print(f"[Clusterer] 대상 링크: {len(links)}개")

    # ── 텍스트 준비 ──────────────────────────────────────────────────────────
    texts = [
        f"{row['title']} {row['description']}".strip() or row["original_url"]
        for row in links
    ]

    # ── 임베딩 ───────────────────────────────────────────────────────────────
    print(f"[Clusterer] 임베딩 중... (model={config.EMBEDDING_MODEL})")
    vecs = _embed_texts(texts)
    vecs = normalize(vecs)  # 코사인 거리를 위해 L2 정규화

    # ── DBSCAN ───────────────────────────────────────────────────────────────
    db_scan = DBSCAN(
        eps=config.DBSCAN_EPS,
        min_samples=config.DBSCAN_MIN_SAMPLES,
        metric="cosine",
        algorithm="brute",
        n_jobs=-1
    )
    labels = db_scan.fit_predict(vecs)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise    = list(labels).count(-1)
    print(f"[Clusterer] 1차 클러스터: {n_clusters}개, 노이즈: {n_noise}개")

    # ── 클러스터 집계 ─────────────────────────────────────────────────────────
    cluster_map: dict[int, Cluster] = {}

    for idx, (link, label) in enumerate(zip(links, labels)):
        # 노이즈(-1)는 각각 독립 클러스터로 처리
        effective_label = label if label != -1 else -(idx + 1000)

        if effective_label not in cluster_map:
            cluster_map[effective_label] = Cluster(
                cluster_id=str(uuid.uuid4())
            )

        cluster = cluster_map[effective_label]
        cluster.url_hashes.append(link["url_hash"])
        cluster.titles.append(link["title"] or "")
        cluster.descriptions.append(link["description"] or "")
        cluster.total_authority_score += link["authority_score"] or 0.0

        db.assign_link_to_cluster(link["url_hash"], cluster.cluster_id)

    # ── authority 상위 top_n개 선택 ───────────────────────────────────────────
    clusters = sorted(
        cluster_map.values(),
        key=lambda c: c.total_authority_score,
        reverse=True
    )[:top_n]

    # ── 포스트 본문 로드 (LLM 요약에 사용) ───────────────────────────────────
    for cluster in clusters:
        cluster.post_texts = db.get_post_texts_for_links(cluster.url_hashes)

    print(f"[Clusterer] 완료 — 상위 {len(clusters)}개 클러스터 반환 (전체 {len(cluster_map)}개 중)")
    return clusters
