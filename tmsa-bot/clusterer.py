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
from google.genai.errors import ClientError
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import normalize

import config
import database as db

_client = genai.Client(api_key=config.GEMINI_API_KEY)
_local_embedder = None   # sentence-transformers는 필요 시 lazy load


@dataclass
class Cluster:
    cluster_id: str
    url_hashes: list[str] = field(default_factory=list)
    titles: list[str] = field(default_factory=list)
    descriptions: list[str] = field(default_factory=list)
    post_texts: list[str] = field(default_factory=list)   # 실제 텔레그램 메시지 본문
    post_ids: list[str] = field(default_factory=list)     # 텍스트 전용 클러스터에서 t.me 링크 생성에 사용
    channel_ids: list[str] = field(default_factory=list)  # 텍스트 전용 클러스터 채널 추적
    total_authority_score: float = 0.0


def _embed_gemini(model: str, texts: list[str]) -> np.ndarray:
    """Gemini API 임베딩. 429 시 ClientError 발생."""
    batch_size = 100
    all_vecs = []
    for i in range(0, len(texts), batch_size):
        result = _client.models.embed_content(
            model=model, contents=texts[i : i + batch_size]
        )
        all_vecs.extend([emb.values for emb in result.embeddings])
    return np.array(all_vecs, dtype=np.float32)


def _embed_local(model_name: str, texts: list[str]) -> np.ndarray:
    """sentence-transformers 로컬 임베딩 (API 불필요)."""
    global _local_embedder
    from sentence_transformers import SentenceTransformer
    if _local_embedder is None or _local_embedder.model_card_data.model_name != model_name:
        print(f"  [로컬 임베딩] 모델 로드 중: {model_name} (최초 1회만)")
        _local_embedder = SentenceTransformer(model_name)
    vecs = _local_embedder.encode(texts, batch_size=64, show_progress_bar=False)
    return np.array(vecs, dtype=np.float32)


def _embed_texts(texts: list[str]) -> np.ndarray:
    """
    임베딩 fallback 체인:
      1. Gemini API 모델들 순서대로 시도
      2. 429 소진 시 로컬 sentence-transformers로 자동 전환
    """
    for model in config.EMBEDDING_MODEL_FALLBACKS:
        is_local = not model.startswith("gemini")
        try:
            if is_local:
                vecs = _embed_local(model, texts)
            else:
                vecs = _embed_gemini(model, texts)
            if model != config.EMBEDDING_MODEL:
                print(f"  (임베딩 fallback 성공: {model})")
            return vecs
        except ClientError as e:
            err = str(e)
            if err.startswith("401") or err.startswith("403"):
                print(f"  [!] {model} 임베딩 인증 오류 — API 키 확인 필요")
                break  # 인증 오류는 다른 API 모델도 동일하게 실패
            elif "429" in err or "RESOURCE_EXHAUSTED" in err:
                print(f"  [429] {model} 임베딩 한도 초과 → 다음으로 즉시 fallback")
            else:
                print(f"  [!] {model} 임베딩 오류: {err[:80]} → 다음으로 fallback")
        except Exception as e:
            print(f"  [!] {model} 임베딩 오류: {e}")
            if is_local:
                break  # 로컬 모델 오류는 더 이상 fallback 없음

    raise RuntimeError("모든 임베딩 모델 실패 — 클러스터링 불가")


def run_unified_clustering(top_n: int = 15) -> list[Cluster]:
    """
    모든 포스트를 대상으로 통합 임베딩 + DBSCAN 클러스터링.

    - 포스트 본문을 임베딩 텍스트로 사용 (URL 있든 없든 동일하게 처리)
    - 클러스터 점수 = Σ(포스트 Authority Score)  ← 메시지가 주인공
    - URL Authority Score는 포스트 점수에 이미 반영된 보조값
    - 노이즈(-1) 포스트는 제외 (단독 포스트는 트렌드 아님)
    - top_n개 반환 (기본 15)
    """
    db.clear_signals()

    rows = db.get_posts_for_clustering(
        min_length=30, collect_hours=config.COLLECT_HOURS
    )
    if not rows:
        print("[Clusterer] 클러스터링할 포스트가 없습니다.")
        return []

    print(f"[Clusterer] 대상 포스트: {len(rows)}개")

    # ── 임베딩 텍스트 준비 (포스트 본문 500자 절단) ──────────────────────────
    texts = [row["content"][:500] for row in rows]

    # ── 임베딩 ───────────────────────────────────────────────────────────────
    print(f"[Clusterer] 임베딩 중... (model={config.EMBEDDING_MODEL})")
    vecs = _embed_texts(texts)
    vecs = normalize(vecs)

    # ── DBSCAN ───────────────────────────────────────────────────────────────
    db_scan = DBSCAN(
        eps=config.DBSCAN_EPS,
        min_samples=config.DBSCAN_MIN_SAMPLES,
        metric="cosine",
        algorithm="brute",
        n_jobs=-1,
    )
    labels = db_scan.fit_predict(vecs)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise    = list(labels).count(-1)
    print(f"[Clusterer] 클러스터: {n_clusters}개, 노이즈(제외): {n_noise}개")

    # ── 클러스터 집계 ─────────────────────────────────────────────────────────
    cluster_map: dict[int, Cluster] = {}

    for row, label in zip(rows, labels):
        if label == -1:
            continue  # 노이즈 제외

        if label not in cluster_map:
            cluster_map[label] = Cluster(cluster_id=str(uuid.uuid4()))

        cluster = cluster_map[label]
        cluster.post_texts.append(row["content"])
        cluster.post_ids.append(row["post_id"])
        cluster.channel_ids.append(row["channel_id"])
        cluster.total_authority_score += row["authority_score"] or 0.0

        # 이 포스트에 연결된 URL 해시 수집 (중복 제거)
        if row["url_hashes_raw"]:
            for h in row["url_hashes_raw"].split("|"):
                if h and h not in cluster.url_hashes:
                    cluster.url_hashes.append(h)

    # ── authority 상위 top_n 선택 ─────────────────────────────────────────────
    clusters = sorted(
        cluster_map.values(),
        key=lambda c: c.total_authority_score,
        reverse=True,
    )[:top_n]

    # ── URL 메타데이터 + cluster_id 할당 ─────────────────────────────────────
    for cluster in clusters:
        if cluster.url_hashes:
            meta = db.get_links_metadata(cluster.url_hashes)
            cluster.titles       = [meta[h]["title"] or ""       for h in cluster.url_hashes if h in meta]
            cluster.descriptions = [meta[h]["description"] or "" for h in cluster.url_hashes if h in meta]
            for h in cluster.url_hashes:
                db.assign_link_to_cluster(h, cluster.cluster_id)

    print(f"[Clusterer] 완료 — 상위 {len(clusters)}개 반환 (전체 {len(cluster_map)}개 클러스터 중)")
    return clusters
