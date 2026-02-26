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

    db.clear_signals()  # 이전 클러스터링/시그널 초기화
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


def run_text_clustering(top_n: int = 5) -> list[Cluster]:
    """
    URL이 없는 포스트들을 임베딩 + DBSCAN으로 클러스터링합니다.
    여러 채널에서 동시에 등장하는 바이럴 텍스트 시그널을 탐지합니다.

    조건:
      - URL 링크가 없는 포스트
      - 최소 50자 이상의 내용
      - 2개 이상의 서로 다른 채널에서 등장
    """
    rows = db.get_posts_without_links(min_length=50, collect_hours=config.COLLECT_HOURS)
    if not rows:
        print("[Clusterer-Text] 텍스트 전용 포스트 없음")
        return []

    print(f"[Clusterer-Text] 텍스트 전용 포스트: {len(rows)}개")

    texts = [row["content"][:500] for row in rows]  # 500자 절단으로 임베딩 품질 유지

    vecs = _embed_texts(texts)
    vecs = normalize(vecs)

    db_scan = DBSCAN(
        eps=config.DBSCAN_EPS,
        min_samples=config.DBSCAN_MIN_SAMPLES,
        metric="cosine",
        algorithm="brute",
        n_jobs=-1,
    )
    labels = db_scan.fit_predict(vecs)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    print(f"[Clusterer-Text] 1차 클러스터: {n_clusters}개 (노이즈 제외)")

    cluster_map: dict[int, Cluster] = {}

    for idx, (row, label) in enumerate(zip(rows, labels)):
        if label == -1:
            continue  # 단독 포스트는 바이럴 아님 — 제외

        if label not in cluster_map:
            cluster_map[label] = Cluster(cluster_id=str(uuid.uuid4()))

        cluster = cluster_map[label]
        cluster.post_texts.append(row["content"])
        cluster.post_ids.append(row["post_id"])
        cluster.channel_ids.append(row["channel_id"])
        cluster.total_authority_score += row["views"] or 0.0

    # 바이럴 조건: 2개 이상의 서로 다른 채널에서 등장
    viral = [
        c for c in cluster_map.values()
        if len(set(c.channel_ids)) >= 2
    ]

    result = sorted(viral, key=lambda c: c.total_authority_score, reverse=True)[:top_n]
    print(f"[Clusterer-Text] 바이럴 클러스터 {len(result)}개 반환 (전체 {len(cluster_map)}개 중)")
    return result
