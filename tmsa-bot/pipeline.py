"""
pipeline.py
───────────
전체 TMSA 파이프라인을 순서대로 실행하는 오케스트레이터.

실행 순서
 Step 1. collect       — Telegram 채널 메시지 수집 + URL 추출 + 메타데이터 크롤링
 Step 2a. score_links  — URL Authority Score 계산
 Step 2b. score_posts  — 포스트 Authority Score 계산 (URL 어소리티를 보조값으로 활용)
 Step 3. cluster       — 모든 포스트 통합 임베딩 + DBSCAN → 상위 15개
 Step 4. summarize     — LLM 요약 + Signal 테이블 저장
 Step 5. display       — 터미널 출력
"""

from __future__ import annotations

import asyncio
import time

import collector
import scorer
import clusterer
import summarizer
import config


async def run_pipeline(
    channels: list[str] | None = None,
    use_subscribed: bool = False,
) -> None:
    t0 = time.perf_counter()

    # ── Step 1: 수집 ─────────────────────────────────────────────────────────
    print("\n── Step 1/4: Telegram 수집 ──────────────────────────────────────")
    await collector.collect(channels, use_subscribed=use_subscribed)

    # ── Step 2a: URL 스코어링 ─────────────────────────────────────────────────
    print("\n── Step 2a: URL Authority Score 계산 ────────────────────────────")
    scorer.run_scoring()

    # ── Step 2b: 포스트 스코어링 ──────────────────────────────────────────────
    print("\n── Step 2b: 포스트 Authority Score 계산 (URL 보조 반영) ─────────")
    scorer.run_post_scoring()

    # ── Step 3: 통합 클러스터링 ───────────────────────────────────────────────
    print("\n── Step 3/4: 통합 포스트 임베딩 + DBSCAN 클러스터링 ────────────")
    clusters = clusterer.run_unified_clustering(top_n=config.TOP_SIGNALS)

    # ── Step 4: 요약 ─────────────────────────────────────────────────────────
    print("\n── Step 4/4: LLM 요약 + Signal 저장 ────────────────────────────")
    summarizer.run_summarization(clusters)

    # ── 결과 출력 ─────────────────────────────────────────────────────────────
    summarizer.print_signals()

    elapsed = time.perf_counter() - t0
    print(f"\n✅ 파이프라인 완료 ({elapsed:.1f}s)")
    print("   봇 전송: python main.py --send")
