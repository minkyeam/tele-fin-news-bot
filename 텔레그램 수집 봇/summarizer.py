"""
summarizer.py
─────────────
클러스터링된 링크 묶음을 GPT-4o-mini에 전달하여
마켓 시그널 요약을 생성하고 Signal 테이블에 저장합니다.

출력 형식 (PRD 3.3)
 - 시그널 제목: 15자 이내
 - 핵심 요약: Bullet point 3줄
 - DB 저장: Signal 테이블 (cluster_id, representative_title, summary_text, total_authority_score)
"""

from __future__ import annotations

import re
import time

from google import genai
from google.genai import types
from google.genai.errors import ClientError

import config
import database as db
from clusterer import Cluster

_client = genai.Client(api_key=config.GEMINI_API_KEY)

_SYSTEM_PROMPT = """당신은 금융·블록체인·DeFi 시장 전문 애널리스트입니다.
여러 텔레그램 채널에서 동시에 주목받은 기사와 메시지 묶음을 분석하여
핵심 마켓 시그널을 추출합니다.

출력 형식 (반드시 준수):
제목: [15자 이내 한국어 시그널 제목]
요약: [3문장 이내의 한국어 산문 요약]

규칙:
- 제목은 반드시 "제목: "으로 시작
- 요약은 반드시 "요약: "으로 시작
- 요약은 최대 3문장, 구체적 수치·프로젝트명·시장 영향 포함
- 광고·노이즈·일상 잡담은 완전히 무시
- 텔레그램 메시지 원문(=커뮤니티 반응)과 기사 내용을 함께 고려"""


def _build_user_message(cluster: Cluster) -> str:
    # 기사 목록
    articles = []
    for i, (title, desc) in enumerate(
        zip(cluster.titles, cluster.descriptions), start=1
    ):
        if title or desc:
            articles.append(f"{i}. 제목: {title}\n   설명: {desc}")
    article_block = "\n\n".join(articles) if articles else "(기사 없음)"

    # 텔레그램 포스트 본문 (상위 5개, 각 200자 절단)
    post_block = ""
    if cluster.post_texts:
        posts = [f"- {t[:200].strip()}" for t in cluster.post_texts[:5]]
        post_block = "\n=== 텔레그램 메시지 원문 ===\n" + "\n".join(posts)

    return (
        f"Authority Score 합계: {cluster.total_authority_score:.2f}\n"
        f"관련 기사: {len(cluster.url_hashes)}개\n"
        f"\n=== 기사 목록 ===\n{article_block}"
        f"{post_block}"
    )


def _parse_response(text: str) -> tuple[str, str]:
    """LLM 응답에서 제목과 요약 텍스트를 파싱합니다."""
    title = ""
    summary = ""

    for line in text.strip().splitlines():
        line = line.strip()
        if line.startswith("제목:"):
            title = line[3:].strip()[:15]
        elif line.startswith("요약:"):
            summary = line[3:].strip()

    # 요약이 여러 줄에 걸쳐 있을 경우 처리
    if not summary:
        lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
        body = [l for l in lines if not l.startswith("제목:")]
        summary = " ".join(body)[:400]

    return title or "시그널", summary


def _extract_retry_delay(error: ClientError) -> float:
    """429 응답에서 retry delay(초)를 추출합니다. 없으면 기본 60초."""
    match = re.search(r"retry[^0-9]*([0-9]+(?:\.[0-9]+)?)\s*s", str(error), re.I)
    return float(match.group(1)) + 1.0 if match else 60.0


def _call_model(model: str, user_msg: str) -> str:
    """단일 모델 호출. 성공 시 응답 텍스트, 429면 None 반환, 기타 오류는 예외."""
    resp = _client.models.generate_content(
        model=model,
        contents=user_msg,
        config=types.GenerateContentConfig(
            system_instruction=_SYSTEM_PROMPT,
            temperature=0.3,
            max_output_tokens=400,
        ),
    )
    return resp.text or ""


def summarize_cluster(cluster: Cluster) -> tuple[str, str]:
    """
    모델 체인(config.CHAT_MODEL_FALLBACKS)을 순서대로 시도합니다.
    각 모델에서 429가 나오면 다음 모델로 fallback합니다.
    Returns: (representative_title, summary_text)
    """
    user_msg = _build_user_message(cluster)
    fallback_title = cluster.titles[0][:15] if cluster.titles else "시그널"

    for model in config.CHAT_MODEL_FALLBACKS:
        try:
            raw = _call_model(model, user_msg)
            if model != config.CHAT_MODEL:
                print(f"    (fallback 성공: {model})")
            return _parse_response(raw)

        except ClientError as e:
            if "429" in str(e)[:20]:
                delay = _extract_retry_delay(e)
                print(f"  [429] {model} 한도 초과 → 다음 모델로 fallback (대기 {delay:.0f}s)")
                time.sleep(delay)
            else:
                print(f"  [!] {model} 호출 실패: {str(e)[:100]}")
                break  # 429 외 오류는 다음 모델 시도 무의미

        except Exception as e:
            print(f"  [!] {model} 오류: {e}")
            break

    print("  [!] 모든 fallback 모델 소진 — 요약 생성 불가")
    return fallback_title, "(요약 생성 실패 — 전체 모델 한도 초과)"


def run_summarization(clusters: list[Cluster]) -> None:
    """
    모든 클러스터에 대해 요약을 생성하고 Signal 테이블에 저장합니다.
    기존 시그널은 삭제 후 현재 실행 결과만 유지합니다.
    """
    if not clusters:
        print("[Summarizer] 요약할 클러스터가 없습니다.")
        return

    db.clear_signals()
    print(f"[Summarizer] {len(clusters)}개 클러스터 요약 시작...")

    for i, cluster in enumerate(clusters, start=1):
        title, summary = summarize_cluster(cluster)

        db.upsert_signal(
            cluster_id=cluster.cluster_id,
            representative_title=title,
            summary_text=summary,
            total_authority_score=cluster.total_authority_score,
        )

        print(f"  [{i}/{len(clusters)}] 「{title}」 — {len(cluster.url_hashes)}개 링크")

    print("[Summarizer] 완료.")


def print_signals() -> None:
    """터미널에 최종 시그널을 출력합니다."""
    signals = db.get_signals_with_links()

    if not signals:
        print("\n[결과] 생성된 시그널이 없습니다.")
        return

    print("\n" + "=" * 60)
    print(f"  📊  마켓 시그널 — {len(signals)}개")
    print("=" * 60)

    for entry in signals:
        sig   = entry["signal"]
        links = entry["links"]

        print(f"\n🔹 {sig['representative_title']}")
        print(f"   Authority: {sig['total_authority_score']:.2f}  |  링크: {len(links)}개")
        print()
        for line in sig["summary_text"].splitlines():
            print(f"   {line}")
        print()
        print("   관련 링크:")
        for lnk in links[:5]:
            score = lnk["authority_score"] or 0
            title = lnk["title"] or lnk["original_url"][:60]
            print(f"   [{score:.2f}] {title}")
        print("-" * 60)
