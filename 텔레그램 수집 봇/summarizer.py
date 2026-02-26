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

from google import genai
from google.genai import types
from google.genai.errors import ClientError

import config
import database as db
import stock_fetcher
from clusterer import Cluster

_client = genai.Client(api_key=config.GEMINI_API_KEY)

_SYSTEM_PROMPT = """당신은 금융·블록체인·DeFi 시장 전문 애널리스트입니다.
여러 텔레그램 채널에서 동시에 주목받은 기사와 메시지 묶음을 분석하여
핵심 마켓 시그널을 추출합니다.

출력 형식 (반드시 준수):
제목: [핵심을 담은 한국어 시그널 제목]
요약: [3문장 이내의 한국어 산문 요약]
종목: [직접 관련된 상장 종목, 없으면 "없음"]

종목 형식: 종목명(티커) — 쉼표로 구분, 최대 4개
  예: 삼성전자(005930.KS), SK하이닉스(000660.KS), NVIDIA(NVDA), 비트코인(BTC-USD)
  한국 코스피: 코드.KS / 코스닥: 코드.KQ / 미국 주식: 심볼 / 암호화폐: 심볼-USD

규칙:
- 제목은 반드시 "제목: "으로 시작
- 요약은 반드시 "요약: "으로 시작
- 종목은 반드시 "종목: "으로 시작
- 요약은 최대 3문장, 구체적 수치·프로젝트명·시장 영향 포함
- 직접 관련 종목이 없으면 종목: 없음
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


def _parse_response(text: str) -> tuple[str, str, str]:
    """LLM 응답에서 (제목, 요약, 종목문자열) 튜플을 파싱합니다."""
    title       = ""
    summary     = ""
    tickers_raw = ""

    for line in text.strip().splitlines():
        line = line.strip()
        if line.startswith("제목:"):
            title = line[3:].strip()
        elif line.startswith("요약:"):
            summary = line[3:].strip()
        elif line.startswith("종목:"):
            tickers_raw = line[3:].strip()

    # 요약이 여러 줄에 걸쳐 있을 경우 처리
    if not summary:
        lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
        body = [
            l for l in lines
            if not l.startswith("제목:") and not l.startswith("요약:") and not l.startswith("종목:")
        ]
        summary = "\n".join(body)[:500]

    return title or "시그널", summary, tickers_raw



_TEXT_SYSTEM_PROMPT = """당신은 금융·블록체인·DeFi 시장 전문 애널리스트입니다.
여러 텔레그램 채널에서 동시에 확산되는 바이럴 메시지를 분석하여
핵심 마켓 정보를 추출합니다.

출력 형식 (반드시 준수):
제목: [핵심을 담은 한국어 시그널 제목]
요약: [3문장 이내의 한국어 산문 요약]
종목: [직접 관련된 상장 종목 (최대 4개, 없으면 "없음")]

종목 형식: 종목명(티커) — 쉼표로 구분
  예: 삼성전자(005930.KS), NVIDIA(NVDA), 비트코인(BTC-USD)
  한국 코스피: 코드.KS / 코스닥: 코드.KQ / 미국 주식: 심볼 / 암호화폐: 심볼-USD

규칙:
- 제목은 반드시 "제목: "으로 시작
- 요약은 반드시 "요약: "으로 시작
- 종목은 반드시 "종목: "으로 시작
- 요약은 최대 3문장, 구체적 수치·프로젝트명·시장 영향 포함
- 광고·노이즈·일상 잡담은 완전히 무시"""


def _call_model(model: str, user_msg: str,
                system_prompt: str = _SYSTEM_PROMPT) -> str:
    """단일 모델 호출. Gemma는 system_instruction 미지원이므로 프롬프트에 병합."""
    is_gemma = model.startswith("gemma")

    if is_gemma:
        combined = f"{system_prompt}\n\n---\n\n{user_msg}"
        cfg = types.GenerateContentConfig(temperature=0.3, max_output_tokens=400)
        resp = _client.models.generate_content(model=model, contents=combined, config=cfg)
    else:
        cfg = types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=0.3,
            max_output_tokens=400,
        )
        resp = _client.models.generate_content(model=model, contents=user_msg, config=cfg)

    return resp.text or ""


def summarize_cluster(cluster: Cluster) -> tuple[str, str, str]:
    """
    모델 체인(config.CHAT_MODEL_FALLBACKS)을 순서대로 시도합니다.
    각 모델에서 429가 나오면 다음 모델로 fallback합니다.
    Returns: (representative_title, summary_text, tickers_raw)
    """
    user_msg = _build_user_message(cluster)
    fallback_title = cluster.titles[0] if cluster.titles else "시그널"

    for model in config.CHAT_MODEL_FALLBACKS:
        try:
            raw = _call_model(model, user_msg)
            if model != config.CHAT_MODEL:
                print(f"    (fallback 성공: {model})")
            return _parse_response(raw)

        except ClientError as e:
            if "429" in str(e)[:20]:
                print(f"  [429] {model} 한도 초과 → 다음 모델로 즉시 fallback")
            else:
                print(f"  [!] {model} 호출 실패: {str(e)[:100]}")
                break  # 429 외 오류는 다음 모델 시도 무의미

        except Exception as e:
            print(f"  [!] {model} 오류: {e}")
            break

    print("  [!] 모든 fallback 모델 소진 — og:description으로 대체")
    fallback_summary = next(
        (d.strip()[:500] for d in cluster.descriptions if d and d.strip()),
        "(요약 정보 없음)"
    )
    return fallback_title, fallback_summary, ""


def run_summarization(clusters: list[Cluster]) -> None:
    """
    모든 클러스터에 대해 요약을 생성하고 Signal 테이블에 저장합니다.
    기존 시그널은 삭제 후 현재 실행 결과만 유지합니다.
    """
    if not clusters:
        print("[Summarizer] 요약할 클러스터가 없습니다.")
        return

    # note: db.clear_signals()는 이제 clusterer에서 호출하거나 pipeline에서 관리합니다.
    print(f"[Summarizer] {len(clusters)}개 클러스터 요약 시작...")

    for i, cluster in enumerate(clusters, start=1):
        title, summary, tickers_raw = summarize_cluster(cluster)

        # 주가 조회
        stocks_text = ""
        if tickers_raw:
            price_data = stock_fetcher.fetch_prices(tickers_raw)
            stocks_text = stock_fetcher.format_stocks_text(price_data)

        db.upsert_signal(
            cluster_id=cluster.cluster_id,
            representative_title=title,
            summary_text=summary,
            total_authority_score=cluster.total_authority_score,
            stocks_text=stocks_text,
        )

        stock_info = f"  📈 {stocks_text[:60]}" if stocks_text else ""
        print(f"  [{i}/{len(clusters)}] 「{title}」 — {len(cluster.url_hashes)}개 링크{stock_info}")

    print("[Summarizer] 완료.")


# ── 텍스트 전용 바이럴 클러스터 요약 ──────────────────────────────────────────

def _build_text_user_message(cluster: Cluster) -> str:
    """URL 없는 바이럴 텍스트 클러스터의 LLM 입력 메시지를 구성합니다."""
    n_channels = len(set(cluster.channel_ids))
    posts = [
        f"{i}. {t[:300].strip()}"
        for i, t in enumerate(cluster.post_texts[:8], start=1)
    ]
    post_block = "\n\n".join(posts) if posts else "(내용 없음)"
    return (
        f"바이럴 채널 수: {n_channels}개  |  포스트 수: {len(cluster.post_texts)}개\n"
        f"총 조회수: {cluster.total_authority_score:.0f}\n"
        f"\n=== 채널 메시지 원문 ===\n{post_block}"
    )


def _build_tme_links(cluster: Cluster) -> str:
    """텍스트 클러스터의 post_ids에서 채널별 t.me 링크를 생성합니다."""
    links: list[str] = []
    seen_channels: set[str] = set()

    for post_id, channel_id in zip(cluster.post_ids, cluster.channel_ids):
        if channel_id in seen_channels or len(links) >= 5:
            continue
        seen_channels.add(channel_id)

        # post_id = "{channel_id}_{message_id}"
        parts = post_id.split("_", 1)
        msg_id = parts[1] if len(parts) == 2 else ""

        username = db.get_channel_username(channel_id)
        if username:
            links.append(f"https://t.me/{username.lstrip('@')}/{msg_id}")
        else:
            # 비공개 채널: -100 접두어 제거 후 t.me/c/ 형식
            cid = channel_id.lstrip("-")
            if cid.startswith("100"):
                cid = cid[3:]
            links.append(f"https://t.me/c/{cid}/{msg_id}")

    return "\n".join(links)


def summarize_text_cluster(cluster: Cluster) -> tuple[str, str, str]:
    """
    텍스트 전용 바이럴 클러스터를 요약합니다.
    Returns: (representative_title, summary_text, tickers_raw)
    """
    user_msg = _build_text_user_message(cluster)
    fallback_title = (cluster.post_texts[0][:30] + "...") if cluster.post_texts else "바이럴"

    for model in config.CHAT_MODEL_FALLBACKS:
        try:
            raw = _call_model(model, user_msg, system_prompt=_TEXT_SYSTEM_PROMPT)
            if model != config.CHAT_MODEL:
                print(f"    (fallback 성공: {model})")
            return _parse_response(raw)

        except ClientError as e:
            if "429" in str(e)[:20]:
                print(f"  [429] {model} 한도 초과 → 다음 모델로 즉시 fallback")
            else:
                print(f"  [!] {model} 호출 실패: {str(e)[:100]}")
                break

        except Exception as e:
            print(f"  [!] {model} 오류: {e}")
            break

    fallback_summary = cluster.post_texts[0][:500] if cluster.post_texts else "(요약 정보 없음)"
    return fallback_title, fallback_summary, ""


def run_text_summarization(text_clusters: list[Cluster]) -> None:
    """바이럴 텍스트 클러스터를 요약하고 Signal 테이블에 저장합니다."""
    if not text_clusters:
        return

    print(f"[Summarizer] 바이럴 텍스트 {len(text_clusters)}개 클러스터 요약...")

    for i, cluster in enumerate(text_clusters, start=1):
        title, summary, tickers_raw = summarize_text_cluster(cluster)

        stocks_text = ""
        if tickers_raw:
            price_data = stock_fetcher.fetch_prices(tickers_raw)
            stocks_text = stock_fetcher.format_stocks_text(price_data)

        tme_links = _build_tme_links(cluster)

        db.upsert_signal(
            cluster_id=cluster.cluster_id,
            representative_title=title,
            summary_text=summary,
            total_authority_score=cluster.total_authority_score,
            stocks_text=stocks_text,
            tme_links=tme_links,
        )

        n_ch = len(set(cluster.channel_ids))
        print(f"  [{i}/{len(text_clusters)}] [바이럴/{n_ch}채널] 「{title}」")

    print("[Summarizer] 바이럴 텍스트 요약 완료.")


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
        stocks = sig.get("stocks_text", "")
        if stocks:
            print()
            print("   📈 관련 종목:")
            for line in stocks.splitlines():
                print(f"   {line}")
        print()
        print("   관련 링크:")
        for lnk in links[:5]:
            score = lnk["authority_score"] or 0
            title = lnk["title"] or lnk["original_url"][:60]
            print(f"   [{score:.2f}] {title}")
        print("-" * 60)
