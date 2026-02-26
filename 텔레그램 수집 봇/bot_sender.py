"""
bot_sender.py
─────────────
생성된 마켓 시그널을 텔레그램 봇으로 지정 채팅방에 전송합니다.
HTML parse_mode 사용 — 인라인 링크, 카테고리 태그, Authority 이모지 포함.

설정 (`.env`)
  TELEGRAM_BOT_TOKEN    — BotFather에서 발급받은 토큰
  TELEGRAM_TARGET_CHAT  — 결과를 받을 채팅 ID 또는 @채널명

메시지 포맷 예시
  ━━━ 📊 TMSA 마켓 시그널 리포트 ⏰ 2025-02-25 10:30 UTC ━━━

  🔥 [반도체] AI칩 수출 규제 강화
  삼성·SK하이닉스 미국 규제에 따른 대중 수출 제한 강화 예정.
  출처: Reuters | 블룸버그

  ⭐️ [거시경제] 연준 금리 동결 시사
  ...
  ━━━━━━━━━━━━━━━━━━━━━━━━━━
  🤖 TMSA  |  Signals: 15  |  Powered by Gemini
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

import config
import database as db

# ── 봇 설정 ──────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TARGET_CHAT = os.getenv("TELEGRAM_TARGET_CHAT", "")

_API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
_MAX_LEN  = 4000  # Telegram 메시지 최대 길이 여유치


# ── HTML 헬퍼 ─────────────────────────────────────────────────────────────────

def _escape_html(text: str) -> str:
    """Telegram HTML 모드에서 필요한 문자 이스케이프."""
    return (
        text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _authority_emoji(rank: int, total: int) -> str:
    """순위 기반 Authority 이모지 (1-indexed rank)."""
    if total == 0:
        return "🔹"
    pct = rank / total
    if pct <= 0.30:
        return "🔥"
    if pct <= 0.60:
        return "⭐️"
    return "🔹"


def _make_source_link(url: str, index: int) -> str:
    """URL을 '<a href="...">도메인</a>' 형태의 인라인 링크로 변환."""
    try:
        domain = urlparse(url).netloc.lstrip("www.")
        label = domain or f"출처{index}"
    except Exception:
        label = f"출처{index}"
    return f'<a href="{url}">{_escape_html(label)}</a>'


# ── 메시지 포맷터 ─────────────────────────────────────────────────────────────

def _format_signal(sig: dict, links: list[dict], emoji: str) -> str:
    """시그널 하나를 HTML 블록으로 변환합니다."""
    title   = _escape_html(sig.get("representative_title") or "시그널")
    score   = sig.get("total_authority_score", 0)
    summary = sig.get("summary_text") or ""

    lines = [f"{emoji} <b>{title}</b>  <i>({score:.1f})</i>"]

    for line in summary.splitlines():
        stripped = line.strip()
        if stripped:
            lines.append(_escape_html(stripped))

    # 관련 종목 주가
    stocks = sig.get("stocks_text", "")
    if stocks:
        lines.append("")
        lines.append("📈 <b>관련 종목</b>")
        for stock_line in stocks.splitlines():
            if stock_line.strip():
                lines.append(_escape_html(stock_line.strip()))

    # 출처 인라인 링크 (상위 3개)
    source_links = []
    for i, lnk in enumerate(links[:3], start=1):
        url = lnk.get("original_url", "")
        if url:
            source_links.append(_make_source_link(url, i))
    if source_links:
        lines.append("")
        lines.append("출처: " + " | ".join(source_links))

    return "\n".join(lines)


def build_messages(signals: list[dict]) -> list[str]:
    """
    시그널 목록 → 전송할 HTML 메시지 문자열 리스트.
    4000자 초과 시 자동으로 분할합니다.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = len(signals)

    header = (
        f"━━━ 📊 TMSA 마켓 시그널 리포트 ⏰ {now} ━━━"
    )
    footer = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 TMSA  |  Signals: {total}  |  Powered by Gemini"
    )

    blocks: list[str] = [header]
    for rank, entry in enumerate(signals, start=1):
        emoji = _authority_emoji(rank, total)
        block = _format_signal(entry["signal"], entry["links"], emoji)
        blocks.append(block)
    blocks.append(footer)

    # 4000자 단위로 분할
    messages: list[str] = []
    current = ""
    for block in blocks:
        candidate = (current + "\n\n" + block).strip()
        if len(candidate) > _MAX_LEN and current:
            messages.append(current.strip())
            current = block
        else:
            current = candidate

    if current.strip():
        messages.append(current.strip())

    return messages


# ── 봇 API 호출 ───────────────────────────────────────────────────────────────

async def _send_message(text: str, client: httpx.AsyncClient) -> bool:
    try:
        resp = await client.post(
            f"{_API_BASE}/sendMessage",
            json={
                "chat_id":                TARGET_CHAT,
                "text":                   text,
                "parse_mode":             "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15.0,
        )
        data = resp.json()
        if not data.get("ok"):
            print(f"  [!] 봇 전송 실패: {data.get('description')}")
            return False
        return True
    except Exception as e:
        print(f"  [!] 봇 전송 오류: {e}")
        return False


async def send_signals() -> None:
    """
    DB에서 시그널을 읽어 텔레그램 봇으로 전송합니다.
    BOT_TOKEN 또는 TARGET_CHAT이 없으면 건너뜁니다.
    """
    if not BOT_TOKEN or not TARGET_CHAT:
        print("[BotSender] TELEGRAM_BOT_TOKEN 또는 TELEGRAM_TARGET_CHAT 미설정 — 건너뜀")
        print("            BotFather에서 토큰 발급 후 .env에 추가하세요.")
        return

    signals = db.get_signals_with_links()
    if not signals:
        print("[BotSender] 전송할 시그널이 없습니다.")
        return

    messages = build_messages(signals)
    print(f"[BotSender] {len(signals)}개 시그널을 {len(messages)}개 메시지로 전송 중...")

    async with httpx.AsyncClient() as client:
        for i, msg in enumerate(messages, 1):
            ok = await _send_message(msg, client)
            status = "✓" if ok else "✗"
            print(f"  [{i}/{len(messages)}] {status}")

    print(f"[BotSender] 전송 완료 → {TARGET_CHAT}")
