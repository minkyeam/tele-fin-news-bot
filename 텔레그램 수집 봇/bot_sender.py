"""
bot_sender.py
─────────────
생성된 마켓 시그널을 텔레그램 봇으로 지정 채팅방에 전송합니다.

설정 (`.env`)
  TELEGRAM_BOT_TOKEN    — BotFather에서 발급받은 토큰
  TELEGRAM_TARGET_CHAT  — 결과를 받을 채팅 ID 또는 @채널명
                          개인 DM: 숫자 ID (예: 123456789)
                          채널:    @my_signal_channel

메시지 포맷 예시
  ━━━━━━━━━━━━━━━━━━━━━━━━━━
  📊 마켓 시그널 · 2025-02-25 10:30
  ━━━━━━━━━━━━━━━━━━━━━━━━━━

  🔹 BTC ETF 승인 임박  [Authority 142.3]
  • 블랙록 현물 ETF 신청 최종 검토 단계
  • SEC, 이번 주 내 결정 예정
  • 시장 기대감으로 BTC 5% 상승

  🔗 coindesk.com/btc-etf-...
  🔗 theblock.co/sec-review-...
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import httpx

import config
import database as db

# ── 봇 설정 ──────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TARGET_CHAT = os.getenv("TELEGRAM_TARGET_CHAT", "")

_API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
_MAX_LEN  = 4000  # Telegram 메시지 최대 길이 여유치


# ── 메시지 포맷터 ─────────────────────────────────────────────────────────────

def _format_signal(sig: dict, links: list[dict]) -> str:
    """시그널 하나를 텍스트 블록으로 변환합니다."""
    title  = sig.get("representative_title", "시그널")
    score  = sig.get("total_authority_score", 0)
    summary = sig.get("summary_text", "")

    lines = [f"🔹 {title}  [Authority {score:.1f}]"]
    for line in summary.splitlines():
        if line.strip():
            lines.append(line.strip())

    # 관련 링크 (상위 3개)
    if links:
        lines.append("")
        for lnk in links[:3]:
            url   = lnk.get("original_url", "")
            # 도메인만 표시
            try:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc.lstrip("www.")
                display = f"{domain}{urlparse(url).path[:40]}"
            except Exception:
                display = url[:60]
            lines.append(f"🔗 {display}")

    return "\n".join(lines)


def build_messages(signals: list[dict]) -> list[str]:
    """
    시그널 목록 → 전송할 메시지 문자열 리스트.
    4000자 초과 시 자동으로 분할합니다.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    header = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 마켓 시그널 · {now}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )

    blocks = [header]
    for entry in signals:
        block = _format_signal(entry["signal"], entry["links"])
        blocks.append(block)

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
                "chat_id":    TARGET_CHAT,
                "text":       text,
                "parse_mode": "",           # 특수문자 이스케이프 불필요
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
