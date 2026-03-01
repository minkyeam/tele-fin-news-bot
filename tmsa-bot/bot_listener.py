"""
bot_listener.py
───────────────
텔레그램 봇 명령어 폴링 + 정기 자동 실행.

실행
  python main.py --listen

명령어 (본인 채팅방에서 입력)
  /run    — 전체 파이프라인 실행 후 시그널 전송
  /send   — 저장된 시그널만 재전송 (수집·분석 없음)
  /status — 마지막 실행 시각 및 시그널 수 확인
  /help   — 명령어 목록

자동 실행 설정 (.env)
  AUTO_RUN_HOURS=8   — 8시간마다 자동으로 /run 실행
  AUTO_RUN_HOURS=0   — 자동 실행 비활성화 (기본값)
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import httpx

import bot_sender
import config
import database as db
import pipeline

_BOT_TOKEN   = bot_sender.BOT_TOKEN
_TARGET_CHAT = bot_sender.TARGET_CHAT
_API_BASE    = bot_sender._API_BASE

_last_run: datetime | None = None
_running = False


# ── 내부 헬퍼 ─────────────────────────────────────────────────────────────────

async def _notify(text: str, client: httpx.AsyncClient) -> None:
    """짧은 상태 알림 전송 (plain text)."""
    try:
        await client.post(
            f"{_API_BASE}/sendMessage",
            json={"chat_id": _TARGET_CHAT, "text": text},
            timeout=10.0,
        )
    except Exception:
        pass


# ── 명령어 핸들러 ─────────────────────────────────────────────────────────────

async def _handle_run(client: httpx.AsyncClient) -> None:
    global _last_run, _running
    if _running:
        await _notify("⚠️ 파이프라인이 이미 실행 중입니다. 잠시 후 다시 시도하세요.", client)
        return
    _running = True
    try:
        await _notify("🔄 파이프라인 시작합니다... (수분 소요)", client)
        await pipeline.run_pipeline(use_subscribed=True)
        await bot_sender.send_signals()
        _last_run = datetime.now(timezone.utc)
        count = len(db.get_signals_with_links())
        await _notify(f"✅ 완료! 시그널 {count}개 전송됨", client)
    except Exception as e:
        await _notify(f"❌ 오류 발생: {e}", client)
    finally:
        _running = False


async def _handle_send(client: httpx.AsyncClient) -> None:
    await _notify("📤 저장된 시그널 전송 중...", client)
    await bot_sender.send_signals()


async def _handle_status(client: httpx.AsyncClient) -> None:
    count = len(db.get_signals_with_links())
    last = _last_run.strftime("%Y-%m-%d %H:%M UTC") if _last_run else "없음"
    auto = f"{config.AUTO_RUN_HOURS}시간마다" if config.AUTO_RUN_HOURS > 0 else "비활성화"
    await _notify(
        f"📊 TMSA 상태\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"마지막 실행: {last}\n"
        f"저장된 시그널: {count}개\n"
        f"자동 실행: {auto}\n"
        f"현재 실행 중: {'예' if _running else '아니오'}",
        client,
    )


_HELP_TEXT = (
    "📌 TMSA 명령어\n"
    "━━━━━━━━━━━━━━━━━\n"
    "/run    — 전체 파이프라인 실행 + 시그널 전송\n"
    "/send   — 저장된 시그널만 전송\n"
    "/status — 실행 상태 확인\n"
    "/help   — 이 메시지"
)


# ── 폴링 루프 ─────────────────────────────────────────────────────────────────

async def _poll_once(offset: int, client: httpx.AsyncClient) -> int:
    """getUpdates 한 번 호출. 수신된 명령어를 처리하고 다음 offset을 반환합니다."""
    try:
        resp = await client.get(
            f"{_API_BASE}/getUpdates",
            params={"offset": offset, "timeout": 30, "allowed_updates": ["message"]},
            timeout=35.0,
        )
        updates = resp.json().get("result", [])
    except Exception as e:
        print(f"[Listener] 폴링 오류: {e}")
        await asyncio.sleep(5)
        return offset

    for update in updates:
        offset = update["update_id"] + 1
        msg     = update.get("message", {})
        chat_id = str(msg.get("chat", {}).get("id", ""))
        text    = (msg.get("text") or "").strip().lower()

        # 보안: 등록된 채팅방에서만 명령 수락
        if chat_id != str(_TARGET_CHAT):
            continue

        if text.startswith("/run"):
            asyncio.create_task(_handle_run(client))
        elif text.startswith("/send"):
            asyncio.create_task(_handle_send(client))
        elif text.startswith("/status"):
            asyncio.create_task(_handle_status(client))
        elif text.startswith("/help") or text.startswith("/start"):
            asyncio.create_task(_notify(_HELP_TEXT, client))

    return offset


async def _command_loop(client: httpx.AsyncClient) -> None:
    """Telegram getUpdates 롱폴링 루프."""
    offset = 0
    print(f"[Listener] 명령어 수신 대기 중 (채팅 ID: {_TARGET_CHAT})")
    print("[Listener] 사용 가능: /run  /send  /status  /help")
    while True:
        offset = await _poll_once(offset, client)


async def _auto_run_loop(client: httpx.AsyncClient) -> None:
    """AUTO_RUN_HOURS 간격으로 파이프라인을 자동 실행합니다."""
    interval_sec = config.AUTO_RUN_HOURS * 3600
    print(f"[Listener] 자동 실행: {config.AUTO_RUN_HOURS}시간마다")
    while True:
        await asyncio.sleep(interval_sec)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"[Listener] 자동 실행 트리거 ({now})")
        await _handle_run(client)


# ── Render 헬스체크 서버 ───────────────────────────────────────────────────────

async def _health_server() -> None:
    """Render Web Service용 최소 HTTP 서버 (포트 바인딩)."""
    port = int(os.environ.get("PORT", 8080))

    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(1024)
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(_handle, "0.0.0.0", port)
    print(f"[Health] Listening on port {port}")
    async with server:
        await server.serve_forever()


# ── 진입점 ────────────────────────────────────────────────────────────────────

async def start_listener() -> None:
    """봇 리스너를 시작합니다. Ctrl+C로 종료합니다."""
    if not _BOT_TOKEN or not _TARGET_CHAT:
        print("[Listener] TELEGRAM_BOT_TOKEN 또는 TELEGRAM_TARGET_CHAT 미설정 — 종료")
        print("           .env 파일에 두 값을 추가하세요.")
        return

    async with httpx.AsyncClient() as client:
        tasks = [
            asyncio.create_task(_health_server()),
            asyncio.create_task(_command_loop(client)),
        ]
        if config.AUTO_RUN_HOURS > 0:
            tasks.append(asyncio.create_task(_auto_run_loop(client)))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("\n[Listener] 종료됨.")
