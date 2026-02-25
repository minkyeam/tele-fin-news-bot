"""
collector.py
────────────
Telethon을 이용해 텔레그램 채널에서 최근 N시간 치 메시지를 수집하고,
정규표현식으로 URL을 추출하여 DB에 저장합니다.

기능
 1. 내가 구독한 채널 자동 탐색 (iter_dialogs) 또는 수동 지정
 2. 채널 메타데이터 수집 (구독자 수 포함)
 3. 지정 시간 범위(COLLECT_HOURS)의 메시지 수집
 4. 메시지에서 URL 추출 (정규표현식)
 5. URL 메타데이터(title, description) 크롤링
 6. Channel / Post / Link / post_links 테이블 업데이트
"""

from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from telethon import TelegramClient
from telethon.tl.types import Channel

import config
import database as db

# ─── URL 추출 정규식 ─────────────────────────────────────────────────────────
# t.me 링크, localhost, IP 등 노이즈 제거
_URL_RE = re.compile(
    r'https?://'                    # scheme
    r'(?!t\.me|telegram\.me)'       # 텔레그램 자체 링크 제외
    r'(?:[a-zA-Z0-9\-]+\.)+[a-zA-Z]{2,}'  # domain
    r'(?:/[^\s\)\]\}\"\',<>]*)?',  # path
    re.IGNORECASE
)

_SKIP_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".mp4", ".pdf", ".zip"}


def extract_urls(text: str) -> list[str]:
    """텍스트에서 외부 URL을 추출하고 정제합니다."""
    if not text:
        return []
    urls = []
    for url in _URL_RE.findall(text):
        # 불필요한 trailing 문자 제거
        url = url.rstrip(".,;!?)")
        parsed = urlparse(url)
        ext = parsed.path.rsplit(".", 1)[-1].lower()
        if f".{ext}" in _SKIP_EXTENSIONS:
            continue
        urls.append(url)
    return list(dict.fromkeys(urls))  # 중복 제거, 순서 유지


# ─── 메타데이터 크롤링 ────────────────────────────────────────────────────────

async def fetch_url_metadata(url: str, client: httpx.AsyncClient) -> tuple[str, str]:
    """URL의 og:title / og:description 또는 <title> 태그를 반환합니다."""
    try:
        resp = await client.get(
            url,
            follow_redirects=True,
            timeout=8.0,
            headers={"User-Agent": "Mozilla/5.0 (TMSA/1.0)"}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        title = (
            (soup.find("meta", property="og:title") or {}).get("content")
            or (soup.find("title") or soup.new_tag("x")).get_text()
            or ""
        ).strip()[:300]

        description = (
            (soup.find("meta", property="og:description") or {}).get("content")
            or (soup.find("meta", attrs={"name": "description"}) or {}).get("content")
            or ""
        ).strip()[:500]

        return title, description
    except Exception:
        return "", ""


# ─── 구독 채널 자동 탐색 ──────────────────────────────────────────────────────

async def fetch_subscribed_channels(tg: TelegramClient) -> list[Channel]:
    """
    내가 참여 중인 채널(broadcast) 및 슈퍼그룹 목록을 반환합니다.
    username이 없는 비공개 채널도 포함됩니다.
    """
    entities: list[Channel] = []
    async for dialog in tg.iter_dialogs():
        entity = dialog.entity
        if isinstance(entity, Channel):
            entities.append(entity)
    return entities


# ─── 메인 수집 함수 ───────────────────────────────────────────────────────────

async def collect(
    channels: list[str] | None = None,
    use_subscribed: bool = False,
) -> None:
    """
    channels       : 수집할 채널 username 리스트. None이면 config.CHANNEL_LIST 사용.
    use_subscribed : True이면 내가 구독한 채널 전체를 자동 탐색합니다.
    """
    db.init_db()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=config.COLLECT_HOURS)

    async with TelegramClient(
        "tmsa_session",
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH
    ) as tg:
        # ── 채널 목록 결정 ────────────────────────────────────────────────────
        if use_subscribed or (not channels and not config.CHANNEL_LIST):
            print("[Collector] 구독 채널 목록을 불러오는 중...")
            entities = await fetch_subscribed_channels(tg)
            print(f"[Collector] 구독 채널 {len(entities)}개 발견")
            _print_channel_list(entities)
        else:
            # username 문자열 → entity 변환
            target_usernames = channels or config.CHANNEL_LIST
            print(f"[Collector] 지정 채널 {len(target_usernames)}개 로드 중...")
            entities = []
            for username in target_usernames:
                try:
                    entity = await tg.get_entity(username)
                    if isinstance(entity, Channel):
                        entities.append(entity)
                    else:
                        print(f"  [!] {username} 은 채널이 아닙니다 (건너뜀)")
                except Exception as e:
                    print(f"  [!] 채널 조회 실패: {username} — {e}")

        if not entities:
            print("[Collector] 수집할 채널이 없습니다.")
            return

        print(f"\n[Collector] 수집 시작 | 채널 수: {len(entities)} | 기준: 최근 {config.COLLECT_HOURS}시간")
        print(f"            cutoff: {cutoff.isoformat()}\n")

        async with httpx.AsyncClient() as http:
            for entity in entities:
                await _collect_channel(tg, http, entity, cutoff)

    print("\n[Collector] 수집 완료.")


def _print_channel_list(entities: list[Channel]) -> None:
    """발견된 채널 목록을 출력합니다."""
    print("\n  ┌──────────────────────────────────────────────────────")
    for e in entities:
        sub = getattr(e, "participants_count", 0) or 0
        name = getattr(e, "title", "?")
        username = f"@{e.username}" if getattr(e, "username", None) else "(비공개)"
        kind = "📢" if getattr(e, "broadcast", False) else "👥"
        print(f"  │ {kind} {name:<30} {username:<25} 구독자 {sub:>8,}")
    print("  └──────────────────────────────────────────────────────\n")


async def _collect_channel(
    tg: TelegramClient,
    http: httpx.AsyncClient,
    entity: Channel,
    cutoff: datetime
) -> None:
    # 구독자 수: dialog에서 이미 participants_count를 가져옴 (추가 API 호출 없음)
    subscriber_count = getattr(entity, "participants_count", 0) or 0
    channel_id = str(entity.id)
    name = getattr(entity, "title", channel_id)
    username_str = f"@{entity.username}" if getattr(entity, "username", None) else f"id={channel_id}"

    db.upsert_channel(
        channel_id=channel_id,
        name=name,
        subscriber_count=subscriber_count,
        category=""
    )
    print(f"  [채널] {name} ({username_str}, 구독자={subscriber_count:,})")

    msg_count = 0
    url_count = 0

    async for message in tg.iter_messages(entity, offset_date=None, reverse=False):
        if not message.date:
            continue
        # naive → aware 변환
        msg_time = message.date.replace(tzinfo=timezone.utc)
        if msg_time < cutoff:
            break  # 오래된 메시지 → 종료 (reverse=False: 최신순)

        if not message.text:
            continue

        post_id = f"{channel_id}_{message.id}"
        views   = getattr(message, "views", 0) or 0

        db.upsert_post(
            post_id=post_id,
            channel_id=channel_id,
            content=message.text,
            views=views,
            timestamp=msg_time
        )
        msg_count += 1

        urls = extract_urls(message.text)
        for url in urls:
            url_hash = db.upsert_link(url)
            db.link_post_link(post_id, url_hash)
            url_count += 1

            # 메타데이터가 아직 없으면 크롤링
            with db.get_conn() as conn:
                existing = conn.execute(
                    "SELECT title FROM links WHERE url_hash = ?", (url_hash,)
                ).fetchone()

            if existing and not existing["title"]:
                title, desc = await fetch_url_metadata(url, http)
                if title:
                    db.update_link_metadata(url_hash, title, desc)

    print(f"    → 메시지 {msg_count}개, URL {url_count}개 저장")
