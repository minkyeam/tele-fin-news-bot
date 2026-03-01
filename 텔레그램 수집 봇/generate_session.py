"""
generate_session.py
───────────────────
Render 등 클라우드 배포를 위한 Telegram StringSession 생성 스크립트.

사용법 (로컬에서 1회만 실행):
  python generate_session.py

출력된 세션 문자열을 Render 환경변수 TG_SESSION_STRING 에 붙여넣으세요.
"""

import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv
import os

load_dotenv()

API_ID   = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
PHONE    = os.getenv("TELEGRAM_PHONE", "")


async def main() -> None:
    async with TelegramClient(StringSession(), API_ID, API_HASH) as client:
        if not PHONE:
            raise SystemExit("TELEGRAM_PHONE이 .env에 없습니다.")
        await client.send_code_request(PHONE)
        code = input("텔레그램으로 받은 인증코드 입력: ").strip()
        await client.sign_in(PHONE, code)

        session_string = client.session.save()
        print("\n" + "=" * 60)
        print("아래 문자열을 Render 환경변수 TG_SESSION_STRING 에 붙여넣으세요:")
        print("=" * 60)
        print(session_string)
        print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
