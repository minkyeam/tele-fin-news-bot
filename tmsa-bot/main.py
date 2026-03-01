"""
main.py
───────
TMSA CLI 진입점.

사용법:
  python main.py                    # 구독 채널 전체 수집 → 스코어 → 클러스터 → 요약 → DB 저장
  python main.py --my-channels      # 위와 동일 (명시적)
  python main.py coindesk theblock  # 채널 직접 지정
  python main.py --signal-only      # DB 링크로 클러스터+요약만 재실행 → DB 저장
  python main.py --score-only       # 스코어링만 재실행
  python main.py --send             # DB에 저장된 시그널을 텔레그램 봇으로 전송
  python main.py --show             # 저장된 시그널 터미널 출력
  python main.py --listen           # 봇 명령어 수신 + 자동 실행 데몬 시작
"""

from __future__ import annotations

import asyncio
import sys

import pipeline
import scorer
import clusterer
import summarizer
import bot_sender
import bot_listener
import config


def main() -> None:
    args = sys.argv[1:]

    if "--listen" in args:
        asyncio.run(bot_listener.start_listener())
        return

    if "--show" in args:
        summarizer.print_signals()
        return

    if "--send" in args:
        asyncio.run(bot_sender.send_signals())
        return

    if "--score-only" in args:
        scorer.run_scoring()
        return

    if "--signal-only" in args:
        scorer.run_scoring()
        scorer.run_post_scoring()
        clusters = clusterer.run_unified_clustering(top_n=config.TOP_SIGNALS)
        summarizer.run_summarization(clusters)
        summarizer.print_signals()
        print("\n봇 전송: python main.py --send")
        return

    # 채널 인수 파싱 (플래그가 아닌 것)
    channels = [a for a in args if not a.startswith("--")] or None

    use_subscribed = "--my-channels" in args or (
        not channels and not config.CHANNEL_LIST
    )

    asyncio.run(pipeline.run_pipeline(channels, use_subscribed=use_subscribed))


if __name__ == "__main__":
    main()
