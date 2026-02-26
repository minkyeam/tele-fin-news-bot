"""
stock_fetcher.py
────────────────
LLM이 출력한 종목 문자열을 파싱하고 yfinance로 현재 주가를 조회합니다.

입력 형식 (LLM 출력):
  삼성전자(005930.KS), SK하이닉스(000660.KS), NVIDIA(NVDA), 비트코인(BTC-USD)

티커 규칙:
  - 한국 코스피: 코드.KS  (예: 005930.KS)
  - 한국 코스닥: 코드.KQ  (예: 035720.KQ)
  - 미국 주식:   심볼     (예: NVDA, AAPL, TSLA)
  - 암호화폐:    심볼-USD (예: BTC-USD, ETH-USD)
"""

from __future__ import annotations

import re

import yfinance as yf


def parse_tickers(tickers_raw: str) -> list[tuple[str, str]]:
    """
    '삼성전자(005930.KS), NVIDIA(NVDA)' 형태의 문자열을
    [(이름, 티커), ...] 리스트로 파싱합니다.
    """
    if not tickers_raw:
        return []
    normalized = tickers_raw.strip().rstrip(".")
    if normalized.lower() in ("없음", "none", "-", ""):
        return []

    results: list[tuple[str, str]] = []
    for entry in tickers_raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        m = re.match(r"(.+?)\(([^)]+)\)", entry)
        if m:
            results.append((m.group(1).strip(), m.group(2).strip()))
        else:
            # 괄호 없이 티커만 있는 경우 그대로 사용
            results.append((entry, entry))

    return results[:4]


def fetch_prices(tickers_raw: str) -> list[dict]:
    """
    LLM 출력 문자열에서 종목을 파싱하고 yfinance로 현재 주가를 조회합니다.
    Returns: [{"name": ..., "ticker": ..., "display": ...}, ...]
    """
    pairs = parse_tickers(tickers_raw)
    if not pairs:
        return []

    results: list[dict] = []
    for name, sym in pairs:
        try:
            hist = yf.Ticker(sym).history(period="5d")
            if hist.empty or len(hist) < 1:
                print(f"  [주가] {sym}: 데이터 없음")
                continue

            current = float(hist["Close"].iloc[-1])
            prev    = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else current
            change_pct = (current - prev) / prev * 100 if prev else 0.0

            if sym.endswith(".KS") or sym.endswith(".KQ"):
                price_str = f"₩{current:,.0f}"
            else:
                price_str = f"${current:,.2f}"

            arrow = "▲" if change_pct >= 0 else "▼"
            sign  = "+" if change_pct >= 0 else ""
            results.append({
                "name":    name,
                "ticker":  sym,
                "display": f"{name}: {price_str} {arrow} {sign}{change_pct:.2f}%",
            })
        except Exception as e:
            print(f"  [주가] {sym} 조회 실패: {e}")

    return results


def format_stocks_text(price_data: list[dict]) -> str:
    """주가 데이터를 DB 저장용 텍스트로 변환합니다."""
    return "\n".join(item["display"] for item in price_data)
