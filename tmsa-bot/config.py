"""
config.py
─────────
.env 파일에서 설정을 로드하고, 전역 상수를 제공합니다.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_API_ID     = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH   = os.environ["TELEGRAM_API_HASH"]
TELEGRAM_PHONE      = os.getenv("TELEGRAM_PHONE", "")       # 세션 생성 시에만 필요
TG_SESSION_STRING   = os.getenv("TG_SESSION_STRING", "")    # Render 등 클라우드 배포용

# ── Google Gemini ─────────────────────────────────────────────────────────────
GEMINI_API_KEY    = os.environ["GEMINI_API_KEY"]

# ── Authority Score 가중치 ────────────────────────────────────────────────────
AUTHORITY_W1      = float(os.getenv("AUTHORITY_W1", "0.7"))
AUTHORITY_W2      = float(os.getenv("AUTHORITY_W2", "0.3"))
# 포스트 Authority 계산 시 URL 어소리티의 보조 가중치 (0이면 URL 점수 무시)
POST_URL_WEIGHT   = float(os.getenv("POST_URL_WEIGHT", "0.3"))

# ── 수집 설정 ─────────────────────────────────────────────────────────────────
COLLECT_HOURS       = int(os.getenv("COLLECT_HOURS", "24"))
CLUSTER_TOP_PERCENT = int(os.getenv("CLUSTER_TOP_PERCENT", "20"))

# ── 자동 실행 (--listen 모드) ─────────────────────────────────────────────────
# 0 이면 비활성화. 예: AUTO_RUN_HOURS=8 → 8시간마다 파이프라인 자동 실행
AUTO_RUN_HOURS = int(os.getenv("AUTO_RUN_HOURS", "0"))

# ── DBSCAN ───────────────────────────────────────────────────────────────────
DBSCAN_EPS         = float(os.getenv("DBSCAN_EPS", "0.55"))
DBSCAN_MIN_SAMPLES = int(os.getenv("DBSCAN_MIN_SAMPLES", "2"))
TOP_SIGNALS        = int(os.getenv("TOP_SIGNALS", "15"))

# ── 데이터베이스 ──────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "tmsa.db")

# ── 수집 채널 리스트 ──────────────────────────────────────────────────────────
_raw = os.getenv("CHANNEL_LIST", "")
CHANNEL_LIST: list[str] = [ch.strip() for ch in _raw.split(",") if ch.strip()]

# ── Gemini 모델 ───────────────────────────────────────────────────────────────
# 임베딩 모델 우선순위 체인
# API 모델 소진 시 로컬 sentence-transformers로 자동 fallback
EMBEDDING_MODEL = "gemini-embedding-001"   # Gemini API (3072차원)
EMBEDDING_MODEL_FALLBACKS = [
    "gemini-embedding-001",                          # Gemini API
    "paraphrase-multilingual-mpnet-base-v2",         # 로컬 — 한국어 포함 다국어, 768차원
    "paraphrase-multilingual-MiniLM-L12-v2",         # 로컬 — 경량 다국어, 384차원
]

# 요약 모델 우선순위 체인 — 앞 모델이 429이면 다음으로 자동 fallback
# Gemini 계열: 낮은 쿼터 / Gemma 계열: 별도 쿼터 체계 (사실상 무제한에 가까움)
CHAT_MODEL = "gemini-flash-latest"
CHAT_MODEL_FALLBACKS = [
    "gemini-flash-latest",       # Gemini 계열 (성능 우선)
    "gemini-flash-lite-latest",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
    "gemini-2.5-pro",
    "gemma-3-27b-it",            # Gemma 계열 (별도 쿼터 — 사실상 무제한)
    "gemma-3-12b-it",
    "gemma-3-4b-it",
]
