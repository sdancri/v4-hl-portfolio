FROM python:3.12-slim AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
        tzdata curl gcc python3-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Europe/Bucharest \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MALLOC_ARENA_MAX=2

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

RUN mkdir -p /app/logs /app/data
VOLUME ["/app/logs", "/app/data"]

EXPOSE 8204

HEALTHCHECK --interval=30s --timeout=5s --start-period=45s --retries=3 \
    CMD curl -sf http://localhost:${CHART_PORT:-8204}/api/status || exit 1

CMD ["sh", "-c", "python -u main.py --config ${CONFIG_FILE:-config/config_v4_hl.yaml}"]
