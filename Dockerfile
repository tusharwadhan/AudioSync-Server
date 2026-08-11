FROM python:3.11-slim

WORKDIR /app

# System deps: ffmpeg for yt-dlp, curl/ca-certificates for installs, iptables for tailscale
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    ca-certificates \
    iptables \
    && curl -fsSL https://tailscale.com/install.sh | sh \
    && curl -L -o /tmp/gost.gz https://github.com/ginuerzh/gost/releases/download/v2.11.5/gost-linux-amd64-2.11.5.gz \
    && gunzip /tmp/gost.gz \
    && mv /tmp/gost /usr/local/bin/gost \
    && chmod +x /usr/local/bin/gost \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY main.py .
COPY room_manager.py .
COPY analytics_db.py .
COPY config.json .
COPY start.sh .
# Cloud-sync surface (Phase 1+): Postgres + Firebase auth + Alembic migrations.
COPY auth.py .
COPY db.py .
COPY models.py .
COPY sync.py .
# Social v1 — global lounge + presence + DMs (imported by main.py).
COPY social.py .
# Remote control — browser drives the phone (imported by main.py). Missing
# this COPY is not a degraded feature, it is an ImportError at boot that
# takes the whole API down, extraction included.
COPY control_session.py .
# The /remote page is read from disk at request time, so without this the
# route 404s in production while working perfectly in local dev.
COPY static ./static
COPY alembic.ini .
COPY alembic ./alembic
RUN chmod +x start.sh

EXPOSE 8000

CMD ["./start.sh"]
