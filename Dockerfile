FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for yt-dlp
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY room_manager.py .
COPY analytics_api.py .
COPY analytics_db.py .
COPY config.json .
COPY dashboard.html .

# Expose port
EXPOSE 8000

# Run the server
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
