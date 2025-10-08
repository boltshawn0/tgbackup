# No mise. Use official Python, super stable.
FROM python:3.11-slim

# Optional system deps that help with qrcode/Pillow, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libjpeg62-turbo-dev zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app
COPY . .

# Log unbuffered
ENV PYTHONUNBUFFERED=1

# Start your backup script
CMD ["python", "tg_backup.py"]
