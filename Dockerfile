FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

RUN apt-get update && apt-get install -y \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

COPY . .

EXPOSE 8080 8091

CMD ["python", "service_entrypoint.py"]
