FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8651 \
    PYTHONUNBUFFERED=1

EXPOSE 8651

USER 65532

CMD ["python", "server.py"]
