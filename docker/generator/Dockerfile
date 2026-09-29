FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
WORKDIR /app

COPY requirements/generator.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY generator /app/generator
COPY schemas /app/schemas
COPY scripts /app/scripts

CMD ["python", "-m", "generator.producer"]
