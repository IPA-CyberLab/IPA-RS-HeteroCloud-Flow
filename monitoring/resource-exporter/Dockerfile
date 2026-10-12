FROM python:3.13.9-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY monitoring/resource-exporter/requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
COPY monitoring/resource-exporter/exporter.py .
USER 65532:65532
EXPOSE 9790
ENTRYPOINT ["python", "/app/exporter.py"]
