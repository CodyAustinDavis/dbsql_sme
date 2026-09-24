FROM python:3.12-slim

# Run as a non-root user.
RUN useradd --create-home --uid 10001 exporter
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY auth.py exporter.py prometheus_sink.py warehouse_monitor.py ./

USER exporter
EXPOSE 9877

# All configuration is via environment variables (see README): WAREHOUSE_WORKSPACE_MAP,
# DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET, POLL_INTERVAL_SECONDS, METRICS_PORT.
CMD ["python", "exporter.py"]
