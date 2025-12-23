FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY schemahub/ ./schemahub/
COPY config/ ./config/

# Set default command to ingest
ENTRYPOINT ["python", "-m", "schemahub.cli"]
CMD ["ingest"]
