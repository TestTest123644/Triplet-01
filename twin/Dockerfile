# Lightweight Python 3.11 Image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install minimal OS dependencies for network & cryptography builds
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy Twin source package, dashboard, and mock components
COPY src/ ./src/
COPY dashboard.py ./
COPY mocks/ ./mocks/

# Expose MCP Command Server port and Streamlit Dashboard port
EXPOSE 8000 8501

# Set environment defaults
ENV PYTHONPATH=/app/src
ENV KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
ENV KAFKA_TOPIC="twin.sensors"
ENV MCP_HOST="0.0.0.0"
ENV MCP_PORT="8000"
ENV WAIT_FOR_KAFKA="true"
ENV SIM_SEED="42"

# Run Twin Layer unified runtime (10 Hz simulation stream + MCP server)
CMD ["python", "-m", "twin.main"]
