FROM python:3.11-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependencies manifest
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy system source code
COPY . .

# Expose ports
EXPOSE 8000 50051 50052 50053

CMD ["python", "coordinator/coordinator_service.py"]
