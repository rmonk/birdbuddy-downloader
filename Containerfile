# Stage 1: Export lightweight YOLO nano ONNX model for bird detection
FROM python:3.11-slim AS model-builder
WORKDIR /build
RUN pip install --no-cache-dir ultralytics && \
    python3 -c "from ultralytics import YOLO; YOLO('yolo26n.pt').export(format='onnx', imgsz=640, opset=12)"

# Stage 2: Final runtime container image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output for real-time logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bundled YOLO nano ONNX model from builder stage
RUN mkdir -p /app/models
COPY --from=model-builder /build/yolo26n.onnx /app/models/yolo26n.onnx

# Copy downloader application script
COPY downloader.py .

# Expose default download and database volumes
VOLUME ["/app/downloads", "/app/data"]

# Expose web dashboard port
EXPOSE 8080

# Default entrypoint runs downloader script
ENTRYPOINT ["python", "downloader.py"]
