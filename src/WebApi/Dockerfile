FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

EXPOSE 8080

# Use Gunicorn with optimized settings for performance
CMD ["gunicorn", "--workers=4", "--threads=2", "--worker-class=sync", "--bind=0.0.0.0:8080", "--timeout=30", "app:app"]