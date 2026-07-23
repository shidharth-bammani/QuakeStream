
FROM python:3.12-slim
 
WORKDIR /app
 
# deps first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
COPY producer.py .
 
# unbuffered so logs show up live in `docker compose logs`
ENV PYTHONUNBUFFERED=1
 
CMD ["python", "producer.py"]
 