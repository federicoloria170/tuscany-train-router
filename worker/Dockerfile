# Build context = radice del repo
FROM python:3.12-slim
WORKDIR /app
COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY worker/graph.py worker/main.py ./
COPY data/network.json ./data/network.json
CMD ["python", "-u", "main.py"]
