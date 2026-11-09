# Build context = radice del repo
FROM python:3.12-slim
WORKDIR /app
COPY simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY simulator/simulator.py .
COPY data/network.json ./data/network.json
CMD ["python", "-u", "simulator.py"]
