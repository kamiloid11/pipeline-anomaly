FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src

RUN useradd -m appuser && chown -R appuser /app
USER appuser
ENV PYTHONPATH=/app/src

CMD ["bash", "-c", "sleep infinity"]