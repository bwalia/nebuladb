# NebulaDB examples — agents HTTP service (Claude over MCP).
FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir 'fastapi>=0.115' 'uvicorn[standard]>=0.32'

COPY nebula_agents.py seed.py run.py server.py ./

ENV PYTHONUNBUFFERED=1 \
    NEBULA_MCP_URL=http://127.0.0.1:8090/mcp \
    PORT=8088

EXPOSE 8088

USER nobody
CMD ["sh", "-c", "exec uvicorn server:app --host 0.0.0.0 --port ${PORT:-8088}"]
