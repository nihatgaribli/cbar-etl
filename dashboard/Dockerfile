# Build context is the repository root (see docker-compose.yml): the app reuses etl/config.py
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

COPY dashboard/requirements.txt .
RUN pip install -r requirements.txt

COPY etl/__init__.py etl/config.py etl/
COPY dashboard/app.py dashboard/
COPY .streamlit/ .streamlit/

RUN useradd --create-home --uid 1000 app
USER app

EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
