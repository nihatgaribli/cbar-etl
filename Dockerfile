FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

# Dependencies first: this layer stays cached while only the code changes
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY etl/ etl/
COPY sql/ sql/

RUN useradd --create-home --uid 1000 etl
USER etl

ENTRYPOINT ["python", "-m", "etl.main"]
CMD ["--days", "30"]
