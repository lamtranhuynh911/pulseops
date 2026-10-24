# syntax=docker/dockerfile:1
# Container image for the PulseOps service (Python 3.12).
# Build: docker build -t pulseops-service:dev service
FROM python:3.12.15-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

WORKDIR /app
# Dependencies first, in their own layer, so code changes don't reinstall them.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project
COPY pulseops ./pulseops

ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER 10001
EXPOSE 8000
# Exec form, so uvicorn is PID 1 and gets SIGTERM: it then runs the shutdown, and the Kafka consumer
# leaves its group. One worker per pod; scale with replicas.
CMD ["uvicorn", "pulseops.main:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "20"]
