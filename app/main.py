import asyncio
import random

import logging
import uuid
from pythonjsonlogger import jsonlogger
import time
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse

import os
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.trace import Status, StatusCode

# Экспорт трейсов уже настроен ниже вручную; встроенная телеметрия FastAPI
# добавила бы второй экспортёр с неверным путем (/v1/traces/v1/traces)
app = FastAPI(telemetry={"auto_configure": False})

otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318/v1/traces")

resource = Resource(attributes={"service.name": "lab2-service"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint)))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("lab2-service")

FastAPIInstrumentor.instrument_app(app)

REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)

ERRORS = Counter(
    "http_errors_total",
    "Total HTTP 5xx errors",
    ["method", "path"],
)

LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5],
)

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    span = trace.get_current_span()
    span_context = span.get_span_context()
    trace_id = format(span_context.trace_id, "032x") if span_context.trace_id else "no-trace"
    duration = time.perf_counter() - start

    REQUESTS.labels(
        method=request.method,
        path=request.url.path,
        status=response.status_code,
    ).inc()

    LATENCY.labels(
        method=request.method,
        path=request.url.path,
    ).observe(duration)

    if response.status_code >= 500:
        ERRORS.labels(
            method=request.method,
            path=request.url.path,
        ).inc()

    logger.info(
        "request handled",
        extra={
            "trace_id": trace_id,
            "path": request.url.path,
            "status": response.status_code,
        },
    )

    response.headers["X-Trace-Id"] = trace_id
    return response

logger = logging.getLogger("lab2")
logger.setLevel(logging.INFO)

log_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(message)s %(trace_id)s %(path)s %(status)s"
)
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)

PAGE = """
<h1>Lab 2 service</h1>
<button onclick="call('/error')">Создать ошибку</button>
<button onclick="call('/slow')">Создать задержку</button>
<button onclick="call('/load', 'POST')">Нагрузка</button>
<pre id="out"></pre>
<script>
async function call(url, method = 'GET') {
  const r = await fetch(url, { method });
  document.getElementById('out').textContent = url + ' -> ' + r.status;
}
</script>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


@app.get("/ok")
def ok():
    return {"status": "ok"}


@app.get("/error")
def error():
    span = trace.get_current_span()
    span.set_status(Status(StatusCode.ERROR))
    span.set_attribute("error", True)
    raise HTTPException(status_code=500, detail="simulated error")


@app.get("/slow")
async def slow():
    with tracer.start_as_current_span("slow-dependency"):
        await asyncio.sleep(random.uniform(1, 3))
    return {"status": "slow"}


@app.post("/load")
async def load(n: int = 50):
    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        await asyncio.gather(*[client.get("/ok") for _ in range(n)])
    return {"sent": n}

@app.post("/alert-webhook")
async def alert_webhook(request: Request):
    payload = await request.json()
    alerts = payload.get("alerts", [])
    for alert in alerts:
        logger.warning(
            "alert %s: %s - %s",
            alert.get("status"),
            alert.get("labels", {}).get("alertname"),
            alert.get("annotations", {}).get("summary"),
        )
    return {"received": len(alerts)}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)