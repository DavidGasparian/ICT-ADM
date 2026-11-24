"""
Сервис пишет записи в primary PostgreSQL, читает список — с реплики (read/write splitting).
"""

from __future__ import annotations

import html
import os
import random
import time
from contextlib import asynccontextmanager
from urllib.parse import urlencode, urlparse

import psycopg2
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from prometheus_client import Counter, CONTENT_TYPE_LATEST, generate_latest

WRITE_DSN = os.getenv(
    "WRITE_DATABASE_URL",
    "postgresql://postgres:postgres@pg-primary:5432/lab4",
)
READ_DSN = os.getenv(
    "READ_DATABASE_URL",
    "postgresql://postgres:postgres@pg-standby:5432/lab4",
)

WRITES_TOTAL = Counter("lab4_writes_total", "Записей создано через сервис")
WRITE_ERRORS_TOTAL = Counter("lab4_write_errors_total", "Неудачных попыток записи")
READ_ERRORS_TOTAL = Counter("lab4_read_errors_total", "Неудачных попыток чтения")
LOADS_TOTAL = Counter("lab4_load_batches_total", "Нажатий кнопки «Налить нагрузку»")

ADJECTIVES = [
    "рыжий", "сонный", "быстрый", "ленивый", "странный", "важный",
    "серебряный", "квантовый", "последний", "секретный", "мокрый", "громкий",
]
NOUNS = [
    "кот", "самовар", "контейнер", "стендап", "баг", "коммит", "докер",
    "кластер", "слон", "слайд", "отчёт", "WAL", "снапшот", "деплой",
]


def connect(dsn: str):
    """Короткий коннект под одну операцию — чтобы падение узла было видно сразу."""
    return psycopg2.connect(
        dsn,
        connect_timeout=2,
        application_name="lab4-service",
        options="-c timezone=Europe/Moscow",
    )


def ensure_schema() -> None:
    """Таблица создаётся на primary; на реплику она приезжает через WAL."""
    last_error: Exception | None = None
    for _ in range(30):
        try:
            conn = connect(WRITE_DSN)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS lab4_records (
                            id         bigserial PRIMARY KEY,
                            title      text NOT NULL,
                            payload    text NOT NULL,
                            created_at timestamptz NOT NULL DEFAULT now()
                        )
                        """
                    )
                conn.commit()
            finally:
                conn.close()
            return
        except Exception as exc:  # primary может ещё подниматься
            last_error = exc
            time.sleep(1)
    print(f"[app] schema init failed: {last_error}", flush=True)


def random_title() -> str:
    return f"{random.choice(ADJECTIVES)} {random.choice(NOUNS)} #{random.randint(100, 999)}"


def insert_one(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO lab4_records (title, payload) VALUES (%s, %s) RETURNING id",
            (random_title(), "created via web"),
        )
        row = cur.fetchone()
    conn.commit()
    return int(row[0])


def node_name(dsn: str) -> str:
    parsed = urlparse(dsn)
    return f"{parsed.hostname or '?'}:{parsed.port or 5432}/{parsed.path.lstrip('/') or '?'}"


def fetch_write_state() -> dict:
    state = {
        "ok": False,
        "count": None,
        "lag_seconds": None,
        "lag_bytes": None,
        "replica_state": None,
        "error": None,
    }
    try:
        conn = connect(WRITE_DSN)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM lab4_records")
                state["count"] = int(cur.fetchone()[0])
                cur.execute(
                    """
                    SELECT state,
                           pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint,
                           EXTRACT(EPOCH FROM replay_lag)::float
                    FROM pg_stat_replication
                    ORDER BY application_name
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
            if row:
                state["replica_state"] = row[0]
                state["lag_bytes"] = int(row[1]) if row[1] is not None else None
                state["lag_seconds"] = float(row[2]) if row[2] is not None else None
            state["ok"] = True
        finally:
            conn.close()
    except Exception as exc:
        state["error"] = str(exc).strip()
    return state


def fetch_read_state(limit: int = 20) -> dict:
    state = {
        "ok": False,
        "count": None,
        "in_recovery": None,
        "records": [],
        "error": None,
    }
    try:
        conn = connect(READ_DSN)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery()")
                state["in_recovery"] = bool(cur.fetchone()[0])
                cur.execute("SELECT count(*) FROM lab4_records")
                state["count"] = int(cur.fetchone()[0])
                cur.execute(
                    """
                    SELECT id, title, payload, created_at
                    FROM lab4_records
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                state["records"] = cur.fetchall()
            state["ok"] = True
        finally:
            conn.close()
    except Exception as exc:
        state["error"] = str(exc).strip()
        READ_ERRORS_TOTAL.inc()
    return state


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_schema()
    yield


app = FastAPI(title="Lab 4 — PostgreSQL replication demo", lifespan=lifespan)


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
def healthz():
    return {"status": "ok", "write": node_name(WRITE_DSN), "read": node_name(READ_DSN)}


@app.post("/records")
def create_record():
    try:
        conn = connect(WRITE_DSN)
        try:
            record_id = insert_one(conn)
        finally:
            conn.close()
        WRITES_TOTAL.inc()
        message = f"Создана запись #{record_id} в primary. Читаем-то мы с реплики, так что она появится там не сразу."
    except Exception as exc:
        WRITE_ERRORS_TOTAL.inc()
        message = f"Запись не удалась: {exc}"
    return RedirectResponse("/?" + urlencode({"msg": message}), status_code=303)


@app.post("/load")
def load(count: int = Query(default=50000, ge=1, le=100000)):
    LOADS_TOTAL.inc()
    created = 0
    error = None
    try:
        conn = connect(WRITE_DSN)
        try:
            for _ in range(count):
                insert_one(conn)
                WRITES_TOTAL.inc()
                created += 1
        finally:
            conn.close()
    except Exception as exc:
        error = str(exc)

    if error:
        message = f"Залито только {created} из {count}: {error}"
    else:
        message = (
            f"Залито {created} записей в primary. "
        )
    return RedirectResponse("/?" + urlencode({"msg": message}), status_code=303)


@app.get("/", response_class=HTMLResponse)
def index(msg: str = ""):
    write_state = fetch_write_state()
    read_state = fetch_read_state()

    write_count = write_state["count"]
    read_count = read_state["count"]
    lag_cards = ""

    # replay_lag — мгновенная метрика и на всплеске может не успеть обновиться.
    # Поэтому «размер» лага для пользователя показываем в записях, а секунды —
    # отдельной подписью, как есть.
    display_lag = write_state["lag_seconds"]
    delta = None

    if write_state["ok"] and read_state["ok"] and write_count is not None and read_count is not None:
        delta = write_count - read_count
        if delta > 0:
            lag_cards = f"""
            <div class="banner warn">
              <b>Реплика отстаёт.</b> На primary {write_count} записей, на реплике {read_count}
              (разница {delta}). Именно поэтому в списке ниже нет только что созданных записей.
            </div>"""

    if not write_state["ok"]:
        lag_cards += f"""
        <div class="banner error">
          <b>Primary недоступен.</b> Запись не проходит: <code>{html.escape(write_state["error"] or "")}</code>
        </div>"""
    if not read_state["ok"]:
        lag_cards += f"""
        <div class="banner error">
          <b>Реплика недоступна.</b> Чтение не проходит: <code>{html.escape(read_state["error"] or "")}</code>
        </div>"""

    if read_state["ok"] and read_state["in_recovery"] is False:
        lag_cards += """
        <div class="banner info">
          <b>Читающий узел уже не в recovery.</b> Похоже, это бывшая реплика после промоушена —
          теперь она primary, и запись/чтение идут в неё.
        </div>"""

    def fmt_count(value) -> str:
        return "—" if value is None else str(value)

    def fmt_lag(value) -> str:
        if value is None:
            return "—"
        if value < 1:
            return f"{value * 1000:.0f} мс"
        return f"{value:.1f} c"

    lag_card_value = f"{delta} запис." if delta and delta > 0 else fmt_lag(display_lag)

    write_status = "ok" if write_state["ok"] else "down"
    read_status = "ok" if read_state["ok"] else "down"
    recovery_text = (
        "n/a" if read_state["in_recovery"] is None
        else ("да, hot standby" if read_state["in_recovery"] else "нет, это primary")
    )

    rows = "".join(
        f"""
        <tr>
          <td class="mono">{rec_id}</td>
          <td>{html.escape(title)}</td>
          <td class="muted">{html.escape(created_at.strftime("%H:%M:%S"))}</td>
          <td class="muted">{html.escape(payload)}</td>
        </tr>"""
        for rec_id, title, payload, created_at in read_state["records"]
    )
    if not rows:
        rows = '<tr><td colspan="4" class="muted empty">Пока пусто. Нажми «Создать запись».</td></tr>'

    flash = f'<div class="banner info">{html.escape(msg)}</div>' if msg else ""

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lab 4 — PostgreSQL replication</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; background: #eef1f5; color: #1c2430;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }}
    .wrap {{ max-width: 1080px; margin: 0 auto; padding: 32px 20px 48px; }}
    h1 {{ margin: 0 0 6px; font-size: 30px; letter-spacing: -0.4px; }}
    .sub {{ color: #5d6b7e; margin-bottom: 24px; }}
    .cards {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 18px; }}
    .card {{ background: #fff; border: 1px solid #dde3ea; border-radius: 12px; padding: 14px 16px; }}
    .card .label {{ font-size: 12px; color: #6a7688; text-transform: uppercase; letter-spacing: .5px; }}
    .card .value {{ font-size: 22px; font-weight: 650; margin-top: 6px; }}
    .card .hint {{ font-size: 12px; color: #7c8898; margin-top: 4px; word-break: break-all; }}
    .dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }}
    .ok .dot {{ background: #1f9d55; }}
    .down .dot {{ background: #d64545; }}
    .banner {{ border-radius: 10px; padding: 12px 16px; margin: 12px 0; border: 1px solid; font-size: 14px; line-height: 1.45; }}
    .banner.warn {{ background: #fff4e5; border-color: #f0c987; }}
    .banner.error {{ background: #fdecec; border-color: #e6a3a3; }}
    .banner.info {{ background: #eaf2ff; border-color: #b9cdf0; }}
    .toolbar {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin: 20px 0; }}
    button, .refresh {{
      border: 0; border-radius: 9px; padding: 11px 16px; font-size: 14px;
      font-weight: 600; cursor: pointer; text-decoration: none; display: inline-block;
    }}
    .primary {{ background: #2f6fed; color: #fff; }}
    .warn-btn {{ background: #f0a132; color: #2b1c00; }}
    .refresh {{ background: #fff; color: #2b3646; border: 1px solid #cfd7e2; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; border: 1px solid #dde3ea; }}
    th, td {{ padding: 10px 14px; text-align: left; border-bottom: 1px solid #eef1f5; font-size: 14px; }}
    th {{ background: #f8fafc; font-size: 12px; text-transform: uppercase; letter-spacing: .5px; color: #6a7688; }}
    tr:last-child td {{ border-bottom: 0; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
    .muted {{ color: #7c8898; }}
    .empty {{ text-align: center; padding: 28px; }}
    .footer {{ margin-top: 18px; color: #7c8898; font-size: 12px; line-height: 1.6; }}
    .footer code {{ color: #3b4657; }}
    h2 {{ font-size: 18px; margin: 26px 0 10px; }}
    @media (max-width: 800px) {{ .cards {{ grid-template-columns: 1fr 1fr; }} }}
  </style>
</head>
<body>
<div class="wrap">
  <h1>PostgreSQL: primary + реплика</h1>
  <div class="sub">Пишем в primary, читаем с реплики. Кнопка «Налить нагрузку» нужна, чтобы поймать replication lag.</div>

  <div class="cards">
    <div class="card {write_status}">
      <div class="label"><span class="dot"></span>write → primary</div>
      <div class="value">{fmt_count(write_count)}</div>
      <div class="hint">{html.escape(node_name(WRITE_DSN))}</div>
    </div>
    <div class="card {read_status}">
      <div class="label"><span class="dot"></span>read → replica</div>
      <div class="value">{fmt_count(read_count)}</div>
      <div class="hint">{html.escape(node_name(READ_DSN))}</div>
    </div>
    <div class="card">
      <div class="label">replica lag</div>
      <div class="value" style="font-size:{'20px' if delta and delta > 0 else '22px'}">{lag_card_value}</div>
      <div class="hint">{write_state["replica_state"] or "реплика не подключена"} · replay_lag {fmt_lag(write_state["lag_seconds"])}</div>
    </div>
    <div class="card">
      <div class="label">replica recovery</div>
      <div class="value" style="font-size:16px;margin-top:10px">{recovery_text}</div>
      <div class="hint">lag в байтах: {write_state["lag_bytes"] if write_state["lag_bytes"] is not None else "—"}</div>
    </div>
  </div>

  {flash}
  {lag_cards}

  <div class="toolbar">
    <form method="post" action="/records"><button class="primary" type="submit">Создать запись</button></form>
    <form method="post" action="/load?count=200"><button class="warn-btn" type="submit">Налить нагрузку</button></form>
    <a class="refresh" href="/">Обновить список</a>
  </div>

  <h2>Записи (читаем с реплики)</h2>
  <table>
    <thead><tr><th>id</th><th>заголовок</th><th>время</th><th>источник</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>

  <div class="footer">
    WRITE_DATABASE_URL: <code>{html.escape(node_name(WRITE_DSN))}</code><br>
    READ_DATABASE_URL: <code>{html.escape(node_name(READ_DSN))}</code><br>
    Метрики сервиса: <a href="/metrics">/metrics</a> · health: <a href="/healthz">/healthz</a>
  </div>
</div>
</body>
</html>"""
