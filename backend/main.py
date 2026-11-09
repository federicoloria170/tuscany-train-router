"""
BACKEND / GATEWAY  (il "cameriere" del ristorante)

Cosa fa:
  1. Tiene aperte le connessioni WebSocket con i browser.
  2. Quando un utente chiede un percorso -> scrive un messaggio su Redis Stream "route-requests".
  3. Ascolta due stream di Redis e inoltra ai browser:
       - "route-results"             -> risposta del calcolo (solo al client che l'ha chiesta)
       - "tuscany-train-telemetry"   -> posizioni treni (a TUTTI i client)
  4. Salva i percorsi calcolati su PostgreSQL.
Non fa MAI calcoli pesanti: quelli sono compito dei worker.
"""
import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Literal

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError
from redis.exceptions import ResponseError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gateway")

# --- Configurazione (arriva dalle variabili d'ambiente, impostate da Docker/K8s) ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://train:train@localhost:5432/trainrouter")
DATA_PATH = os.getenv("DATA_PATH", "data/network.json")

REQUEST_STREAM = "route-requests"
RESULT_STREAM = "route-results"
TELEMETRY_STREAM = "tuscany-train-telemetry"
WORKER_GROUP = "workers"


# --- Validazione dei messaggi in arrivo dal browser ---
class RouteRequest(BaseModel):
    origin: str = Field(min_length=2, max_length=80)
    destination: str = Field(min_length=2, max_length=80)
    criterion: Literal["shortest", "cheapest"] = "shortest"


class ConnectionManager:
    """Registro delle connessioni WebSocket aperte: client_id -> WebSocket."""

    def __init__(self) -> None:
        self.active: dict[str, WebSocket] = {}

    async def connect(self, client_id: str, ws: WebSocket) -> None:
        await ws.accept()
        self.active[client_id] = ws

    def disconnect(self, client_id: str) -> None:
        self.active.pop(client_id, None)

    async def send_to(self, client_id: str, message: dict) -> None:
        ws = self.active.get(client_id)
        if ws:
            try:
                await ws.send_json(message)
            except Exception:
                self.disconnect(client_id)

    async def broadcast(self, message: dict) -> None:
        for client_id in list(self.active):
            await self.send_to(client_id, message)


manager = ConnectionManager()


async def retry(fn, what: str, attempts: int = 30):
    """All'avvio in Kubernetes i pod partono insieme: Redis/Postgres potrebbero non essere pronti. Riproviamo."""
    for i in range(attempts):
        try:
            return await fn()
        except Exception as exc:  # noqa: BLE001
            log.warning("%s non pronto (%s) tentativo %d/%d", what, exc, i + 1, attempts)
            await asyncio.sleep(2)
    raise RuntimeError(f"Impossibile connettersi a {what}")


async def ensure_consumer_group(r: aioredis.Redis) -> None:
    """Crea lo stream e il consumer group. Lo facciamo QUI (e non solo nel worker) perché KEDA
    deve poter leggere il 'lag' della coda anche quando ci sono 0 worker accesi."""
    try:
        await r.xgroup_create(REQUEST_STREAM, WORKER_GROUP, id="0", mkstream=True)
        log.info("Consumer group creato")
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def init_db(pool: asyncpg.Pool) -> None:
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_routes (
            id SERIAL PRIMARY KEY,
            request_id TEXT UNIQUE,
            origin TEXT, destination TEXT, criterion TEXT,
            total_minutes INT, total_price NUMERIC(6,2),
            explanation TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        )
        """
    )


async def read_stream_forever(r: aioredis.Redis, stream: str, handler) -> None:
    """Legge uno stream per sempre, passando ogni messaggio a `handler`.
    Parte dall'ultimo messaggio esistente: ci interessano solo i messaggi NUOVI.
    (Usiamo l'ID esplicito e non '$' per non perdere messaggi tra una chiamata e l'altra.)"""
    last = await r.xrevrange(stream, count=1)
    last_id = last[0][0] if last else "0-0"
    while True:
        try:
            response = await r.xread({stream: last_id}, count=50, block=5000)
            for _stream, messages in response or []:
                for msg_id, fields in messages:
                    last_id = msg_id
                    await handler(fields)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("Errore nel listener di %s", stream)
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=30, socket_connect_timeout=10)
    await retry(app.state.redis.ping, "Redis")
    await ensure_consumer_group(app.state.redis)

    app.state.db = await retry(lambda: asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5), "PostgreSQL")
    await init_db(app.state.db)

    with open(DATA_PATH, encoding="utf-8") as f:
        app.state.stations = json.load(f)["stations"]

    async def on_result(fields: dict) -> None:
        payload = json.loads(fields["payload"])
        client_id = payload.pop("client_id", None)
        if payload.get("ok"):
            try:
                await app.state.db.execute(
                    """INSERT INTO saved_routes (request_id, origin, destination, criterion,
                       total_minutes, total_price, explanation)
                       VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING""",
                    payload["request_id"], payload["origin"]["name"], payload["destination"]["name"],
                    payload["criterion"], payload["total_minutes"], payload["total_price"],
                    payload.get("explanation", ""),
                )
            except Exception:  # noqa: BLE001
                log.exception("Salvataggio su Postgres fallito")
        if client_id:
            await manager.send_to(client_id, {"type": "route_result", **payload})

    async def on_telemetry(fields: dict) -> None:
        await manager.broadcast({"type": "telemetry", **json.loads(fields["payload"])})

    tasks = [
        asyncio.create_task(read_stream_forever(app.state.redis, RESULT_STREAM, on_result)),
        asyncio.create_task(read_stream_forever(app.state.redis, TELEMETRY_STREAM, on_telemetry)),
    ]
    yield
    for t in tasks:
        t.cancel()
    await app.state.db.close()
    await app.state.redis.aclose()


app = FastAPI(title="Tuscany Train Router - Gateway", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ------------------------- REST -------------------------
@app.get("/health")
async def health():
    """Usato da Kubernetes per sapere se il pod è vivo/pronto."""
    return {"status": "ok", "connected_clients": len(manager.active)}


@app.get("/api/stations")
async def stations():
    return app.state.stations


@app.get("/api/routes")
async def saved_routes():
    rows = await app.state.db.fetch("SELECT * FROM saved_routes ORDER BY created_at DESC LIMIT 20")
    return [dict(r) for r in rows]


# ------------------------- WEBSOCKET -------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    client_id = str(uuid.uuid4())
    await manager.connect(client_id, ws)
    await ws.send_json({"type": "connected", "client_id": client_id})
    try:
        while True:
            data = await ws.receive_json()
            if data.get("type") != "route_request":
                await ws.send_json({"type": "error", "message": "Tipo di messaggio sconosciuto"})
                continue
            try:
                req = RouteRequest.model_validate(data)
            except ValidationError:
                await ws.send_json({"type": "error", "message": "Compila origine e destinazione (min. 2 caratteri)."})
                continue

            request_id = str(uuid.uuid4())
            # Scriviamo il "ticket" sulla bacheca (Redis Stream). I worker lo prenderanno.
            await app.state.redis.xadd(
                REQUEST_STREAM,
                {"request_id": request_id, "client_id": client_id, **req.model_dump()},
                maxlen=10_000, approximate=True,
            )
            await ws.send_json({"type": "queued", "request_id": request_id})
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(client_id)
