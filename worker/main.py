"""
WORKER (il "cuoco")

Loop infinito:
   1. prende un ticket dalla coda Redis Stream "route-requests" (come membro del consumer group "workers")
   2. esegue il grafo LangGraph
   3. scrive il risultato sullo stream "route-results"
   4. conferma (XACK) il ticket -> solo ora Redis lo considera "fatto"

Consumer group = più worker condividono la stessa coda e ogni messaggio va a UN SOLO worker.
"""
import asyncio
import json
import logging
import os
import socket

import redis.asyncio as aioredis
from langgraph.checkpoint.redis.aio import AsyncRedisSaver
from redis.exceptions import ResponseError

from graph import build_graph

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REQUEST_STREAM, RESULT_STREAM, GROUP = "route-requests", "route-results", "workers"
CONSUMER = os.getenv("HOSTNAME", socket.gethostname())   # in K8s = nome del pod, unico per ogni worker


async def process(graph, r: aioredis.Redis, fields: dict) -> None:
    request_id = fields["request_id"]
    initial = {
        "request_id": request_id,
        "client_id": fields["client_id"],
        "origin_text": fields["origin"],
        "destination_text": fields["destination"],
        "criterion": fields.get("criterion", "shortest"),
    }
    try:
        # thread_id = identificatore dell'esecuzione: lo stato nel checkpointer è salvato sotto questo nome
        state = await graph.ainvoke(initial, config={"configurable": {"thread_id": request_id}})
        if state.get("error"):
            payload = {"ok": False, "error": state["error"]}
        else:
            payload = {
                "ok": True,
                "origin": state["origin"], "destination": state["destination"],
                "criterion": state["criterion"], "path": state["path"],
                "total_minutes": state["total_minutes"], "total_price": state["total_price"],
                "legs": state["legs"], "alternative": state.get("alternative"),
                "explanation": state.get("explanation", ""),
            }
    except Exception as exc:  # noqa: BLE001
        log.exception("Errore nel grafo")
        payload = {"ok": False, "error": f"Errore interno: {exc}"}

    payload.update({"request_id": request_id, "client_id": fields["client_id"]})
    await r.xadd(RESULT_STREAM, {"payload": json.dumps(payload, ensure_ascii=False)}, maxlen=10_000, approximate=True)


async def main() -> None:
    # socket_timeout deve essere MAGGIORE del block (5s) usato in xreadgroup
    r = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=30, socket_connect_timeout=10)
    try:
        await r.xgroup_create(REQUEST_STREAM, GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    async with AsyncRedisSaver.from_conn_string(REDIS_URL) as checkpointer:
        await checkpointer.asetup()                       # crea gli indici necessari su Redis
        graph = build_graph(checkpointer)
        log.info("Worker %s pronto, in ascolto su %s", CONSUMER, REQUEST_STREAM)

        while True:
            try:
                resp = await r.xreadgroup(GROUP, CONSUMER, {REQUEST_STREAM: ">"}, count=1, block=5000)
            except (TimeoutError, ConnectionError, aioredis.RedisError) as exc:
                log.warning("Redis non risponde (%s), riprovo...", exc)
                await asyncio.sleep(1)
                continue
            for _stream, messages in resp or []:
                for msg_id, fields in messages:
                    log.info("Elaboro %s", fields.get("request_id"))
                    await process(graph, r, fields)
                    await r.xack(REQUEST_STREAM, GROUP, msg_id)

if __name__ == "__main__":
    asyncio.run(main())
