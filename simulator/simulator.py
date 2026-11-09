"""
TELEMETRY SIMULATOR: finge di essere i treni toscani.
Ogni TICK_SECONDS pubblica sullo stream Redis "tuscany-train-telemetry" la posizione di tutti i treni.
In produzione, al posto di questo script leggeresti dati reali (es. ViaggiaTreno).
"""
import json
import math
import os
import random
import time

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
STREAM = "tuscany-train-telemetry"
TICK_SECONDS = 2
TIME_SCALE = 12          # 1 secondo reale = 12 secondi simulati (così i treni si vedono muovere)
SPEED_KMH = 90

with open(os.getenv("DATA_PATH", "data/network.json"), encoding="utf-8") as f:
    STATIONS = {s["id"]: s for s in json.load(f)["stations"]}

# Linee = sequenze ordinate di stazioni. Il treno va avanti e indietro.
LINES = {
    "R-FI-PI (via Empoli)":   ["FI_SMN", "EMP", "PONT", "PI_C"],
    "R-FI-LU (via Pistoia)":  ["FI_SMN", "FI_RIF", "PO_C", "PT", "MT", "LU"],
    "R-PI-GR (costa)":        ["PI_C", "LI_C", "CEC", "GR"],
    "RV-FI-AR":               ["FI_CDM", "AR"],
    "R-EMP-SI":               ["EMP", "SI"],
}


def haversine_km(a, b):
    la1, lo1, la2, lo2 = map(math.radians, (a["lat"], a["lon"], b["lat"], b["lon"]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(h))


class Train:
    def __init__(self, train_id, line, ids, offset):
        self.id, self.line = train_id, line
        self.pts = [STATIONS[i] for i in ids]
        self.seg_len = [haversine_km(a, b) for a, b in zip(self.pts, self.pts[1:])]
        self.total = sum(self.seg_len)
        self.pos = offset * self.total       # km percorsi dall'inizio della linea
        self.dir = 1
        self.delay = random.choice([0, 0, 0, 2, 5, 8])

    def step(self, dt_seconds):
        self.pos += self.dir * SPEED_KMH * TIME_SCALE * dt_seconds / 3600
        if self.pos >= self.total:
            self.pos, self.dir = self.total, -1
            self.delay = random.choice([0, 0, 0, 2, 5, 8])
        elif self.pos <= 0:
            self.pos, self.dir = 0, 1
            self.delay = random.choice([0, 0, 0, 2, 5, 8])

    def snapshot(self):
        remaining, i = self.pos, 0
        while i < len(self.seg_len) - 1 and remaining > self.seg_len[i]:
            remaining -= self.seg_len[i]
            i += 1
        a, b = self.pts[i], self.pts[i + 1]
        f = min(max(remaining / self.seg_len[i], 0), 1)
        nxt = b if self.dir == 1 else a
        return {
            "train_id": self.id, "line": self.line,
            "lat": round(a["lat"] + (b["lat"] - a["lat"]) * f, 5),
            "lon": round(a["lon"] + (b["lon"] - a["lon"]) * f, 5),
            "next_station": nxt["name"], "delay_min": self.delay,
            "status": "in ritardo" if self.delay >= 5 else "in orario",
        }


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    trains = []
    for n, (line, ids) in enumerate(LINES.items()):
        for k, offset in enumerate((0.1, 0.6)):          # 2 treni per linea
            trains.append(Train(f"T{n}{k}", line, ids, offset))
    print(f"Simulo {len(trains)} treni", flush=True)
    last = time.time()
    while True:
        time.sleep(TICK_SECONDS)
        now = time.time()
        for t in trains:
            t.step(now - last)
        last = now
        payload = {"ts": int(now), "trains": [t.snapshot() for t in trains]}
        try:
            r.xadd(STREAM, {"payload": json.dumps(payload)}, maxlen=200, approximate=True)
        except redis.RedisError as exc:
            print("Redis non raggiungibile:", exc, flush=True)


if __name__ == "__main__":
    main()
