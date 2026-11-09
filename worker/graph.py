"""
IL CERVELLO: workflow LangGraph a 4 nodi.

   START -> [1 station_rag] -> [2 graph_routing] -> [3 schedule_matching] -> [4 llm_supervisor] -> END
                 |                    |
                 +-- errore --> END   +-- errore --> END

Regola d'oro: la MATEMATICA (NetworkX) calcola il percorso, l'LLM SPIEGA e RIFINISCE.
Gli LLM sbagliano i conti: non gli facciamo mai calcolare il tragitto.
"""
import json
import logging
import os
import random
from datetime import datetime, timedelta
from typing import Optional, TypedDict
from zoneinfo import ZoneInfo

import networkx as nx
from langgraph.graph import END, START, StateGraph
from rapidfuzz import fuzz, process

log = logging.getLogger("graph")

# ---------------------------------------------------------------------------
# 0. Dati caricati una volta sola all'avvio del processo
# ---------------------------------------------------------------------------
with open(os.getenv("DATA_PATH", "data/network.json"), encoding="utf-8") as f:
    _DATA = json.load(f)

STATIONS: dict[str, dict] = {s["id"]: s for s in _DATA["stations"]}

# Il grafo della rete: stazioni = nodi, tratte = archi con DUE pesi (minuti e prezzo).
GRAPH = nx.Graph()
for s in _DATA["stations"]:
    GRAPH.add_node(s["id"])
for e in _DATA["edges"]:
    GRAPH.add_edge(e["a"], e["b"], minutes=e["minutes"], price=e["price"], type=e["type"])

# Indice di ricerca per il RAG: ogni nome/alias punta all'id della sua stazione.
_ALIASES: list[tuple[str, str]] = []
for s in _DATA["stations"]:
    for text in [s["name"], *s["aliases"]]:
        _ALIASES.append((text.lower(), s["id"]))
_ALIAS_TEXTS = [t for t, _ in _ALIASES]

# ---------------------------------------------------------------------------
# LLM (Groq). Se manca la chiave il sistema funziona comunque, con risposte "di riserva".
# ---------------------------------------------------------------------------
_llm = None
if os.getenv("GROQ_API_KEY"):
    from langchain_groq import ChatGroq

    _llm = ChatGroq(model=os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"), temperature=0, max_tokens=2000)
else:
    log.warning("GROQ_API_KEY mancante: uso risposte di riserva senza LLM")


# ---------------------------------------------------------------------------
# 1. STATO CONDIVISO: la "valigetta" che passa da un nodo all'altro
# ---------------------------------------------------------------------------
class RouteState(TypedDict, total=False):
    # input
    request_id: str
    client_id: str
    origin_text: str
    destination_text: str
    criterion: str            # "shortest" | "cheapest"
    # compilati dai nodi
    origin: Optional[dict]
    destination: Optional[dict]
    path: list[str]
    total_minutes: int
    total_price: float
    alternative: Optional[dict]
    legs: list[dict]
    explanation: str
    error: Optional[str]


# ---------------------------------------------------------------------------
# NODO 1 - Station RAG
#   Retrieval: ricerca "fuzzy" nel database locale -> candidati
#   Generation: se ambiguo, l'LLM sceglie USANDO SOLO i candidati recuperati
# ---------------------------------------------------------------------------
def retrieve_candidates(text: str, k: int = 3) -> list[dict]:
    results = process.extract(text.lower(), _ALIAS_TEXTS, scorer=fuzz.WRatio, limit=12)
    seen, out = set(), []
    for _match, score, idx in results:
        sid = _ALIASES[idx][1]
        if sid in seen:
            continue
        seen.add(sid)
        out.append({"station": STATIONS[sid], "score": score})
        if len(out) == k:
            break
    return out


async def resolve_station(text: str) -> Optional[dict]:
    cands = retrieve_candidates(text)
    if not cands:
        return None
    best = cands[0]
    second = cands[1]["score"] if len(cands) > 1 else 0
    # Caso facile: un candidato vince nettamente -> niente LLM, risparmiamo tempo e soldi.
    if best["score"] >= 90 and best["score"] - second >= 5:
        return best["station"]
    # Caso ambiguo: chiediamo all'LLM, dandogli i candidati come contesto (questo è il RAG).
    if _llm:
        context = "\n".join(
            f'- id={c["station"]["id"]} | {c["station"]["name"]} | principale={c["station"]["main"]} '
            f'| servizi={", ".join(c["station"]["services"])}'
            for c in cands
        )
        prompt = (
            f'L\'utente ha scritto: "{text}".\nStazioni candidate:\n{context}\n\n'
            "Scegli la stazione più probabile. Se l'utente indica solo una città, scegli la stazione principale. "
            'Rispondi SOLO con JSON: {"id": "<id candidato>"} oppure {"id": null} se nessuna è plausibile.'
        )
        try:
            reply = await _llm.ainvoke(prompt)
            chosen = json.loads(reply.content.strip().strip("`").removeprefix("json").strip()).get("id")
            if chosen in STATIONS and chosen in {c["station"]["id"] for c in cands}:
                return STATIONS[chosen]
            return None
        except Exception:  # noqa: BLE001
            log.exception("LLM non disponibile nel RAG, uso il miglior candidato")
    return best["station"] if best["score"] >= 70 else None


async def station_rag_node(state: RouteState) -> dict:
    origin = await resolve_station(state["origin_text"])
    destination = await resolve_station(state["destination_text"])
    if not origin:
        return {"error": f'Non ho trovato la stazione di partenza "{state["origin_text"]}".'}
    if not destination:
        return {"error": f'Non ho trovato la stazione di arrivo "{state["destination_text"]}".'}
    if origin["id"] == destination["id"]:
        return {"error": "Partenza e arrivo coincidono."}
    return {"origin": origin, "destination": destination}


# ---------------------------------------------------------------------------
# NODO 2 - Graph Routing Engine (Dijkstra con NetworkX, zero LLM)
# ---------------------------------------------------------------------------
def _totals(path: list[str]) -> tuple[int, float]:
    edges = [GRAPH[a][b] for a, b in zip(path, path[1:])]
    return sum(e["minutes"] for e in edges), round(sum(e["price"] for e in edges), 2)


def graph_routing_node(state: RouteState) -> dict:
    o, d = state["origin"]["id"], state["destination"]["id"]
    chosen_weight = "minutes" if state["criterion"] == "shortest" else "price"
    other_weight = "price" if chosen_weight == "minutes" else "minutes"
    try:
        path = nx.shortest_path(GRAPH, o, d, weight=chosen_weight)        # Dijkstra
        alt_path = nx.shortest_path(GRAPH, o, d, weight=other_weight)     # per confronto
    except nx.NetworkXNoPath:
        return {"error": "Non esiste un collegamento ferroviario tra queste stazioni."}
    minutes, price = _totals(path)
    alt_minutes, alt_price = _totals(alt_path)
    alternative = None
    if alt_path != path:
        alternative = {
            "criterion": "cheapest" if chosen_weight == "minutes" else "shortest",
            "path": alt_path, "total_minutes": alt_minutes, "total_price": alt_price,
        }
    return {"path": path, "total_minutes": minutes, "total_price": price, "alternative": alternative}


# ---------------------------------------------------------------------------
# NODO 3 - Schedule matching (MOCK: simula orari e ritardi; in produzione chiameresti ViaggiaTreno/GTFS)
# ---------------------------------------------------------------------------
def schedule_matching_node(state: RouteState) -> dict:
    now = datetime.now(ZoneInfo("Europe/Rome"))
    t = (now + timedelta(minutes=10 - now.minute % 10)).replace(second=0, microsecond=0)  # prossimo :x0
    legs = []
    for a, b in zip(state["path"], state["path"][1:]):
        edge = GRAPH[a][b]
        rng = random.Random(f"{a}{b}{t:%H%M}")            # ritardo "casuale" ma ripetibile
        delay = rng.choice([0, 0, 0, 0, 2, 5, 10])
        dep = t
        arr = dep + timedelta(minutes=edge["minutes"] + delay)
        legs.append({
            "from": STATIONS[a]["name"], "to": STATIONS[b]["name"],
            "train_type": edge["type"], "train_number": f"{rng.randint(2000, 3999)}",
            "depart": dep.strftime("%H:%M"), "arrive": arr.strftime("%H:%M"),
            "delay_min": delay, "minutes": edge["minutes"], "price": edge["price"],
        })
        t = arr + timedelta(minutes=3)                    # 3 minuti per il cambio
    return {"legs": legs}


# ---------------------------------------------------------------------------
# NODO 4 - LLM Supervisor: verifica le preferenze e scrive la spiegazione
# ---------------------------------------------------------------------------
async def llm_supervisor_node(state: RouteState) -> dict:
    crit_it = "più breve" if state["criterion"] == "shortest" else "più economico"
    fallback = (
        f'Percorso {crit_it} da {state["origin"]["name"]} a {state["destination"]["name"]}: '
        f'{state["total_minutes"]} minuti, circa {state["total_price"]:.2f} €, '
        f'{len(state["legs"])} tratte.'
    )
    if not _llm:
        return {"explanation": fallback}
    facts = {
        "criterio_richiesto": crit_it,
        "partenza": state["origin"]["name"], "arrivo": state["destination"]["name"],
        "minuti_totali": state["total_minutes"], "prezzo_totale_eur": state["total_price"],
        "tratte": state["legs"],
        "alternativa": state.get("alternative"),
    }
    prompt = (
        "Sei l'assistente di un'app ferroviaria toscana. Ti do i risultati GIÀ CALCOLATI da un algoritmo: "
        "non ricalcolare e non inventare dati. Scrivi in italiano, in 3-4 frasi semplici: "
        "1) conferma che il percorso rispetta il criterio richiesto, 2) cita gli eventuali cambi e ritardi, "
        "3) se c'è un'alternativa, spiega in una frase il compromesso (tempo vs prezzo).\n\n"
        f"DATI:\n{json.dumps(facts, ensure_ascii=False)}"
    )
    try:
        reply = await _llm.ainvoke(prompt)
        return {"explanation": reply.content.strip()}
    except Exception:  # noqa: BLE001
        log.exception("LLM non disponibile nel supervisor")
        return {"explanation": fallback}


# ---------------------------------------------------------------------------
# CONDIZIONI DI TRANSIZIONE: dopo un nodo, "continua" oppure "esci con errore"
# ---------------------------------------------------------------------------
def continue_or_stop(state: RouteState) -> str:
    return "stop" if state.get("error") else "continue"


def build_graph(checkpointer=None):
    g = StateGraph(RouteState)
    g.add_node("station_rag", station_rag_node)
    g.add_node("graph_routing", graph_routing_node)
    g.add_node("schedule_matching", schedule_matching_node)
    g.add_node("llm_supervisor", llm_supervisor_node)

    g.add_edge(START, "station_rag")
    g.add_conditional_edges("station_rag", continue_or_stop, {"continue": "graph_routing", "stop": END})
    g.add_conditional_edges("graph_routing", continue_or_stop, {"continue": "schedule_matching", "stop": END})
    g.add_edge("schedule_matching", "llm_supervisor")
    g.add_edge("llm_supervisor", END)
    # Il checkpointer salva lo stato dopo OGNI nodo (qui: su Redis)
    return g.compile(checkpointer=checkpointer)
