# Tuscany Train Router
Progetto didattico: Kubernetes + Redis Streams + LangGraph (Groq) + KEDA + WebSocket.

Ricorda , prima di iniziare di :
1. Copia `.env.example` in `.env` e inserisci la chiave Groq.
2. Test rapido: `docker compose up --build` -> http://localhost:8080
3. Su Kubernetes: segui la guida passo-passo nella chat.

### Separazione dei Ruoli

-Kubernetes decide quanti programmi far girare e dove.
-LangGraph decide quali passi esegue un programma al suo interno.

```
┌─────────────────────────────────────────────────────────────────────┐
│ LIVELLO 0 - IL TUO PORTATILE (Windows / macOS / Linux)              │
│                                                                     │     
│   Tu al terminale:  kubectl apply ...   kubectl get pods ...        │
│        │            (kubectl = telecomando del cluster, solo per te)│
│        ▼                                                            │
│ ┌───────────────────────────────────────────────────────────────┐   │
│ │ LIVELLO 1 - MINIKUBE (dentro Docker)                          │   │
│ │ È un "mini cluster Kubernetes finto" con un solo nodo.        │   │
│ │ Serve solo a simulare un cluster vero sul tuo PC.             │   │
│ │                                                               │   │
│ │ ┌─────────────────────────────────────────────────────────┐   │   │
│ │ │ LIVELLO 2 - KUBERNETES (il direttore d'orchestra)       │   │   │
│ │ │  • API Server: riceve i tuoi comandi kubectl            │   │   │
│ │ │  • Scheduler: decide su quale nodo far girare un pod    │   │   │
│ │ │  • Controller: se un pod muore, ne crea un altro        │   │   │
│ │ │  • KEDA: guarda la coda Redis e dice "servono N worker" │   │   │
│ │ │                                                         │   │   │
│ │ │ ┌─────────────────────────────────────────────────────┐ │   │   │
│ │ │ │ LIVELLO 3 - I POD (namespace tuscany-router)        │ │   │   │
│ │ │ │                                                     │ │   │   │
│ │ │ │  [frontend] [backend] [simulator] [redis] [postgres]│ │   │   │
│ │ │ │                                                     │ │   │   │
│ │ │ │  [worker] [worker] [worker]  ← KEDA li accende      │ │   │   │
│ │ │ │     │                          o li spegne          │ │   │   │
│ │ │ │     ▼                                               │ │   │   │
│ │ │ │ ┌─────────────────────────────────────────────────┐ │ │   │   │
│ │ │ │ │ LIVELLO 4 - DENTRO UN POD WORKER                │ │ │   │   │
│ │ │ │ │  Container Docker                               │ │ │   │   │
│ │ │ │ │   └─ python main.py   (il loop sulla coda)      │ │ │   │   │
│ │ │ │ │       └─ LANGGRAPH  ◄── QUI vive LangGraph      │ │ │   │   │
│ │ │ │ │           Nodo 1 → Nodo 2 → Nodo 3 → Nodo 4     │ │ │   │   │
│ │ │ │ └─────────────────────────────────────────────────┘ │ │   │   │
│ │ │ └─────────────────────────────────────────────────────┘ │   │   │
│ │ └─────────────────────────────────────────────────────────┘   │   │
│ └───────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

# Funzione di Redis 

```
   Utente ──► Backend ──► REDIS (coda "route-requests") ◄──┐
                               │                           │
                               │ KEDA legge quanti         │ il worker (con
                               │ messaggi ci sono          │ LangGraph dentro)
                               ▼                           │ prende il messaggio
                        ┌─────────────┐                    │
                        │    KEDA     │                    │
                        └──────┬──────┘                    │
                               │ "Coda piena: serve 1 pod" │
                               ▼                           │
                        ┌─────────────┐  crea il pod       │
                        │ KUBERNETES  │ ───────────────────┘
                        └─────────────┘

```
1-Kubernetes e KEDA guardano solo il numero di messaggi in coda e accendono o spengono pod.
2-Il worker non sa chi lo ha acceso. Appena parte, legge la coda ed esegue il grafo.
3-Redis è l’unico terreno comune.

# Dove passa la richiesta nel sistema , in modo cronologico

```
TEMPO  CHI AGISCE            COSA SUCCEDE
─────  ────────────────────  ───────────────────────────────────────────
t=0    Backend               Scrive il messaggio su Redis
t=1    KEDA                  Vede "1 messaggio in coda, 0 worker"
t=2    Kubernetes            Crea un pod worker (scarica l'immagine, avvia)
t=8    Worker (python)       Parte e legge il messaggio
t=9    LangGraph             Esegue Nodo 1 → 2 → 3 → 4
t=15   Worker                Scrive il risultato su Redis
t=16   Backend               Lo inoltra al browser via WebSocket
...
t=75   KEDA + Kubernetes     Coda vuota da 60s: spengono il pod (→ 0)
```