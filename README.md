# Test rapido con Docker 

Così verifichi che il codice funzioni prima di aggiungere la complessità di Kubernetes.
```
   docker compose up --build
```

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

# Guida all'Installazione e Distribuzione del Cluster

Questa guida contiene le istruzioni passo-passo per installare gli strumenti necessari, avviare un cluster Minikube localmente, configurare KEDA ed eseguire l'applicazione.

---

## 1. Installazione degli Strumenti (Minikube e kubectl)

Assicurati di avere già **Docker** installato sul sistema.

### Windows (PowerShell come Amministratore)
```powershell
winget install Kubernetes.minikube
winget install Kubernetes.kubectl
```

### macOS (con Homebrew)
```bash
brew install minikube kubectl
```

### Linux (Debian / Ubuntu)
1. Segui le istruzioni ufficiali su [minikube.sigs.k8s.io/docs/start](https://minikube.sigs.k8s.io/docs/start).
2. Installa `kubectl`:
```bash
sudo snap install kubectl --classic
```

### Verifica Installazione
Chiudi e riapri il terminale, quindi verifica le versioni installate:
```bash
minikube version
kubectl version --client
```

---

## 2. Avvio del Cluster Minikube

Avvia il cluster specificando le risorse (regola i valori in base alla RAM disponibile):

```bash
minikube start --driver=docker --cpus=4 --memory=6144
```
> **Nota:** Se il tuo portatile ha poca RAM, usa la configurazione ridotta:
> ```bash
> minikube start --driver=docker --cpus=2 --memory=4096
> ```

Verifica che il nodo sia pronto ed in stato **Ready**:
```bash
kubectl get nodes
```

---

## 3. Installazione di KEDA

Installazione dell'add-on KEDA per l'autoscaling (da eseguire una sola volta):

```bash
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.16.1/keda-2.16.1.yaml
```

Attendi circa un minuto e verifica che i pod di KEDA siano in stato **Running**:
```bash
kubectl get pods -n keda
```
*(Se la versione non fosse più disponibile, consulta le ultime release nel repository ufficiale `kedacore/keda`)*.

---

## 4. Build delle Immagini Docker dentro Minikube

Le immagini devono essere costruite nel Docker interno di Minikube. Esegui i comandi dalla **radice del progetto**.

> ⚠️ **Importante:** Il comando `docker-env` si applica solo al terminale corrente. Se ne apri un altro, va rieseguito.

### Linux / macOS / Git Bash
```bash
eval $(minikube docker-env)
./build-images.sh
```

### Windows PowerShell
```powershell
& minikube -p minikube docker-env --shell powershell | Invoke-Expression
foreach ($s in "backend","worker","simulator","frontend") { docker build -f "$s/Dockerfile" -t "tuscany/${s}:dev" . }
```

---

## 5. Caricamento del Progetto nel Cluster

Esegui in sequenza i seguenti passaggi per distribuire l'applicazione:

### 1. Creazione del Namespace
```bash
kubectl apply -f k8s/00-namespace.yaml
```

### 2. Configurazione dei Segreti
Inserisci le credenziali nel cluster:
```bash
kubectl create secret generic app-secrets -n tuscany-router \
  --from-literal=GROQ_API_KEY=gsk_la_tua_chiave \
  --from-literal=POSTGRES_PASSWORD=una_password_a_tua_scelta
```

### 3. Avvio Database (Redis e Postgres)
```bash
kubectl apply -f k8s/10-redis.yaml -f k8s/11-postgres.yaml
```

### 4. Avvio delle Applicazioni
```bash
kubectl apply -f k8s/20-backend.yaml -f k8s/30-worker.yaml -f k8s/40-simulator.yaml -f k8s/50-frontend.yaml
```

### 5. Attivazione dell'Autoscaling KEDA
```bash
kubectl apply -f k8s/31-keda-scaledobject.yaml
```

### 6. Verifica dello Stato dei Pod
```bash
kubectl get pods -n tuscany-router
```
*Attendi che `redis`, `postgres`, `backend`, `simulator` e `frontend` siano su **Running**. Il pod `worker` sarà a 0 ed è il comportamento corretto (la coda è vuota).*

---

## 6. Accesso all'Applicazione Web

Avvia il port-forward per esporre l'interfaccia web:

```bash
kubectl port-forward -n tuscany-router svc/frontend 8080:80
```
Lascia questo terminale aperto e apri nel browser il link: **[http://localhost:8080](http://localhost:8080)**.

---

## 7. Monitoraggio e Testing dell'Autoscaling (KEDA)

1. Apri un **secondo terminale** e avvia il monitoraggio in tempo reale:
   ```bash
   kubectl get pods -n tuscany-router -w
   ```
2. Dal sito web, effettua una ricerca di percorso: vedrai comparire un pod `worker-...` che passerà da `ContainerCreating` a `Running`.
3. Dopo la risposta (e circa 60 secondi di coda vuota), il pod si spegnerà automaticamente.
4. Per testare lo scaling su più pod, invia multiple richieste simultanee premendo velocemente "Calcola" da più schede.

### Comandi Utili di Log e Diagnostica
```bash
# Visualizza i log del Worker
kubectl logs -n tuscany-router deploy/worker

# Visualizza i log del Gateway/Backend
kubectl logs -n tuscany-router deploy/backend

# Controlla lo stato e le metriche di KEDA
kubectl describe scaledobject worker-scaler -n tuscany-router
```

---

## 🛠️ Risoluzione Problemi (Troubleshooting)

* **Errore `ErrImageNeverPull`:** Le immagini non sono state costruite dentro l'ambiente Docker di Minikube. Esegui nuovamente i comandi della **Sezione 4** nello stesso terminale.
* **Il worker non parte mai:** Controlla lo stato con `kubectl describe scaledobject worker-scaler -n tuscany-router`. Assicurati che il `backend` sia già in esecuzione, poiché è il componente che crea il consumer group letto da KEDA.
* **Errore nel worker su `AsyncRedisSaver`:** La libreria `langgraph-checkpoint-redis` si aggiorna rapidamente. Verifica con `kubectl logs deploy/worker` l'errore specifico.
* **Niente spiegazione dall'LLM:** Se la chiave Groq non è valida, il sistema usa un testo fallback. Controlla i log del worker.
* **Redis specific:** È stato impiegato `redis-stack-server` poiché il checkpointer necessita dei moduli JSON e Search (assenti nella versione standard di Redis).

---

## ⚠️ Limiti Noti da Considerare

* **Persistenza Dati:** Redis non ha un volume persistente, quindi i dati andranno persi in caso di riavvio del pod.
* **Gestione Messaggi Pendenti:** Se un worker si arresta durante l'elaborazione di una richiesta, il messaggio rimane nello stato `pending` e non viene ripreso (richiede l'implementazione di `XAUTOCLAIM`).
* **Simulazione:** Orari e ritardi sono attualmente simulati.