# MACS — Multi-Agent Capital Scout

An autonomous multi-agent pipeline that monitors incoming investment deals, routes them through three adversarial AI agents, and produces validated risk-adjusted memos — saved permanently to Supabase.

---

## How It Works

```
Producer → Supabase (NEW) → Orchestrator → LangGraph Workflow
                                                │
                                         ┌──────▼──────┐
                                         │    Scout     │  math + LLM upside analysis
                                         └──────┬───────┘
                                                │
                                         ┌──────▼──────┐
                                         │ Contrarian  │  LLM stress-test + risk flags
                                         └──────┬───────┘
                                                │
                                         ┌──────▼──────┐
                                         │    Judge    │  DecisionEngine + LLM synthesis
                                         └──────┬───────┘
                                                │
                                     conflict AND cycle < 2?
                                       YES ─────┘  NO → Save → FINALIZED
                                     (loop back to Scout)
```

Three agents, one shared state object (`DealState`), one decision. If Scout and Contrarian significantly disagree, the workflow loops back for a second review cycle. On the final cycle, deterministic math overrides the LLM.

---

## Tech Stack

| | |
|---|---|
| **Agent framework** | LangGraph (StateGraph with conditional loop) |
| **LLM** | Groq + Llama 3.3 70B (~500 tok/s) |
| **Database** | Supabase PostgreSQL |
| **Validation** | Pydantic v2 — all LLM output is typed and validated |
| **DB driver** | psycopg v3 |

---

## Project Structure

```
MACS/
├── main.py          # Entry point — producer thread + orchestrator
├── config.py        # Env var loader
├── db.py            # All DB logic: init, lock, save, fail
├── graph_state.py   # DealState TypedDict — shared agent memory
├── schemas.py       # Pydantic schemas for agent communication
├── workflow.py      # LangGraph graph + conditional routing
├── scout.py         # Deterministic math scoring (no LLM)
├── agents.py        # Scout, Contrarian, Judge + DecisionEngine
├── llm.py           # Groq LLM instance
├── producer.py      # Deal generator → Supabase
└── orchestrator.py  # Polling loop + workflow runner
```

---

## Setup

### 1. Install
```bash
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # macOS/Linux

pip install -r requirements.txt
```

### 2. Environment
```bash
cp .env.example .env
```
```env
DATABASE_URL=postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres
GROQ_API_KEY=gsk_...
```

### 3. Run
```bash
python main.py
```

Tables are created automatically on first run. To run producer and orchestrator separately:
```bash
python producer.py      # terminal 1
python orchestrator.py  # terminal 2
```

### Recovery after crash
```sql
-- Reset any deals stuck mid-processing
UPDATE deals SET status = 'NEW' WHERE status = 'PROCESSING';
```

---

## Sample Output

```
[Producer] ➕ #6220cff9 | Technology | Rev $12.3M | EBITDA 15% | FCF +$420k

══════════════════════════════════════════════════
  📋 NEW DEAL → 6220cff9 | Technology | $12.3M
══════════════════════════════════════════════════
  🔍 [Scout]      bullish=0.740 | 1.4s
  ⚔️  [Contrarian] bearish=0.280 | 0.8s
  ⚖️  [Judge]      ✅ INVEST | score=0.608 | conf=0.85 | 1.2s

  ✅ FINAL: INVEST | Cycles: 1 | Time: 3.4s
══════════════════════════════════════════════════
```

---

## Key Design Decisions

- **Math before LLM** — Scout scores deals deterministically first. LLM interprets numbers, never invents them.
- **Typed communication** — agents exchange Pydantic-validated JSON, not raw text. Bad output triggers automatic retry with the error fed back into the prompt.
- **Two-layer Judge** — `DecisionEngine` computes a risk-adjusted score independently. If LLM disagrees with math on the final cycle, math wins.
- **`FOR UPDATE SKIP LOCKED`** — atomic DB queue. Multiple orchestrators can run in parallel safely with no coordination layer.
- **Single transaction saves** — all four DB writes (scout, contrarian, memo, status update) commit together or not at all.

---

## Scaling to 1000 Deals/Hour

Current: ~600/hour (one worker, ~6s/deal).

- **Parallel workers** — run 3–4 `orchestrator.py` processes. `FOR UPDATE SKIP LOCKED` handles concurrency with zero code changes → immediate 3–4×
- **Async LLM** — swap `llm.invoke()` for `llm.ainvoke()` + asyncio to pipeline deals
- **Message queue** — replace DB polling with Redis/SQS for lower latency at high volume
- **Model tiering** — use a smaller model for Scout/Contrarian, reserve Llama 70B for Judge only
- **DB partitioning** — partition `deals` by month + index on `(status, created_at)` for sustained query performance

---

## Requirements

```
langgraph>=0.2.0
langchain-core>=0.3.0
langchain-groq>=0.2.0
psycopg[binary]>=3.1.0
pydantic>=2.0.0
python-dotenv>=1.0.0
```
