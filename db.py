import json
import psycopg
from config import DATABASE_URL


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty or not set in .env")
    conn_str = DATABASE_URL
    if "sslmode" not in conn_str:
        conn_str += ("&" if "?" in conn_str else "?") + "sslmode=require"
    return psycopg.connect(conn_str)


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE EXTENSION IF NOT EXISTS "pgcrypto";

                DO $$ BEGIN
                    CREATE TYPE deal_status AS ENUM (
                        'NEW', 'PROCESSING', 'SCOUT_DONE',
                        'CONTRARIAN_DONE', 'CONFLICT', 'FINALIZED', 'FAILED'
                    );
                EXCEPTION WHEN duplicate_object THEN null;
                END $$;

                CREATE TABLE IF NOT EXISTS deals (
                    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    sector                TEXT NOT NULL,
                    revenue               FLOAT NOT NULL,
                    revenue_growth        FLOAT NOT NULL DEFAULT 0,
                    revenue_cagr_3y       FLOAT NOT NULL DEFAULT 0,
                    gross_margin          FLOAT NOT NULL DEFAULT 0,
                    ebitda                FLOAT NOT NULL,
                    ebitda_margin         FLOAT NOT NULL DEFAULT 0,
                    net_debt              FLOAT NOT NULL DEFAULT 0,
                    debt_equity           FLOAT NOT NULL,
                    free_cash_flow        FLOAT NOT NULL DEFAULT 0,
                    employee_count        INT NOT NULL DEFAULT 0,
                    founding_year         INT NOT NULL DEFAULT 2000,
                    customer_concentration FLOAT NOT NULL DEFAULT 0,
                    market_growth         FLOAT NOT NULL DEFAULT 0,
                    status                deal_status DEFAULT 'NEW',
                    review_cycle          INT DEFAULT 0,
                    created_at            TIMESTAMP DEFAULT NOW(),
                    updated_at            TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS scout_reports (
                    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    deal_id            UUID REFERENCES deals(id) ON DELETE CASCADE,
                    growth_score       FLOAT NOT NULL,
                    margin_score       FLOAT NOT NULL,
                    cashflow_score     FLOAT NOT NULL DEFAULT 0,
                    efficiency_score   FLOAT NOT NULL DEFAULT 0,
                    bullish_confidence FLOAT NOT NULL,
                    analysis           TEXT,
                    key_strengths      JSONB,
                    concerns           JSONB,
                    created_at         TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS contrarian_reports (
                    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    deal_id            UUID REFERENCES deals(id) ON DELETE CASCADE,
                    risk_score         FLOAT NOT NULL DEFAULT 0,
                    red_flags          JSONB NOT NULL,
                    risk_summary       TEXT,
                    bearish_confidence FLOAT NOT NULL,
                    created_at         TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS memos (
                    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    deal_id             UUID REFERENCES deals(id) ON DELETE CASCADE,
                    final_decision      TEXT NOT NULL,
                    risk_adjusted_score FLOAT NOT NULL,
                    conflict_flag       BOOLEAN NOT NULL,
                    summary             TEXT NOT NULL,
                    review_cycles       INT DEFAULT 1,
                    created_at          TIMESTAMP DEFAULT NOW()
                );
            """)

            # Safe migrations for existing tables
            cur.execute("""
                ALTER TABLE deals
                    ADD COLUMN IF NOT EXISTS revenue_growth        FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS revenue_cagr_3y       FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS gross_margin          FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS ebitda_margin         FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS net_debt              FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS free_cash_flow        FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS employee_count        INT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS founding_year         INT NOT NULL DEFAULT 2000,
                    ADD COLUMN IF NOT EXISTS customer_concentration FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS market_growth         FLOAT NOT NULL DEFAULT 0;

                ALTER TABLE scout_reports
                    ADD COLUMN IF NOT EXISTS cashflow_score   FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS efficiency_score FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS analysis         TEXT,
                    ADD COLUMN IF NOT EXISTS key_strengths    JSONB,
                    ADD COLUMN IF NOT EXISTS concerns         JSONB;

                ALTER TABLE contrarian_reports
                    ADD COLUMN IF NOT EXISTS risk_summary TEXT;

                ALTER TABLE memos
                    ADD COLUMN IF NOT EXISTS review_cycles INT DEFAULT 1;
            """)
        conn.commit()
    print("[DB] Tables ready.")


def fetch_and_lock_deal() -> dict | None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, sector, revenue, revenue_growth, revenue_cagr_3y,
                       gross_margin, ebitda, ebitda_margin, net_debt, debt_equity,
                       free_cash_flow, employee_count, founding_year,
                       customer_concentration, market_growth
                FROM deals
                WHERE status = 'NEW'
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1;
            """)
            row = cur.fetchone()
            if not row:
                return None

            deal_id = str(row[0])
            cur.execute("""
                UPDATE deals SET status = 'PROCESSING', updated_at = NOW()
                WHERE id = %s;
            """, (deal_id,))
        conn.commit()

    return {
        "id":                     deal_id,
        "sector":                 row[1],
        "revenue":                float(row[2]),
        "revenue_growth":         float(row[3]),
        "revenue_cagr_3y":        float(row[4]),
        "gross_margin":           float(row[5]),
        "ebitda":                 float(row[6]),
        "ebitda_margin":          float(row[7]),
        "net_debt":               float(row[8]),
        "debt_equity":            float(row[9]),
        "free_cash_flow":         float(row[10]),
        "employee_count":         int(row[11]),
        "founding_year":          int(row[12]),
        "customer_concentration": float(row[13]),
        "market_growth":          float(row[14]),
    }


def save_results(deal_id: str, final_state: dict):
    scout      = final_state.get("scout_report", {})
    contrarian = final_state.get("contrarian_report", {})
    metrics    = scout.get("metrics", {})

    bullish  = float(metrics.get("bullish_confidence", 0))
    bearish  = float(contrarian.get("bearish_confidence", 0))
    risk_adj = round((bullish - bearish + 1) / 2, 4)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scout_reports
                    (deal_id, growth_score, margin_score, cashflow_score,
                     efficiency_score, bullish_confidence, analysis, key_strengths, concerns)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                deal_id,
                metrics.get("growth_score", 0),
                metrics.get("margin_score", 0),
                metrics.get("cashflow_score", 0),
                metrics.get("efficiency_score", 0),
                bullish,
                scout.get("analysis"),
                json.dumps(scout.get("key_strengths", [])),
                json.dumps(scout.get("concerns", [])),
            ))

            cur.execute("""
                INSERT INTO contrarian_reports
                    (deal_id, risk_score, red_flags, risk_summary, bearish_confidence)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                deal_id,
                bearish,
                json.dumps(contrarian.get("red_flags", [])),
                contrarian.get("risk_summary", ""),
                bearish,
            ))

            cur.execute("""
                INSERT INTO memos
                    (deal_id, final_decision, risk_adjusted_score,
                     conflict_flag, summary, review_cycles)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                deal_id,
                final_state.get("final_decision", "UNKNOWN"),
                final_state.get("risk_adjusted_score") or risk_adj,
                bool(final_state.get("conflict", False)),
                final_state.get("reasoning", ""),
                final_state.get("review_cycle", 1),
            ))

            cur.execute("""
                UPDATE deals
                SET status = 'FINALIZED', review_cycle = %s, updated_at = NOW()
                WHERE id = %s;
            """, (final_state.get("review_cycle", 1), deal_id))

        conn.commit()


def mark_failed(deal_id: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE deals SET status = 'FAILED', updated_at = NOW()
                WHERE id = %s;
            """, (deal_id,))
        conn.commit()