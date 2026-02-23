import random
import time
import datetime
from db import get_connection

SECTORS = [
    "Technology", "Healthcare", "Energy",
    "FinTech", "RealEstate", "Manufacturing",
    "Consumer", "Biotech",
]

# Rough sector benchmarks for realistic generation
SECTOR_PROFILES = {
    "Technology":     {"gross_margin": (0.55, 0.85), "rev_per_emp": (200_000, 600_000)},
    "Healthcare":     {"gross_margin": (0.40, 0.70), "rev_per_emp": (150_000, 350_000)},
    "Energy":         {"gross_margin": (0.20, 0.50), "rev_per_emp": (300_000, 800_000)},
    "FinTech":        {"gross_margin": (0.50, 0.80), "rev_per_emp": (250_000, 500_000)},
    "RealEstate":     {"gross_margin": (0.30, 0.60), "rev_per_emp": (400_000, 1_200_000)},
    "Manufacturing":  {"gross_margin": (0.15, 0.40), "rev_per_emp": (100_000, 250_000)},
    "Consumer":       {"gross_margin": (0.25, 0.55), "rev_per_emp": (100_000, 200_000)},
    "Biotech":        {"gross_margin": (0.60, 0.90), "rev_per_emp": (200_000, 500_000)},
}


def generate_deal() -> dict:
    sector  = random.choice(SECTORS)
    profile = SECTOR_PROFILES[sector]

    revenue        = random.uniform(1_000_000, 80_000_000)
    revenue_growth = random.uniform(-0.15, 0.50)
    revenue_cagr   = revenue_growth * random.uniform(0.6, 1.2)  # slightly noisy vs single year
    revenue_cagr   = max(-0.20, min(0.60, revenue_cagr))

    gm_lo, gm_hi   = profile["gross_margin"]
    gross_margin    = random.uniform(gm_lo, gm_hi)
    ebitda_margin   = gross_margin * random.uniform(0.25, 0.65)  # EBITDA < gross always
    ebitda          = revenue * ebitda_margin

    debt_equity     = random.uniform(0.1, 5.0)
    net_debt        = revenue * random.uniform(0.0, 2.5)   # net debt as multiple of revenue
    free_cash_flow  = random.uniform(-2_000_000, 5_000_000)

    rev_lo, rev_hi  = profile["rev_per_emp"]
    rev_per_emp     = random.uniform(rev_lo, rev_hi)
    employee_count  = max(5, int(revenue / rev_per_emp))

    founding_year   = random.randint(1985, 2022)
    customer_conc   = random.uniform(0.05, 0.70)
    market_growth   = random.uniform(0.02, 0.30)

    return {
        "sector":                 sector,
        "revenue":                revenue,
        "revenue_growth":         revenue_growth,
        "revenue_cagr_3y":        revenue_cagr,
        "gross_margin":           gross_margin,
        "ebitda":                 ebitda,
        "ebitda_margin":          ebitda_margin,
        "net_debt":               net_debt,
        "debt_equity":            debt_equity,
        "free_cash_flow":         free_cash_flow,
        "employee_count":         employee_count,
        "founding_year":          founding_year,
        "customer_concentration": customer_conc,
        "market_growth":          market_growth,
    }


def insert_deal(deal: dict) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO deals (
                    sector, revenue, revenue_growth, revenue_cagr_3y,
                    gross_margin, ebitda, ebitda_margin, net_debt, debt_equity,
                    free_cash_flow, employee_count, founding_year,
                    customer_concentration, market_growth
                )
                VALUES (
                    %(sector)s, %(revenue)s, %(revenue_growth)s, %(revenue_cagr_3y)s,
                    %(gross_margin)s, %(ebitda)s, %(ebitda_margin)s, %(net_debt)s, %(debt_equity)s,
                    %(free_cash_flow)s, %(employee_count)s, %(founding_year)s,
                    %(customer_concentration)s, %(market_growth)s
                )
                RETURNING id;
            """, deal)
            deal_id = str(cur.fetchone()[0])
        conn.commit()
    return deal_id


def run_producer(interval: float = 6.0):
    print(f"🏭 Producer started — new deal every {interval:.0f}s\n")
    while True:
        deal    = generate_deal()
        deal_id = insert_deal(deal)
        age     = 2024 - deal["founding_year"]
        print(f"[Producer] ➕ #{deal_id[:8]}... | {deal['sector']:13s} | "
              f"Rev ${deal['revenue']/1e6:.1f}M | "
              f"GM {deal['gross_margin']*100:.0f}% | "
              f"EBITDA {deal['ebitda_margin']*100:.0f}% | "
              f"FCF ${deal['free_cash_flow']/1e3:+.0f}k | "
              f"Emp {deal['employee_count']} | "
              f"Age {age}y")
        time.sleep(interval)


if __name__ == "__main__":
    from db import init_db
    init_db()
    run_producer()