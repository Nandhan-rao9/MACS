from pydantic import BaseModel


class ScoutMetrics(BaseModel):
    growth_score:      float
    margin_score:      float
    cashflow_score:    float
    efficiency_score:  float
    bullish_confidence: float


def run_scout(deal_data: dict) -> ScoutMetrics:
    """Deterministic scoring across 4 dimensions."""
    import datetime

    revenue        = deal_data.get("revenue", 0)
    revenue_growth = deal_data.get("revenue_growth", 0)
    revenue_cagr   = deal_data.get("revenue_cagr_3y", revenue_growth)
    gross_margin   = deal_data.get("gross_margin", 0)
    ebitda_margin  = deal_data.get("ebitda_margin", 0)
    ebitda         = deal_data.get("ebitda", revenue * ebitda_margin)
    net_debt       = deal_data.get("net_debt", 0)
    free_cash_flow = deal_data.get("free_cash_flow", 0)
    employee_count = deal_data.get("employee_count", 1) or 1
    market_growth  = deal_data.get("market_growth", 0)

    # 1. Growth score — blended current + 3y CAGR vs market
    raw_growth   = (0.5 * revenue_growth + 0.5 * revenue_cagr)
    growth_score = min(max(raw_growth / 0.35, 0), 1.0)

    # 2. Margin score — gross margin quality + EBITDA conversion
    margin_score = min((gross_margin * 0.4 + ebitda_margin * 0.6) / 0.40, 1.0)

    # 3. Cash flow score — FCF health + net debt coverage
    fcf_score  = 1.0 if free_cash_flow > 0 else max(0.0, 1 + free_cash_flow / 1_000_000)
    debt_cover = 1.0 if ebitda <= 0 else min(1.0, max(0.0, 1 - (net_debt / (ebitda * 5))))
    cashflow_score = round(0.6 * fcf_score + 0.4 * debt_cover, 4)

    # 4. Efficiency score — revenue per employee vs sector norms
    rev_per_emp      = revenue / employee_count
    efficiency_score = min(rev_per_emp / 300_000, 1.0)  # $300k/emp = benchmark

    bullish_confidence = round(
        0.30 * growth_score +
        0.35 * margin_score +
        0.25 * cashflow_score +
        0.10 * efficiency_score,
        4
    )

    return ScoutMetrics(
        growth_score=round(growth_score, 3),
        margin_score=round(margin_score, 3),
        cashflow_score=round(cashflow_score, 3),
        efficiency_score=round(efficiency_score, 3),
        bullish_confidence=bullish_confidence,
    )