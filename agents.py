import time
from llm import llm
from scout import run_scout
from schemas import ScoutLLMOutput, ContrarianOutput, JudgeOutput
from graph_state import DealState
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import ValidationError

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
RISK_CONFIG = {
    # Leverage
    "leverage_threshold":          4.0,
    "leverage_penalty_multiplier": 2.0,

    # Score weights — upside and downside now balanced
    "upside_weight":               1.2,   # was 1.6+boost → was inflating every score
    "downside_weight_normal":      1.0,
    "downside_weight_levered":     1.3,

    # Decision thresholds — spread out so middle outcomes exist
    "invest_threshold":            0.45,  # was 0.25 → too easy
    "pass_threshold":             -0.15,  # was -0.35 → too strict to reach

    # Control
    "max_cycles":  2,
    "max_retries": 2,
}

scout_parser      = PydanticOutputParser(pydantic_object=ScoutLLMOutput)
contrarian_parser = PydanticOutputParser(pydantic_object=ContrarianOutput)
judge_parser      = PydanticOutputParser(pydantic_object=JudgeOutput)


# ─────────────────────────────────────────────
# FACT SHEET
# ─────────────────────────────────────────────
def _fact_sheet(deal: dict) -> str:
    revenue  = deal["revenue"]
    ebitda   = deal.get("ebitda", revenue * deal.get("ebitda_margin", 0))
    net_debt = deal.get("net_debt", 0)
    emp      = deal.get("employee_count", 1) or 1
    age      = 2026 - deal.get("founding_year", 2000)
    nd_ebitda = round(net_debt / ebitda, 2) if ebitda > 0 else "N/A"
    rev_emp   = round(revenue / emp / 1000, 0)

    return f"""
  ┌─ DEAL FACT SHEET ──────────────────────────────────────┐
  │ Sector:               {deal['sector']} ({age} yrs old)
  │ Revenue:              ${revenue/1e6:.2f}M
  │ Revenue Growth (1Y):  {deal.get('revenue_growth',0)*100:+.1f}%
  │ Revenue CAGR (3Y):    {deal.get('revenue_cagr_3y', deal.get('revenue_growth',0))*100:+.1f}%
  │ Gross Margin:         {deal.get('gross_margin',0)*100:.1f}%
  │ EBITDA:               ${ebitda/1e6:.2f}M  ({deal.get('ebitda_margin',0)*100:.1f}% margin)
  │ Net Debt:             ${net_debt/1e6:.2f}M  → {nd_ebitda}x EBITDA
  │ Debt/Equity:          {deal.get('debt_equity',0):.2f}
  │ Free Cash Flow:       ${deal.get('free_cash_flow',0)/1e3:+.0f}k
  │ Employees:            {emp:,}  (${rev_emp}k revenue/employee)
  │ Customer Concentration: {deal.get('customer_concentration',0)*100:.1f}%
  │ Market Growth:        {deal.get('market_growth',0)*100:.1f}% CAGR
  └────────────────────────────────────────────────────────┘"""


# ─────────────────────────────────────────────
# RETRY WRAPPER  (self-correction requirement)
# ─────────────────────────────────────────────
def _invoke_with_retry(prompt, parser, quality_check=None, label="Agent"):
    last_error = None
    for attempt in range(1, RISK_CONFIG["max_retries"] + 1):
        try:
            response = llm.invoke(prompt)
            parsed   = parser.parse(response.content)

            if quality_check:
                error_msg = quality_check(parsed)
                if error_msg:
                    raise ValueError(error_msg)

            if attempt > 1:
                print(f"    ♻️  [{label}] Retry {attempt} succeeded.")
            return parsed

        except (ValidationError, ValueError, Exception) as e:
            last_error = str(e)
            if attempt < RISK_CONFIG["max_retries"]:
                print(f"    ⚠️  [{label}] Attempt {attempt} failed — retrying with stricter prompt...")
                prompt += f"\n\nPREVIOUS ATTEMPT FAILED: {last_error[:200]}\nReturn STRICT JSON only. No prose, no markdown."
            else:
                raise RuntimeError(f"[{label}] Failed after {RISK_CONFIG['max_retries']} retries: {last_error}")


# ─────────────────────────────────────────────
# DECISION ENGINE
# ─────────────────────────────────────────────
class DecisionEngine:

    @staticmethod
    def compute_score(bullish: float, bearish: float, deal_data: dict) -> float:
        """
        Risk-adjusted score.
        
        Expected range with balanced weights:
          bullish=0.8, bearish=0.2 →  0.76  → INVEST
          bullish=0.6, bearish=0.5 →  0.22  → RDD
          bullish=0.4, bearish=0.7 → -0.22  → PASS
          bullish=0.2, bearish=0.9 → -0.66  → PASS
        """
        ebitda   = deal_data.get("ebitda", deal_data["revenue"] * deal_data.get("ebitda_margin", 0))
        net_debt = deal_data.get("net_debt", 0)
        nd_mult  = (net_debt / ebitda) if ebitda > 0 else 0

        # Double the penalty if leverage is dangerously high
        leverage_penalty = (
            RISK_CONFIG["leverage_penalty_multiplier"]
            if nd_mult > RISK_CONFIG["leverage_threshold"]
            else 1.0
        )

        adjusted_bearish  = min(1.0, bearish * leverage_penalty)
        downside_weight   = (
            RISK_CONFIG["downside_weight_levered"]
            if leverage_penalty > 1
            else RISK_CONFIG["downside_weight_normal"]
        )

        score = (bullish * RISK_CONFIG["upside_weight"]) - (adjusted_bearish * downside_weight)
        return round(max(-1.0, min(1.0, score)), 4)

    @staticmethod
    def verdict(score: float) -> str:
        if score > RISK_CONFIG["invest_threshold"]:
            return "INVEST"
        elif score < RISK_CONFIG["pass_threshold"]:
            return "PASS"
        return "REQUIRES_DUE_DILIGENCE"


# ─────────────────────────────────────────────
# CONFLICT SYSTEM
# ─────────────────────────────────────────────
def classify_conflict(bullish: float, bearish: float, score: float) -> str:
    gap = abs(bullish - bearish)
    if gap > 0.40:
        return "PROBABILITY_DISAGREEMENT"
    if -0.20 < score < 0.20:
        return "AMBIGUOUS_SIGNAL"
    return "STRUCTURAL_DISAGREEMENT"


def resolve_conflict(conflict_type: str, score: float) -> str:
    if conflict_type == "AMBIGUOUS_SIGNAL":
        return "REQUIRES_DUE_DILIGENCE"
    if conflict_type == "STRUCTURAL_DISAGREEMENT":
        # Penalise structural disagreements — lean conservative
        return DecisionEngine.verdict(score - 0.15)
    # PROBABILITY_DISAGREEMENT — trust the math
    return DecisionEngine.verdict(score)


# ─────────────────────────────────────────────
# SCOUT NODE
# ─────────────────────────────────────────────
def scout_node(state: DealState) -> dict:
    deal_data = state["deal_data"]
    deal_id   = state["deal_id"]
    cycle     = state.get("review_cycle", 0)

    print(f"\n  🔍 [Scout] Deal {deal_id[:8]} | Cycle {cycle + 1}")
    t = time.time()

    metrics = run_scout(deal_data)
    print(f"     Quant → growth={metrics.growth_score:.2f} | margin={metrics.margin_score:.2f} | "
          f"cashflow={metrics.cashflow_score:.2f} | efficiency={metrics.efficiency_score:.2f}")

    prompt = f"""You are an M&A Scout. Be honest — not every deal is good.

{_fact_sheet(deal_data)}

Quantitative Scores (0=worst, 1=best):
  Growth Score:      {metrics.growth_score:.3f}
  Margin Score:      {metrics.margin_score:.3f}
  Cash Flow Score:   {metrics.cashflow_score:.3f}
  Efficiency Score:  {metrics.efficiency_score:.3f}

Score guide: 0.0–0.3 = weak | 0.3–0.6 = mixed | 0.6–0.8 = solid | 0.8–1.0 = strong

{'CYCLE 2 NOTE: Contrarian flagged concerns last cycle. Provide deeper supporting data.' if cycle >= 1 else ''}

Provide:
- analysis: 3–5 honest sentences about this deal's overall quality
- key_strengths: exactly 3, each citing a specific number
- concerns: exactly 3, each citing a specific number  
- bullish_confidence: probability (0–1) of strong returns — must align with scores above

{scout_parser.get_format_instructions()}
"""

    def quality(p):
        if len(p.key_strengths) != 3: return "Need exactly 3 key_strengths"
        if len(p.concerns) != 3:      return "Need exactly 3 concerns"
        if not 0 <= p.bullish_confidence <= 1: return "bullish_confidence out of range"
        return None

    parsed  = _invoke_with_retry(prompt, scout_parser, quality, "Scout")
    elapsed = time.time() - t
    print(f"  ✓ [Scout] bullish={parsed.bullish_confidence:.3f} | {elapsed:.1f}s")

    return {
        "scout_report": {
            "metrics":            metrics.model_dump(),
            "analysis":           parsed.analysis,
            "key_strengths":      parsed.key_strengths,
            "concerns":           parsed.concerns,
            "bullish_confidence": parsed.bullish_confidence,
        },
        "review_cycle": cycle + 1,
    }


# ─────────────────────────────────────────────
# CONTRARIAN NODE
# ─────────────────────────────────────────────
def contrarian_node(state: DealState) -> dict:
    deal_data = state["deal_data"]
    deal_id   = state["deal_id"]
    scout     = state["scout_report"]
    bullish   = scout["bullish_confidence"]

    print(f"  ⚔️  [Contrarian] Deal {deal_id[:8]} | challenging bullish={bullish:.3f}")
    t = time.time()

    # Pre-compute hard flags so LLM has anchors
    ebitda    = deal_data.get("ebitda", deal_data["revenue"] * deal_data.get("ebitda_margin", 0))
    net_debt  = deal_data.get("net_debt", 0)
    nd_mult   = round(net_debt / ebitda, 1) if ebitda > 0 else 99
    fcf       = deal_data.get("free_cash_flow", 0)
    cust_conc = deal_data.get("customer_concentration", 0)

    hard_flags = []
    if nd_mult > 4:      hard_flags.append(f"Net Debt/EBITDA = {nd_mult}x  (threshold: 4x)")
    if fcf < 0:          hard_flags.append(f"Negative FCF = ${fcf/1e3:.0f}k  (cash burn)")
    if cust_conc > 0.40: hard_flags.append(f"Customer concentration = {cust_conc*100:.0f}%  (threshold: 40%)")
    if not hard_flags:   hard_flags.append("No hard threshold breaches — adjust bearish_confidence down accordingly")

    prompt = f"""You are the Risk Auditor. Stress-test this deal honestly — be fair, not just negative.

{_fact_sheet(deal_data)}

Scout's case (bullish={bullish:.3f}):
  {scout['analysis']}
  Strengths: {' | '.join(scout['key_strengths'])}

Hard flag check:
  {chr(10).join(f'  {f}' for f in hard_flags)}

CALIBRATION — bearish_confidence must match the evidence:
  No hard flags + bullish ≥ 0.70  →  bearish should be 0.15–0.35
  1 hard flag                      →  bearish should be 0.35–0.55
  2+ hard flags                    →  bearish should be 0.55–0.80
  Fundamental business failure     →  bearish 0.80–1.00

Rules:
  - Only flag risks backed by actual numbers
  - Each red_flag must cite a specific number
  - If data is genuinely strong, say so in risk_summary

{contrarian_parser.get_format_instructions()}
"""

    def quality(p):
        if len(p.red_flags) < 1:               return "Need at least 1 red_flag"
        if not 0 <= p.bearish_confidence <= 1: return "bearish_confidence out of range"
        return None

    parsed  = _invoke_with_retry(prompt, contrarian_parser, quality, "Contrarian")
    elapsed = time.time() - t
    print(f"  ✓ [Contrarian] bearish={parsed.bearish_confidence:.3f} | {elapsed:.1f}s")

    return {"contrarian_report": parsed.model_dump()}


# ─────────────────────────────────────────────
# JUDGE NODE
# ─────────────────────────────────────────────
def judge_node(state: DealState) -> dict:
    deal_data  = state["deal_data"]
    deal_id    = state["deal_id"]
    scout      = state["scout_report"]
    contrarian = state["contrarian_report"]
    cycle      = state.get("review_cycle", 1)

    bullish = scout["bullish_confidence"]
    bearish = contrarian["bearish_confidence"]

    # Deterministic math — never overridden by LLM alone
    score          = DecisionEngine.compute_score(bullish, bearish, deal_data)
    system_verdict = DecisionEngine.verdict(score)

    print(f"  ⚖️  [Judge] Deal {deal_id[:8]} | score={score:.3f} → system says {system_verdict}")
    t = time.time()

    prompt = f"""You are the Investment Committee Chair. Make a decisive call.

{_fact_sheet(deal_data)}

━━━ SCOUT (bullish={bullish:.3f}) ━━━
{scout['analysis']}
Strengths: {' | '.join(scout['key_strengths'])}
Concerns:  {' | '.join(scout['concerns'])}

━━━ CONTRARIAN (bearish={bearish:.3f}) ━━━
{contrarian['risk_summary']}
Flags: {' | '.join(contrarian['red_flags'])}

━━━ QUANT SYNTHESIS ━━━
  Risk-Adjusted Score: {score:.3f}
  System verdict:      {system_verdict}
  Confidence gap:      {abs(bullish-bearish):.3f}

Decision rules:
  INVEST               → score > 0.45  (strong deal, risks manageable)
  PASS                 → score < -0.15 (risks dominate, do not proceed)
  REQUIRES_DUE_DILIGENCE → score in between (mixed signals, need more info)

Provide reasoning that cites specific numbers from both reports (3–5 sentences).
Your final_decision should match the system verdict unless you have a strong qualitative reason.

{'⚠️  FINAL CYCLE — provide your definitive call. No further review loops.' if cycle >= 2 else ''}

{judge_parser.get_format_instructions()}
"""

    def quality(p):
        if p.final_decision not in ("INVEST", "PASS", "REQUIRES_DUE_DILIGENCE"):
            return f"Invalid final_decision: {p.final_decision}"
        if not 0 <= p.decision_confidence <= 1:
            return "decision_confidence out of range"
        if len(p.reasoning) < 60:
            return "reasoning too short — must cite specific numbers"
        return None

    parsed = _invoke_with_retry(prompt, judge_parser, quality, "Judge")

    # Conflict = LLM disagrees with deterministic math
    conflict      = (parsed.final_decision != system_verdict)
    conflict_type = classify_conflict(bullish, bearish, score) if conflict else None

    # Final cycle — force resolution, math wins
    is_final     = cycle >= RISK_CONFIG["max_cycles"]
    final_decision = parsed.final_decision

    if is_final and conflict:
        final_decision = resolve_conflict(conflict_type, score)
        source = f"FORCED_{conflict_type}"
        print(f"     ⚠️  Max cycles — overriding LLM ({parsed.final_decision}) → {final_decision}")
    elif conflict:
        source = "CONFLICT_PENDING_REVIEW"
    else:
        source = "LLM_AGREES_WITH_MATH"

    elapsed = time.time() - t
    icon = {"INVEST": "✅", "PASS": "❌", "REQUIRES_DUE_DILIGENCE": "🔶"}.get(final_decision, "❓")
    print(f"  ✓ [Judge] {icon} {final_decision} | conf={parsed.decision_confidence:.2f} | "
          f"conflict={conflict} ({conflict_type}) | src={source} | {elapsed:.1f}s")

    return {
        "final_decision":       final_decision,
        "llm_decision":         parsed.final_decision,
        "system_decision":      system_verdict,
        "decision_source":      source,
        "decision_confidence":  parsed.decision_confidence,
        "reasoning":            parsed.reasoning,
        "risk_adjusted_score":  score,
        "conflict":             conflict,
        "conflict_type":        conflict_type,
    }