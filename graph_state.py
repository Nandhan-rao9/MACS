from typing import TypedDict, Optional, Dict, Any


class DealState(TypedDict):
    # ── Input ──────────────────────────────────────────────────────
    deal_id:   str
    deal_data: Dict[str, Any]

    # ── Scout output ───────────────────────────────────────────────
    scout_report: Optional[Dict]
    # { metrics, analysis, key_strengths, concerns, bullish_confidence }

    # ── Contrarian output ──────────────────────────────────────────
    contrarian_report: Optional[Dict]
    # { red_flags, risk_summary, bearish_confidence }

    # ── Judge output ───────────────────────────────────────────────
    final_decision:       Optional[str]   # INVEST | PASS | REQUIRES_DUE_DILIGENCE
    llm_decision:         Optional[str]   # what the LLM wanted
    system_decision:      Optional[str]   # what the math said
    decision_source:      Optional[str]   # LLM_AGREES_WITH_MATH | CONFLICT_PENDING | FORCED_*
    decision_confidence:  Optional[float]
    reasoning:            Optional[str]
    risk_adjusted_score:  Optional[float]

    # ── Conflict ───────────────────────────────────────────────────
    conflict:       Optional[bool]
    conflict_type:  Optional[str]  # PROBABILITY_DISAGREEMENT | STRUCTURAL_DISAGREEMENT | AMBIGUOUS_SIGNAL

    # ── Loop control ───────────────────────────────────────────────
    review_cycle: int