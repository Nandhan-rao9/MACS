from pydantic import BaseModel, Field
from typing import List, Optional


class ScoutLLMOutput(BaseModel):
    analysis: str = Field(description="Qualitative analysis of the deal metrics, 2-3 sentences")
    key_strengths: List[str] = Field(description="Exactly 3 bullish signals, each citing a specific number")
    concerns: List[str] = Field(description="Exactly 3 concerns, each citing a specific number")
    bullish_confidence: float = Field(description="Probability of strong upside, 0.0 to 1.0")


class ContrarianOutput(BaseModel):
    red_flags: List[str] = Field(description="List of material red flags, each citing specific numbers")
    risk_summary: str = Field(description="Overall risk summary in 2-3 sentences")
    bearish_confidence: float = Field(description="Probability of permanent capital loss, 0.0 to 1.0")


class JudgeOutput(BaseModel):
    conflict: bool = Field(description="True if scout and contrarian have irreconcilable structural disagreement")
    conflict_type: Optional[str] = Field(default=None, description="PROBABILITY_DISAGREEMENT | STRUCTURAL_DISAGREEMENT | AMBIGUOUS_SIGNAL | None")
    final_decision: str = Field(description="Exactly one of: INVEST, PASS, REQUIRES_DUE_DILIGENCE")
    decision_confidence: float = Field(description="How confident you are in this decision, 0.0 to 1.0")
    reasoning: str = Field(description="3-5 sentence reasoning citing specific numbers from both reports")