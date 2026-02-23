import time
import json
from db import fetch_and_lock_deal, save_results, mark_failed
from workflow import workflow
from graph_state import DealState

POLL_INTERVAL = 1.0


def process_deal(deal: dict):
    deal_id = deal["id"]

    print(f"\n{'═'*65}")
    print(f"  📋 NEW DEAL LOCKED  →  {deal_id[:8]}...")
    print(f"  Sector: {deal['sector']} | Revenue: ${deal['revenue']/1e6:.2f}M | "
          f"EBITDA: {deal.get('ebitda_margin',0)*100:.1f}%")
    print(f"  FCF: ${deal.get('free_cash_flow',0)/1e3:+.0f}k | "
          f"D/E: {deal.get('debt_equity',0):.1f} | "
          f"CustConc: {deal.get('customer_concentration',0)*100:.0f}% | "
          f"Employees: {deal.get('employee_count',0):,}")
    print(f"{'─'*65}")
    print(f"  [State] Initialising DealState → passing to LangGraph workflow")

    # ── Build initial shared state ─────────────────────────────────
    initial_state: DealState = {
        "deal_id":              deal_id,
        "deal_data":            deal,
        "scout_report":         None,
        "contrarian_report":    None,
        "conflict":             None,
        "conflict_type":        None,
        "final_decision":       None,
        "decision_confidence":  None,
        "reasoning":            None,
        "risk_adjusted_score":  None,
        "review_cycle":         0,
    }

    t_start = time.time()

    # ── Run full agentic workflow ──────────────────────────────────
    # LangGraph passes state through: Scout → Contrarian → Judge → (loop?)
    final_state = workflow.invoke(initial_state)

    elapsed = time.time() - t_start

    # ── Print state handoff summary ────────────────────────────────
    scout  = final_state.get("scout_report", {})
    contra = final_state.get("contrarian_report", {})
    print(f"\n  {'─'*63}")
    print(f"  [State Handoff Summary]")
    print(f"  Scout  → bullish_confidence : {scout.get('bullish_confidence') or scout.get('metrics',{}).get('bullish_confidence','?'):.3f}")
    print(f"  Contra → bearish_confidence : {contra.get('bearish_confidence','?'):.3f}")
    print(f"  Judge  → risk_adjusted_score: {final_state.get('risk_adjusted_score','?'):.3f}")
    print(f"  Cycles run: {final_state.get('review_cycle', 1)}")

    # ── Final verdict ──────────────────────────────────────────────
    decision = final_state.get("final_decision", "UNKNOWN")
    icon     = {"INVEST": "✅", "PASS": "❌", "REQUIRES_DUE_DILIGENCE": "🔶"}.get(decision, "❓")
    conf     = final_state.get("decision_confidence", 0)

    print(f"\n  {icon}  FINAL MEMO: {decision}  (confidence={conf:.2f})")
    print(f"  Reasoning: {(final_state.get('reasoning') or '')[:180]}...")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"{'═'*65}\n")

    # ── Persist to Supabase ────────────────────────────────────────
    save_results(deal_id, final_state)


def run_orchestrator():
    print("🎯 Orchestrator started — monitoring deal queue...\n")
    while True:
        deal = fetch_and_lock_deal()

        if not deal:
            print(".", end="", flush=True)
            time.sleep(POLL_INTERVAL)
            continue

        try:
            process_deal(deal)
        except Exception as e:
            import traceback
            print(f"\n  ❌ Error processing deal {deal['id'][:8]}: {e}")
            traceback.print_exc()
            mark_failed(deal["id"])


if __name__ == "__main__":
    from db import init_db
    init_db()
    run_orchestrator()