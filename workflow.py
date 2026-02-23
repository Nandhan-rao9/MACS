from langgraph.graph import StateGraph, END
from graph_state import DealState
from agents import scout_node, contrarian_node, judge_node

MAX_CYCLES = 2


def route_after_judge(state: DealState) -> str:
    """
    After judge runs:
    - If there's a real conflict AND we haven't hit max cycles → loop back to scout
    - Otherwise → END
    """
    conflict = state.get("conflict", False)
    cycle    = state.get("review_cycle", 0)

    if conflict and cycle < MAX_CYCLES:
        print(f"\n  🔄 Conflict detected — sending back to Scout (cycle {cycle}/{MAX_CYCLES})")
        return "scout"

    return END


# ─── Build Graph ───────────────────────────────────────────────
graph = StateGraph(DealState)

graph.add_node("scout",      scout_node)
graph.add_node("contrarian", contrarian_node)
graph.add_node("judge",      judge_node)

graph.set_entry_point("scout")

graph.add_edge("scout",      "contrarian")
graph.add_edge("contrarian", "judge")

graph.add_conditional_edges(
    "judge",
    route_after_judge,
    {
        "scout": "scout",
        END:     END,
    }
)

workflow = graph.compile()