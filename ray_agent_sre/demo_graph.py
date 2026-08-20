"""A small LangGraph graph instrumented with GraphRunTracer.

Not a real agent, just enough of a graph (retrieve -> generate -> verify) to
produce realistic per-node latency/retry/error series for the exporter. The
verify node simulates a flaky downstream check (fails ~35% of the time) so
that langgraph_node_errors_total and langgraph_node_retries_total accumulate
non-zero values across repeated runs, same as a real verification call that
occasionally times out.
"""

from __future__ import annotations

import random
import time
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from ray_agent_sre.langgraph_metrics import GraphRunTracer

VERIFY_FAILURE_RATE = 0.35


class GraphState(TypedDict):
    query: str
    retrieved: str
    answer: str


def build_graph(tracer: GraphRunTracer):
    @tracer.wrap("retrieve", max_retries=0)
    def retrieve(state: GraphState) -> GraphState:
        time.sleep(random.uniform(0.01, 0.03))
        return {**state, "retrieved": f"context for '{state['query']}'"}

    @tracer.wrap("generate", max_retries=0)
    def generate(state: GraphState) -> GraphState:
        time.sleep(random.uniform(0.02, 0.05))
        return {**state, "answer": f"answer using {state['retrieved']}"}

    @tracer.wrap("verify", max_retries=2)
    def verify(state: GraphState) -> GraphState:
        time.sleep(random.uniform(0.005, 0.015))
        if random.random() < VERIFY_FAILURE_RATE:
            raise ValueError("verification check timed out")
        return state

    graph = StateGraph(GraphState)
    graph.add_node("retrieve", retrieve)
    graph.add_node("generate", generate)
    graph.add_node("verify", verify)
    graph.add_edge(START, "retrieve")
    graph.add_edge("retrieve", "generate")
    graph.add_edge("generate", "verify")
    graph.add_edge("verify", END)
    return graph.compile()


def run_demo_graph(tracer: GraphRunTracer, query: str) -> GraphState:
    app = build_graph(tracer)
    with tracer.track_run():
        return app.invoke({"query": query, "retrieved": "", "answer": ""})
