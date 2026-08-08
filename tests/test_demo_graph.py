from prometheus_client import CollectorRegistry

from ray_agent_sre.demo_graph import run_demo_graph
from ray_agent_sre.langgraph_metrics import GraphRunTracer


def _value(registry: CollectorRegistry, name: str, labels: dict | None = None) -> float:
    return registry.get_sample_value(name, labels or {})


def test_run_demo_graph_produces_answer_and_metrics():
    registry = CollectorRegistry()
    tracer = GraphRunTracer(registry)

    result = run_demo_graph(tracer, "test query")

    assert "answer" in result
    assert result["answer"] != ""
    assert _value(registry, "langgraph_node_runs_total", {"node": "retrieve"}) == 1.0
    assert _value(registry, "langgraph_node_runs_total", {"node": "generate"}) == 1.0
    assert _value(registry, "langgraph_node_runs_total", {"node": "verify"}) == 1.0


def test_run_demo_graph_many_runs_eventually_records_retries():
    registry = CollectorRegistry()
    tracer = GraphRunTracer(registry)

    for i in range(30):
        try:
            run_demo_graph(tracer, f"query {i}")
        except Exception:
            pass

    total_retries = _value(registry, "langgraph_node_retries_total", {"node": "verify"})
    total_errors = _value(registry, "langgraph_node_errors_total", {"node": "verify"})
    assert total_errors >= total_retries >= 1.0
