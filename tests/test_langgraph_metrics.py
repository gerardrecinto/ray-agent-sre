import pytest
from prometheus_client import CollectorRegistry

from ray_agent_sre.langgraph_metrics import GraphRunTracer


def _value(registry: CollectorRegistry, name: str, labels: dict | None = None) -> float:
    return registry.get_sample_value(name, labels or {})


def test_wrap_records_success_run_and_latency():
    registry = CollectorRegistry()
    tracer = GraphRunTracer(registry)

    @tracer.wrap("retrieve")
    def node(x):
        return x + 1

    assert node(1) == 2
    assert _value(registry, "langgraph_node_runs_total", {"node": "retrieve"}) == 1.0
    assert _value(registry, "langgraph_node_errors_total", {"node": "retrieve"}) == 0.0
    count = _value(registry, "langgraph_node_latency_seconds_count", {"node": "retrieve"})
    assert count == 1.0


def test_wrap_retries_up_to_max_retries_then_succeeds():
    registry = CollectorRegistry()
    tracer = GraphRunTracer(registry)

    calls = {"n": 0}

    @tracer.wrap("verify", max_retries=2)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("not yet")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3
    assert _value(registry, "langgraph_node_runs_total", {"node": "verify"}) == 1.0
    assert _value(registry, "langgraph_node_errors_total", {"node": "verify"}) == 2.0
    assert _value(registry, "langgraph_node_retries_total", {"node": "verify"}) == 2.0


def test_wrap_raises_and_records_error_after_exhausting_retries():
    registry = CollectorRegistry()
    tracer = GraphRunTracer(registry)

    @tracer.wrap("verify", max_retries=1)
    def always_fails():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        always_fails()

    assert _value(registry, "langgraph_node_errors_total", {"node": "verify"}) == 2.0
    assert _value(registry, "langgraph_node_retries_total", {"node": "verify"}) == 1.0


def test_wrap_isolates_metrics_per_node_name():
    registry = CollectorRegistry()
    tracer = GraphRunTracer(registry)

    @tracer.wrap("a")
    def node_a():
        return 1

    @tracer.wrap("b")
    def node_b():
        raise ValueError("boom")

    node_a()
    with pytest.raises(ValueError):
        node_b()

    assert _value(registry, "langgraph_node_runs_total", {"node": "a"}) == 1.0
    assert _value(registry, "langgraph_node_errors_total", {"node": "a"}) == 0.0
    assert _value(registry, "langgraph_node_runs_total", {"node": "b"}) == 1.0
    assert _value(registry, "langgraph_node_errors_total", {"node": "b"}) == 1.0


def test_track_run_records_success_run_and_latency():
    registry = CollectorRegistry()
    tracer = GraphRunTracer(registry)

    with tracer.track_run():
        pass

    assert _value(registry, "langgraph_graph_runs_total") == 1.0
    assert _value(registry, "langgraph_graph_errors_total") == 0.0
    assert _value(registry, "langgraph_graph_run_latency_seconds_count") == 1.0


def test_track_run_records_error_and_reraises():
    registry = CollectorRegistry()
    tracer = GraphRunTracer(registry)

    with pytest.raises(RuntimeError):
        with tracer.track_run():
            raise RuntimeError("boom")

    assert _value(registry, "langgraph_graph_runs_total") == 1.0
    assert _value(registry, "langgraph_graph_errors_total") == 1.0
    assert _value(registry, "langgraph_graph_run_latency_seconds_count") == 1.0
