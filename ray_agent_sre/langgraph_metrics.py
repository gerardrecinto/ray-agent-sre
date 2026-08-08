"""Wraps LangGraph node callables to record latency, retries, and errors.

LangGraph nodes are plain callables added to a StateGraph. GraphRunTracer.wrap
returns a decorator that times each invocation, retries on exception up to
max_retries, and records everything as Prometheus series. It has no
dependency on langgraph itself, so it is unit testable without building a
graph, and the demo graph in demo_graph.py shows it wired to a real
StateGraph.
"""

from __future__ import annotations

import functools
import time
from typing import Callable, TypeVar

from prometheus_client import CollectorRegistry, Counter, Histogram

T = TypeVar("T")


class GraphRunTracer:
    def __init__(self, registry: CollectorRegistry) -> None:
        self.node_runs = Counter(
            "langgraph_node_runs_total", "Total invocations of a graph node", ["node"], registry=registry
        )
        self.node_errors = Counter(
            "langgraph_node_errors_total", "Invocations that raised an exception", ["node"], registry=registry
        )
        self.node_retries = Counter(
            "langgraph_node_retries_total",
            "Retries triggered after a node error",
            ["node"],
            registry=registry,
        )
        self.node_latency = Histogram(
            "langgraph_node_latency_seconds",
            "Wall clock latency per node invocation, including retries",
            ["node"],
            registry=registry,
        )

    def wrap(self, node_name: str, max_retries: int = 0) -> Callable[[Callable[..., T]], Callable[..., T]]:
        def decorator(fn: Callable[..., T]) -> Callable[..., T]:
            # Touch every series for this node once so it reports 0 instead
            # of being absent from /metrics until the first error/retry.
            self.node_runs.labels(node=node_name)
            self.node_errors.labels(node=node_name)
            self.node_retries.labels(node=node_name)

            @functools.wraps(fn)
            def wrapped(*args, **kwargs) -> T:
                start = time.perf_counter()
                self.node_runs.labels(node=node_name).inc()
                attempt = 0
                while True:
                    try:
                        result = fn(*args, **kwargs)
                        self.node_latency.labels(node=node_name).observe(time.perf_counter() - start)
                        return result
                    except Exception:
                        self.node_errors.labels(node=node_name).inc()
                        if attempt < max_retries:
                            attempt += 1
                            self.node_retries.labels(node=node_name).inc()
                            continue
                        self.node_latency.labels(node=node_name).observe(time.perf_counter() - start)
                        raise

            return wrapped

        return decorator
