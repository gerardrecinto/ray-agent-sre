"""Entrypoint: starts a local Ray instance, a LangGraph demo graph, and serves
/metrics on the given port.

Usage:
    python -m ray_agent_sre.server --port 9100 --interval 5
"""

from __future__ import annotations

import argparse
import logging
import threading

from prometheus_client import CollectorRegistry, start_http_server

from ray_agent_sre.demo_graph import run_demo_graph
from ray_agent_sre.langgraph_metrics import GraphRunTracer
from ray_agent_sre.ray_metrics import RayMetricsCollector, collect_live

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("ray_agent_sre.server")


def _refresh_loop(
    ray_collector: RayMetricsCollector,
    tracer: GraphRunTracer,
    interval_seconds: float,
    stop_event: threading.Event,
) -> None:
    queries = [
        "what is the p99 latency budget for the retrieval service",
        "summarize last night's pipeline failures",
        "which node pool is closest to capacity",
    ]
    i = 0
    while not stop_event.is_set():
        try:
            collect_live(ray_collector)
        except Exception:
            logger.exception("failed to refresh Ray metrics")

        try:
            run_demo_graph(tracer, queries[i % len(queries)])
        except Exception:
            logger.info("demo graph run failed after retries, recorded as langgraph_node_errors_total")
        i += 1

        stop_event.wait(interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ray + LangGraph Prometheus exporter")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between refresh cycles")
    parser.add_argument("--ray-cpus", type=int, default=2)
    args = parser.parse_args()

    import ray

    ray.init(num_cpus=args.ray_cpus, ignore_reinit_error=True)
    logger.info("local Ray instance started with %d CPUs", args.ray_cpus)

    registry = CollectorRegistry()
    ray_collector = RayMetricsCollector(registry)
    tracer = GraphRunTracer(registry)

    start_http_server(args.port, registry=registry)
    logger.info("serving /metrics on port %d", args.port)

    stop_event = threading.Event()
    try:
        _refresh_loop(ray_collector, tracer, args.interval, stop_event)
    except KeyboardInterrupt:
        stop_event.set()
        logger.info("shutting down")


if __name__ == "__main__":
    main()
