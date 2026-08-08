"""Integration test against a real local Ray instance.

Slower than the unit tests and skipped if ray fails to start in this
environment (e.g. no loopback networking available), but under normal
conditions this starts a real single-node Ray cluster and confirms
ray.util.state snapshots parse into RayMetricsCollector without error.
"""

import time

import pytest
from prometheus_client import CollectorRegistry

ray = pytest.importorskip("ray")

from ray_agent_sre.ray_metrics import RayMetricsCollector, collect_live  # noqa: E402


@pytest.fixture(scope="module")
def ray_instance():
    # ray.util.state talks to the dashboard's REST API, so the dashboard has
    # to actually be running (the default), and it needs a moment to come up
    # after ray.init() returns.
    ray.init(num_cpus=1, ignore_reinit_error=True, include_dashboard=True)
    time.sleep(2)
    yield ray
    ray.shutdown()


def test_collect_live_against_real_local_ray(ray_instance):
    registry = CollectorRegistry()
    collector = RayMetricsCollector(registry)

    collect_live(collector)

    assert registry.get_sample_value("ray_cluster_node_count") == 1.0
    assert registry.get_sample_value("ray_cluster_alive_node_count") == 1.0
    assert registry.get_sample_value("ray_cluster_cpu_total") == 1.0
