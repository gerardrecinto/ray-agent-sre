from prometheus_client import CollectorRegistry

from ray_agent_sre.ray_metrics import (
    ActorSnapshot,
    ClusterResourceSnapshot,
    NodeSnapshot,
    RayMetricsCollector,
    TaskSnapshot,
)


def _value(registry: CollectorRegistry, name: str, labels: dict | None = None) -> float:
    return registry.get_sample_value(name, labels or {})


def test_update_from_nodes_sets_up_and_cpu_gauges():
    registry = CollectorRegistry()
    collector = RayMetricsCollector(registry)

    nodes = [
        NodeSnapshot(node_id="node-a", alive=True, cpu_total=4.0),
        NodeSnapshot(node_id="node-b", alive=False, cpu_total=8.0),
    ]
    collector.update_from_nodes(nodes)

    assert _value(registry, "ray_node_up", {"node_id": "node-a"}) == 1.0
    assert _value(registry, "ray_node_up", {"node_id": "node-b"}) == 0.0
    assert _value(registry, "ray_node_cpu_total", {"node_id": "node-a"}) == 4.0
    assert _value(registry, "ray_cluster_node_count") == 2.0
    assert _value(registry, "ray_cluster_alive_node_count") == 1.0


def test_update_from_nodes_empty_cluster():
    registry = CollectorRegistry()
    collector = RayMetricsCollector(registry)

    collector.update_from_nodes([])

    assert _value(registry, "ray_cluster_node_count") == 0.0
    assert _value(registry, "ray_cluster_alive_node_count") == 0.0


def test_update_from_cluster_resources():
    registry = CollectorRegistry()
    collector = RayMetricsCollector(registry)

    collector.update_from_cluster_resources(ClusterResourceSnapshot(cpu_total=16.0, cpu_available=6.5))

    assert _value(registry, "ray_cluster_cpu_total") == 16.0
    assert _value(registry, "ray_cluster_cpu_available") == 6.5


def test_update_from_actors_counts_by_state():
    registry = CollectorRegistry()
    collector = RayMetricsCollector(registry)

    actors = [
        ActorSnapshot(actor_id="a1", state="ALIVE"),
        ActorSnapshot(actor_id="a2", state="ALIVE"),
        ActorSnapshot(actor_id="a3", state="DEAD"),
    ]
    collector.update_from_actors(actors)

    assert _value(registry, "ray_actor_state_count", {"state": "ALIVE"}) == 2.0
    assert _value(registry, "ray_actor_state_count", {"state": "DEAD"}) == 1.0


def test_update_from_actors_zeroes_out_states_that_disappear():
    registry = CollectorRegistry()
    collector = RayMetricsCollector(registry)

    collector.update_from_actors([ActorSnapshot(actor_id="a1", state="RESTARTING")])
    assert _value(registry, "ray_actor_state_count", {"state": "RESTARTING"}) == 1.0

    collector.update_from_actors([ActorSnapshot(actor_id="a1", state="ALIVE")])
    assert _value(registry, "ray_actor_state_count", {"state": "ALIVE"}) == 1.0
    assert _value(registry, "ray_actor_state_count", {"state": "RESTARTING"}) == 0.0


def test_update_from_tasks_counts_by_state():
    registry = CollectorRegistry()
    collector = RayMetricsCollector(registry)

    tasks = [
        TaskSnapshot(task_id="t1", state="FINISHED"),
        TaskSnapshot(task_id="t2", state="RUNNING"),
        TaskSnapshot(task_id="t3", state="FAILED"),
        TaskSnapshot(task_id="t4", state="FINISHED"),
    ]
    collector.update_from_tasks(tasks)

    assert _value(registry, "ray_task_state_count", {"state": "FINISHED"}) == 2.0
    assert _value(registry, "ray_task_state_count", {"state": "RUNNING"}) == 1.0
    assert _value(registry, "ray_task_state_count", {"state": "FAILED"}) == 1.0
