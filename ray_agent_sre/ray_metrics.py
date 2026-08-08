"""Turns Ray cluster state into Prometheus metrics.

The collector itself has no dependency on a live Ray process: update_from_*
takes plain snapshot objects and sets gauges from them, so it is unit
testable with fixture data. collect_live() is the thin adapter that pulls
those snapshots from a running cluster via ray.util.state, which is the same
public API a real multi-node Ray cluster exposes over its GCS address.
"""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Gauge


@dataclass(frozen=True)
class NodeSnapshot:
    node_id: str
    alive: bool
    cpu_total: float


@dataclass(frozen=True)
class ClusterResourceSnapshot:
    cpu_total: float
    cpu_available: float


@dataclass(frozen=True)
class ActorSnapshot:
    actor_id: str
    state: str


@dataclass(frozen=True)
class TaskSnapshot:
    task_id: str
    state: str


class RayMetricsCollector:
    """Owns the Ray-side Prometheus gauges and knows how to fill them in."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.registry = registry
        self.node_up = Gauge(
            "ray_node_up", "1 if the Ray node is alive, 0 otherwise", ["node_id"], registry=registry
        )
        self.node_cpu_total = Gauge(
            "ray_node_cpu_total", "Total CPU advertised by the node", ["node_id"], registry=registry
        )
        self.cluster_node_count = Gauge(
            "ray_cluster_node_count", "Total nodes known to the cluster", registry=registry
        )
        self.cluster_alive_node_count = Gauge(
            "ray_cluster_alive_node_count", "Nodes currently alive", registry=registry
        )
        self.cluster_cpu_total = Gauge(
            "ray_cluster_cpu_total", "Total CPU across the cluster", registry=registry
        )
        self.cluster_cpu_available = Gauge(
            "ray_cluster_cpu_available", "Available (unscheduled) CPU across the cluster", registry=registry
        )
        self.actor_state_count = Gauge(
            "ray_actor_state_count", "Number of actors in a given state", ["state"], registry=registry
        )
        self.task_state_count = Gauge(
            "ray_task_state_count", "Number of tasks in a given state", ["state"], registry=registry
        )
        self._known_actor_states: set[str] = set()
        self._known_task_states: set[str] = set()

    def update_from_nodes(self, nodes: list[NodeSnapshot]) -> None:
        self.cluster_node_count.set(len(nodes))
        alive = 0
        for node in nodes:
            self.node_up.labels(node_id=node.node_id).set(1 if node.alive else 0)
            self.node_cpu_total.labels(node_id=node.node_id).set(node.cpu_total)
            if node.alive:
                alive += 1
        self.cluster_alive_node_count.set(alive)

    def update_from_cluster_resources(self, snapshot: ClusterResourceSnapshot) -> None:
        self.cluster_cpu_total.set(snapshot.cpu_total)
        self.cluster_cpu_available.set(snapshot.cpu_available)

    def update_from_actors(self, actors: list[ActorSnapshot]) -> None:
        counts: dict[str, int] = {}
        for actor in actors:
            counts[actor.state] = counts.get(actor.state, 0) + 1
        self._known_actor_states |= set(counts)
        for state in self._known_actor_states:
            self.actor_state_count.labels(state=state).set(counts.get(state, 0))

    def update_from_tasks(self, tasks: list[TaskSnapshot]) -> None:
        counts: dict[str, int] = {}
        for task in tasks:
            counts[task.state] = counts.get(task.state, 0) + 1
        self._known_task_states |= set(counts)
        for state in self._known_task_states:
            self.task_state_count.labels(state=state).set(counts.get(state, 0))


def collect_live(collector: RayMetricsCollector) -> None:
    """Pulls current state from a running Ray cluster and updates collector.

    Requires ray.init() to already have been called in this process. Uses
    getattr defensively because ray.util.state's dataclass fields have
    changed across Ray minor versions.
    """
    import ray
    from ray.util.state import list_actors, list_nodes, list_tasks

    nodes = [
        NodeSnapshot(
            node_id=n.node_id,
            alive=(n.state == "ALIVE"),
            cpu_total=float((n.resources_total or {}).get("CPU", 0.0)),
        )
        for n in list_nodes()
    ]
    collector.update_from_nodes(nodes)

    cluster_total = ray.cluster_resources()
    cluster_available = ray.available_resources()
    collector.update_from_cluster_resources(
        ClusterResourceSnapshot(
            cpu_total=float(cluster_total.get("CPU", 0.0)),
            cpu_available=float(cluster_available.get("CPU", 0.0)),
        )
    )

    actors = [
        ActorSnapshot(actor_id=a.actor_id, state=a.state)
        for a in list_actors()
    ]
    collector.update_from_actors(actors)

    tasks = [
        TaskSnapshot(task_id=getattr(t, "task_id", "unknown"), state=t.state)
        for t in list_tasks()
    ]
    collector.update_from_tasks(tasks)
