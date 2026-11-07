import threading
from uuid import uuid4

from guardian_layer.managers.buffer_manger import BufferManager
from guardian_layer.managers.interface.manager_interface import ManagerInterface
from guardian_layer.utils.channels import FlushSink
from guardian_layer.utils.schema import Ack, Buffer, Cluster, Component, Sensor, Status


class ClusterManager(ManagerInterface):
    """Manage clusters and the buffers belonging to each sensor."""

    def __init__(self, sink: FlushSink) -> None:
        self.clusters: dict[str, Cluster] = {}
        self._sink = sink  # handed to every BufferManager; the manager does not own it
        self._lock = threading.Lock()  # guards check-then-insert on clusters/buffers

    def validate(self, value: str) -> Ack:
        """Check whether a cluster exists."""

        if value not in self.clusters:
            return Ack(
                id=str(uuid4()),
                detail=f"Cluster {value} not found",
                status=Status.FAILED,
                sender=Component.CLUSTER,
                receiver=Component.CONSUMER,
            )

        return Ack(
            id=str(uuid4()),
            detail=f"Cluster {value} exists",
            status=Status.SUCCESS,
            sender=Component.CLUSTER,
            receiver=Component.CONSUMER,
        )

    def create(self, value: Sensor) -> Cluster | None:
        """Create a cluster and initialize its first sensor buffer manager."""
        with self._lock:
            if self.validate(value.cluster_id).ok:
                return None

            sensor_type = (
                value.metadata.type if value.metadata is not None else value.cluster_id
            )
            cluster = Cluster(id=value.cluster_id, sensor_type=sensor_type)
            cluster.buffers[value.sensor_id] = BufferManager(self._sink)
            self.clusters[cluster.id] = cluster
            return cluster

    def add(self, sensor: Sensor) -> Ack:
        """Add a sensor reading to its cluster buffer."""

        cluster = self.clusters.get(sensor.cluster_id)
        if cluster is None:
            return Ack(
                id=str(uuid4()),
                detail=f"Cluster {sensor.cluster_id} not found",
                status=Status.FAILED,
                sender=Component.CLUSTER,
                receiver=Component.CONSUMER,
            )

        buffer_manager = cluster.buffers.get(sensor.sensor_id)
        if buffer_manager is None:
            with self._lock:  # two first readings must not make two managers
                buffer_manager = cluster.buffers.setdefault(
                    sensor.sensor_id, BufferManager(self._sink)
                )
        if sensor.sensor_id not in buffer_manager.buffers:
            buffer_manager.create(sensor.sensor_id)
        buffer_ack = buffer_manager.add(sensor)
        if buffer_ack.ok:
            buffer = buffer_manager.get(sensor.sensor_id)
            if isinstance(buffer, Buffer):
                cluster.last_update = buffer.last_update

        return Ack(
            id=str(uuid4()),
            detail=buffer_ack.detail,
            status=buffer_ack.status,
            sender=Component.CLUSTER,
            receiver=Component.CONSUMER,
        )

    def delete(self, id: str) -> Ack:
        """Delete a cluster and all its buffers."""

        validation = self.validate(id)
        if not validation.ok:
            return validation

        del self.clusters[id]
        return Ack(
            id=str(uuid4()),
            detail=f"Cluster {id} deleted successfully",
            status=Status.SUCCESS,
            sender=Component.CLUSTER,
            receiver=Component.CONSUMER,
        )

    def get(self, value: str) -> Cluster | Ack:
        """Return a cluster by its ID."""

        validation = self.validate(value)
        if not validation.ok:
            return validation
        return self.clusters[value]

    def __len__(self) -> int:
        """Return the number of clusters."""

        return len(self.clusters)
