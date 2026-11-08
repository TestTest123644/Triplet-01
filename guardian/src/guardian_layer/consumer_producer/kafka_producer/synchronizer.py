import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from guardian_layer.consumer_producer.kafka_producer.utils.logger import get_logger
from guardian_layer.consumer_producer.kafka_producer.utils.settings import get_settings

settings = get_settings()
logger = get_logger(__name__)

SYNC_INTERVAL = 30 * 60
METADATA_TOPIC = "devices.metadata"

DEVICE_FIELDS = (
    "device_type",
    "unit",
    "lat",
    "lon",
    "min_val",
    "max_val",
    "zone_id",
)


class DeviceMetadataSynchronizer:
    def __init__(
        self,
        call_tool: Callable[..., Awaitable[Any]],
        bootstrap_servers: str,
        cache_path: str = "cache.json",
    ):
        logger.info("i'm int the constructor of DeviceMetadataSynchronizer")
        self.call_tool = call_tool
        self.cache_path = Path(cache_path)
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "PLAIN",
                "sasl.username": settings.kafka.username,  # "broadcast"
                "sasl.password": settings.kafka.password,
            }
        )
        logger.info(
            f"settings.kafka.password: {settings.kafka.password} and"
            + f"settings.kafka.username: {settings.kafka.username}"
        )
        self.cached_zones: set[str] = set()
        self.cached_devices: dict[str, dict[str, Any]] = {}

        self._load_cache()

    # --------------------------------------------------
    # Cache management
    # --------------------------------------------------

    def _load_cache(self) -> None:
        if not self.cache_path.exists() or self.cache_path.stat().st_size == 0:
            logger.info("No previous metadata cache found.")
            return
        try:
            with self.cache_path.open("r", encoding="utf-8") as file:
                cache = json.load(file)
        except (json.JSONDecodeError, OSError):
            logger.warning("Cache unreadable, starting empty", exc_info=True)
            return
        self.cached_zones = set(cache.get("zones", []))
        self.cached_devices = cache.get("devices", {})

    def _save_cache(self) -> None:
        self.cache_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path = self.cache_path.with_suffix(".tmp")

        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(
                {
                    "zones": sorted(self.cached_zones),
                    "devices": self.cached_devices,
                },
                file,
                indent=2,
            )

        temporary_path.replace(self.cache_path)

    # --------------------------------------------------
    # MCP calls
    # --------------------------------------------------

    @staticmethod
    def _unwrap_result(result: Any) -> Any:
        """
        Support both regular Python results and MCP CallToolResult.
        """
        if isinstance(result, (dict, list)):
            return result

        if getattr(result, "isError", False):
            raise RuntimeError(f"MCP tool returned an error: {result}")

        structured = getattr(result, "structuredContent", None)

        if structured is not None:
            return structured

        content = getattr(result, "content", None)
        result = []
        if content:
            for item in content:
                if hasattr(item, "text"):
                    result.append(json.loads(item.text))
            return result
        raise ValueError("Unable to decode MCP tool response")

    async def _call(self, name: str, arguments: dict) -> Any:
        result = await self.call_tool(name, arguments)
        logger.debug("MCP call %s returned: %r", name, result)
        result = self._unwrap_result(result)
        return result

    async def _fetch_snapshot(
        self,
    ) -> tuple[set[str], dict[str, dict[str, Any]]]:

        # 1. Get city state and discover available zones.
        city_state = await self._call(
            "get_city_state",
            {},
        )
        zones = city_state[0]["zones"]

        zone_ids = set(zones.keys())

        logger.info("Discovered %d zones", len(zone_ids))

        # 2. Fetch device metadata for every zone.
        devices: dict[str, dict[str, Any]] = {}

        for zone_id in sorted(zone_ids):
            zone_devices = await self._call(
                "list_devices",
                {"zone_id": zone_id},
            )
            for device in zone_devices:
                device_id = device["device_id"]

                # Keep only the metadata fields we need.
                metadata = {
                    "device_id": device_id,
                    "zone_id": device["zone_id"],
                    **{
                        field: device[field]
                        for field in DEVICE_FIELDS
                        if field != "zone_id"
                    },
                }

                if device_id in devices:
                    raise ValueError(f"Duplicate device_id detected: {device_id}")

                devices[device_id] = metadata

        return zone_ids, devices

    # --------------------------------------------------
    # Difference detection
    # --------------------------------------------------

    def _compare(
        self,
        current_zones: set[str],
        current_devices: dict[str, dict[str, Any]],
    ) -> tuple[list[dict], list[str]]:

        added_or_updated = []
        deleted = []

        old_ids = set(self.cached_devices)
        new_ids = set(current_devices)

        # New devices.
        for device_id in new_ids - old_ids:
            added_or_updated.append(
                {
                    "operation": "upsert",
                    **current_devices[device_id],
                }
            )

        # Deleted devices.
        for device_id in old_ids - new_ids:
            deleted.append(device_id)

        # Existing devices: detect modifications.
        for device_id in old_ids & new_ids:
            old = self.cached_devices[device_id]
            new = current_devices[device_id]

            changed = any(old.get(field) != new.get(field) for field in DEVICE_FIELDS)

            if changed:
                added_or_updated.append(
                    {
                        "operation": "upsert",
                        **new,
                    }
                )

        # Detect zone changes.
        added_zones = current_zones - self.cached_zones
        removed_zones = self.cached_zones - current_zones

        if added_zones:
            logger.info("New zones: %s", sorted(added_zones))

        if removed_zones:
            logger.info("Removed zones: %s", sorted(removed_zones))

        return added_or_updated, deleted

    # --------------------------------------------------
    # Kafka publishing
    # --------------------------------------------------

    def _publish(
        self,
        messages: list[dict],
        deleted: list[str],
    ) -> None:

        delivery_errors = []

        def on_delivery(error, message):
            if error:
                delivery_errors.append(error)

        for message in messages:
            self.producer.produce(
                topic=METADATA_TOPIC,
                key=message["device_id"],
                value=json.dumps(message),
                on_delivery=on_delivery,
            )

        for device_id in deleted:
            message = {
                "operation": "delete",
                "device_id": device_id,
            }

            self.producer.produce(
                topic=METADATA_TOPIC,
                key=device_id,
                value=json.dumps(message),
                on_delivery=on_delivery,
            )

        remaining = self.producer.flush(timeout=30)

        if remaining:
            raise TimeoutError(f"{remaining} Kafka messages were not delivered")

        if delivery_errors:
            raise RuntimeError(f"Kafka delivery failed: {delivery_errors}")

    # --------------------------------------------------
    # Synchronization
    # --------------------------------------------------

    async def sync(self) -> None:

        logger.info("Starting device metadata synchronization")

        # Fetch a complete new snapshot.
        current_zones, current_devices = await self._fetch_snapshot()

        # Compare against previous snapshot.
        messages, deleted = self._compare(
            current_zones,
            current_devices,
        )

        logger.info(
            "Detected changes: %d upserts, %d deletions",
            len(messages),
            len(deleted),
        )

        # Publish changes before updating the cache.
        if messages or deleted:
            self._publish(messages, deleted)

        # Update local snapshot only after successful publication.
        self.cached_zones = current_zones
        self.cached_devices = current_devices

        self._save_cache()

        logger.info("Metadata synchronization completed")

    async def run(self) -> None:

        while True:
            try:
                await self.sync()

            except Exception:
                logger.exception("Metadata synchronization failed")

            await asyncio.sleep(SYNC_INTERVAL)
