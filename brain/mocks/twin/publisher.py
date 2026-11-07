"""The mock Twin and Guardian on a real broker, so the triplet wiring can be run for real.

This is the `mocks-kafka` profile: the mock Twin publishes `twin.sensors` and serves its MCP
tools, and the mock Guardian publishes `guardian.verdicts` and `guardian.events` beside it.
The Guardian's public key is written where the Brain can read it, because the Brain refuses
verdicts it cannot verify and a container has no other way to be told.
"""

import argparse
import asyncio
import json
from pathlib import Path

from mocks.guardian.mock_guardian import MockGuardian, trust_all
from mocks.keys import derive_keypair
from mocks.twin import MockTwin
from mocks.twin.mcp_server import build_mock_mcp_server

TICK_S = 0.1
TOPICS = {"readings": "twin.sensors", "verdicts": "guardian.verdicts", "events": "guardian.events"}


def _producer(bootstrap: str):
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": bootstrap, "linger.ms": 5, "enable.idempotence": True})


async def publish_forever(
    twin: MockTwin, guardian: MockGuardian, producer, tick_s: float = TICK_S, ticks: int | None = None
) -> int:
    """One tick of the city per `tick_s`: readings, then the verdicts they earned."""
    published = 0
    tick = 0
    while ticks is None or tick < ticks:
        for reading in twin.tick():
            producer.produce(
                TOPICS["readings"], key=reading["device_id"].encode("utf-8"),
                value=json.dumps(reading, separators=(",", ":")).encode("utf-8"))
            published += 1
            for verdict in guardian.on_reading(reading):
                producer.produce(
                    TOPICS["verdicts"], key=reading["device_id"].encode("utf-8"), value=verdict)
                published += 1
        for event in guardian.drain_events():
            producer.produce(TOPICS["events"], value=event)
            published += 1
        producer.poll(0)
        tick += 1
        await asyncio.sleep(tick_s)
    producer.flush(5.0)
    return published


async def serve(args: argparse.Namespace) -> int:
    twin = MockTwin(seed=args.seed)
    private_key, public_key = derive_keypair(f"{args.seed}:guardian")
    if args.guardian_key_out:
        out = Path(args.guardian_key_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(public_key, encoding="ascii")
    print(f"guardian public key: {public_key}", flush=True)
    guardian = MockGuardian(private_key, policy=trust_all())
    server = build_mock_mcp_server(twin)
    server.settings.host, server.settings.port = args.host, args.port
    producer = _producer(args.bootstrap)
    tasks = [
        asyncio.create_task(server.run_sse_async(), name="mcp"),
        asyncio.create_task(publish_forever(twin, guardian, producer), name="kafka"),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mocks.twin.publisher")
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--guardian-key-out", default="/app/logs/guardian-triplet07.pub")
    args = parser.parse_args(argv)
    return asyncio.run(serve(args))


if __name__ == "__main__":
    raise SystemExit(main())
