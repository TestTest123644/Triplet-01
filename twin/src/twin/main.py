"""Unified Runtime Runner for Twin Layer (Layer 1).

Orchestrates both:
1. Fixed-timestep 10 Hz Physics Simulation streaming signed telemetry to Kafka.
2. Synchronous MCP Command & Query Server exposing FastMCP/MCPServer on port 8000.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from typing import Optional

import uvicorn

from twin.kafka_streamer import KafkaStreamer
from twin.mcp_server import mcp, set_active_simulator
from twin.red_team_api import build_red_team_app, get_or_create_red_team_token
from twin.simulation import CitySimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("twin.main")


async def wait_for_kafka_ready(bootstrap_servers: str, max_retries: int = 30, delay: float = 1.0) -> bool:
    """Check connectivity to Kafka broker before starting high-rate streaming."""
    logger.info(f"Checking connectivity to Kafka broker at {bootstrap_servers}...")
    import confluent_kafka.admin

    for attempt in range(1, max_retries + 1):
        try:
            admin = confluent_kafka.admin.AdminClient({"bootstrap.servers": bootstrap_servers, "socket.timeout.ms": 2000})
            metadata = admin.list_topics(timeout=2.0)
            if metadata.brokers:
                logger.info(f"Connected to Kafka broker at {bootstrap_servers} (Brokers: {list(metadata.brokers.keys())})")
                return True
        except Exception as e:
            logger.warning(f"Waiting for Kafka ({attempt}/{max_retries}): {e}")
            await asyncio.sleep(delay)

    logger.error(f"Failed to connect to Kafka broker after {max_retries} attempts.")
    return False


async def run_simulation_stream_loop(
    sim: CitySimulator,
    streamer: KafkaStreamer,
    stop_event: asyncio.Event,
    target_hz: float = 10.0,
) -> None:
    """Fixed-rate 10 Hz loop: updates physics, logs truth ledger, signs & streams to Kafka."""
    step_duration = 1.0 / target_hz
    logger.info(f"Starting 10 Hz simulation loop (42 devices, 420 msgs/sec target)...")

    tick_count = 0
    start_time = time.monotonic()

    while not stop_event.is_set():
        cycle_start = time.monotonic()

        # Execute 1 tick across all 42 devices
        readings = sim.tick()

        # Stream readings to Kafka
        for raw in readings:
            dev = sim.devices.get(raw["device_id"])
            if dev:
                try:
                    streamer.stream_reading(raw, dev.private_key)
                except Exception as e:
                    logger.error(f"Error streaming reading {raw.get('reading_id')}: {e}")

        tick_count += 1
        if tick_count % 100 == 0:
            elapsed = time.monotonic() - start_time
            rate = (tick_count * len(readings)) / elapsed if elapsed > 0 else 0.0
            logger.info(
                f"Tick {tick_count} | Sim Time: {sim.get_sim_time()} | "
                f"Published: {streamer.sent_count} msgs | Throughput: {rate:.1f} msgs/sec"
            )

        # Enforce exact 10 Hz wall-clock cycle pacing
        elapsed_cycle = time.monotonic() - cycle_start
        sleep_needed = step_duration - elapsed_cycle
        if sleep_needed > 0:
            await asyncio.sleep(sleep_needed)
        else:
            # Yield control to event loop even if cycle overran slightly
            await asyncio.sleep(0.001)

    # Flush lingering messages before exiting
    streamer.flush(timeout=2.0)
    logger.info("Simulation stream loop shut down cleanly.")


async def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("KAFKA_TOPIC", "twin.sensors")
    mcp_host = os.getenv("MCP_HOST", "0.0.0.0")
    mcp_port = int(os.getenv("MCP_PORT", "8000"))

    logger.info("Initializing Twin Layer (Layer 1) unified runtime...")

    # 1. Initialize shared Physics Simulator
    sim = CitySimulator(seed=int(os.getenv("SIM_SEED", "42")))
    set_active_simulator(sim)
    logger.info(f"Initialized CitySimulator with {len(sim.devices)} devices across 6 zones.")

    # 2. Wait for Kafka if in production container
    max_retries = int(os.getenv("KAFKA_WAIT_RETRIES", "30"))
    if os.getenv("WAIT_FOR_KAFKA", "true").lower() in ("true", "1", "yes"):
        kafka_ready = await wait_for_kafka_ready(bootstrap_servers=bootstrap_servers, max_retries=max_retries)
        if not kafka_ready:
            logger.warning(
                "Kafka broker not available. Twin Layer will stream to internal sink buffer while keeping MCP Server fully operational."
            )

    # 3. Initialize Kafka Streamer
    streamer = KafkaStreamer(bootstrap_servers=bootstrap_servers, topic=topic)

    red_team_host = os.getenv("RED_TEAM_HOST", "127.0.0.1")
    red_team_port = int(os.getenv("RED_TEAM_PORT", "8100"))

    # 4. Configure MCP ASGI Server (strictly Port 8000, no /api routes)
    mcp_app = mcp.sse_app(host=mcp_host)
    mcp_config = uvicorn.Config(
        app=mcp_app,
        host=mcp_host,
        port=mcp_port,
        log_level="info",
        access_log=False,
    )
    mcp_server = uvicorn.Server(mcp_config)

    # 5. Configure Isolated Red Team Adversary REST Server (Port 8100, token protected)
    token = get_or_create_red_team_token()
    red_team_app = build_red_team_app(sim, token=token)
    red_team_config = uvicorn.Config(
        app=red_team_app,
        host=red_team_host,
        port=red_team_port,
        log_level="info",
        access_log=False,
    )
    red_team_server = uvicorn.Server(red_team_config)

    stop_event = asyncio.Event()

    def handle_signal(*_):
        logger.info("Received termination signal. Shutting down Twin Layer...")
        stop_event.set()
        mcp_server.should_exit = True
        red_team_server.should_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handle_signal)
        except (ValueError, AttributeError):
            pass

    # 6. Run all three tasks concurrently in the same event loop
    sim_task = asyncio.create_task(run_simulation_stream_loop(sim, streamer, stop_event))
    mcp_task = asyncio.create_task(mcp_server.serve())
    red_team_task = asyncio.create_task(red_team_server.serve())

    logger.info(f"Twin Layer online. MCP Command Server listening on http://{mcp_host}:{mcp_port}/sse")
    logger.info(f"Red Team Adversary API listening on http://{red_team_host}:{red_team_port}/api/status")
    logger.info(f"Kafka stream publishing to {bootstrap_servers} topic '{topic}'")

    done, pending = await asyncio.wait(
        [sim_task, mcp_task, red_team_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    stop_event.set()
    mcp_server.should_exit = True
    red_team_server.should_exit = True

    for task in pending:
        task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Twin Layer stopped by user.")
