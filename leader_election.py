"""
Leader Election coordinator node script for the Job Orchestrator cluster.

Ensures only one replica runs system cleanup maintenance and DAG structural updates,
while exposing global performance metrics for Prometheus scraping on port 8080.
"""

import os
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from job_orchestrator.core.config import OrchestratorConfig
from job_orchestrator.locking.redis_lock import RedisLockManager
from job_orchestrator.scheduler.scheduler import Scheduler
from job_orchestrator.workers.pool import WorkerPool
from webhook_notifier import WebhookNotificationHandler

# 🧠 CRITICAL IMPORT: Ensure standard prometheus_client exporter is loaded
from prometheus_client import start_http_server

# Resolve Node Identity dynamically using the container hostname
NODE_ID = os.getenv("HOSTNAME", socket.gethostname())


def run_as_leader(lock_manager: RedisLockManager, scheduler: Scheduler) -> None:
    """Run leader responsibilities with active lock lease extension protections."""
    print(f"[{datetime.now(timezone.utc)}] Node {NODE_ID} successfully elected as Cluster Leader.")

    # Track when we last extended our lock to keep it alive
    last_renew = time.time()

    while scheduler.is_running:
        now = time.time()

        # Explicit Heartbeat Lease Renewal Loop
        # Extend lock every 10 seconds to keep the 30-second TTL from expiring
        if now - last_renew >= 10.0:
            extended = lock_manager.extend(
                "job_orchestrator:leader", owner=f"node-{NODE_ID}", ttl=30.0)
            if not extended:
                print(
                    f"[{datetime.now(timezone.utc)}] CRITICAL: Leadership lease lost! Stepping down.", file=sys.stderr)
                break  # Break out immediately to avoid Split-Brain race conditions
            last_renew = now

        # Execute Leader administrative responsibilities
        try:
            # Regularly sample metrics stats dictionary to refresh global gauges
            stats = scheduler.get_stats()
        except Exception as e:
            print(f"Error executing leader routines: {e}", file=sys.stderr)

        # Low loop cycle to handle rapid lease checks alongside tasks
        time.sleep(2)


def main() -> None:
    # 🧠 FIXED: Force the Prometheus client to listen across ALL network interfaces (0.0.0.0)
    # This ensures that internal Docker health check probes can connect to the port smoothly!
    try:
        start_http_server(8080, addr="0.0.0.0")
        print(
            f"[{datetime.now(timezone.utc)}] Node {NODE_ID} initialized Metrics Exporter on 0.0.0.0:8080")
    except Exception as e:
        print(
            f"CRITICAL: Failed to start Prometheus server on port 8080: {e}", file=sys.stderr)
        pass

    # 1. Load configuration and lock structures from the cluster layout environment
    config = OrchestratorConfig.from_env()
    lock_manager = RedisLockManager(
        redis_urls=["redis://redis:6379/0"],
        key_prefix="cluster:leader:"
    )

    # 2. Instantiate the core scheduler engine instance
    scheduler = Scheduler(config)

    # 3. Instantiate and wire your Slack Webhook Notification engine
    notifier = WebhookNotificationHandler(
        endpoint_url="https://slack.com",
        cluster_env="staging-us-east"
    )

    # Bind the proxy callbacks to listen to execution outcomes across all scaling nodes
    scheduler.on_job_complete(notifier.on_job_completed)
    scheduler.on_job_failed(notifier.on_job_failed_permanently)

    # 4. Instantiate the cluster worker pools manager
    pool = WorkerPool(scheduler)

    print(f"Starting Cluster Instance Loop: {NODE_ID}")

    # 5. Boot up background tracking engines
    scheduler.start()
    pool.start()

    try:
        while scheduler.is_running:
            # Try to non-blockingly capture cluster leadership
            lock = lock_manager.acquire(
                resource="job_orchestrator:leader",
                owner=f"node-{NODE_ID}",
                timeout=0,  # Return immediately if another replica holds leadership
                ttl=30.0,
            )

            if lock:
                try:
                    run_as_leader(lock_manager, scheduler)
                finally:
                    # Cleanly release leadership lock so other nodes can failover instantly on shutdown
                    lock_manager.release(
                        "job_orchestrator:leader", f"node-{NODE_ID}")
                    print(
                        f"[{datetime.now(timezone.utc)}] Node {NODE_ID} released leadership cleanly.")
            else:
                # Node is operating as a standard follower processing engine worker jobs
                # Periodically sample stats to keep standard follower gauge metrics accurate
                try:
                    scheduler.get_stats()
                except Exception:
                    pass
                time.sleep(5)

    except KeyboardInterrupt:
        print("\nShutdown signal captured. Terminating cluster processes gracefully...")
    finally:
        pool.stop(wait=True)
        scheduler.stop()
        print(f"Node {NODE_ID} successfully terminated.")


if __name__ == "__main__":
    main()
