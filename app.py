import os
import time
import sys
# Adjust import paths as needed
from job_orchestrator.core.config import OrchestratorConfig
from job_orchestrator.scheduler.scheduler import Scheduler
from job_orchestrator.workers.pool import WorkerPool


def main():
    print("Initializing Job Orchestrator from environment settings...")

    # 1. Parse configuration parameters from the environment variables set by compose
    storage_conn = os.getenv("JOB_ORCHESTRATOR_STORAGE_CONNECTION_STRING")
    locking_conn = os.getenv("JOB_ORCHESTRATOR_LOCKING_CONNECTION_STRING")

    min_w = int(os.getenv("JOB_ORCHESTRATOR_WORKER_POOL_MIN_WORKERS", "2"))
    max_w = int(os.getenv("JOB_ORCHESTRATOR_WORKER_POOL_MAX_WORKERS", "8"))

    # 2. Instantiate core application engines
    # (Adapt class parameters to match your internal class structures)
    scheduler = Scheduler(storage_uri=storage_conn, lock_uri=locking_conn)

    pool = WorkerPool(
        scheduler=scheduler,
        min_workers=min_w,
        max_workers=max_w
    )

    # 3. Boot services
    scheduler.start()
    pool.start()

    print(
        f"Job Orchestrator active with cluster pool allocation: [{min_w}-{max_w} workers]")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Gracefully terminating cluster processes...")
        pool.stop(wait=True)
        scheduler.stop(wait=True)


if __name__ == "__main__":
    main()
