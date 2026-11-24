"""
Multi-stage Workflow Stress-Testing Simulation Script.

Simulates a high-throughput transaction matching 100 concurrent tasks 
to trigger scale-up actions on the WorkerPool configurations.
"""

from job_orchestrator.workers.pool import WorkerPool, PoolConfig, WorkerType
from job_orchestrator.scheduler.scheduler import Scheduler
from job_orchestrator.core.dag import DAGBuilder
from job_orchestrator.core.config import OrchestratorConfig
import os
import sys
import time
import random
from datetime import datetime, timezone

# Adjust path to map the src layout natively
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../src')))


# ------------------------------------------------------------------
# Concrete Workload Task Blueprints (Must be importable via string path)
# ------------------------------------------------------------------

def stage_ingest_batch(batch_id: int):
    """Simulates multi-source database data extraction ingestion chunks."""
    time.sleep(random.uniform(0.2, 0.5))  # Artificial I/O block
    return f"chunk_data_payload_v1_{batch_id}"


def stage_compute_analytics(raw_payload: str):
    """Simulates intense mathematical computation matrix calculations."""
    # Simulates CPU-bound algorithmic strain
    start = time.time()
    while time.time() - start < 0.3:
        _ = 42 * 42
    return f"processed_metrics_{raw_payload}"


def stage_aggregate_pipeline(results_list: list):
    """Consolidates transactional calculations directly."""
    return {"status": "success", "items_processed": len(results_list)}

# ------------------------------------------------------------------
# Master Simulation Execution Loop
# ------------------------------------------------------------------


def main():
    print("==================================================================")
    print("   Starting Job Orchestrator Pipeline Stress Simulation Tool      ")
    print("==================================================================")

    # 1. Initialize Unified Component Configurations
    config = OrchestratorConfig()
    config.queue.max_size = 5000
    config.job_timeout = 60.0

    scheduler = Scheduler(config)

    pool_config = PoolConfig(
        min_workers=2,
        max_workers=16,               # High scaling cap ceiling parameters
        worker_type=WorkerType.THREAD,  # Fast dynamic local context switches
        scale_up_threshold=0.5,       # Agile capacity additions
        scale_down_threshold=0.1,
        scale_interval=2.0            # Rapid polling intervals for local testing
    )
    pool = WorkerPool(scheduler, pool_config)

    # 2. Boot up Cluster Infrastructure Components
    scheduler.start()
    pool.start()

    try:
        print("\n[1/3] Generating Load Workflows Graph structure mapping...")
        builder = DAGBuilder("black_friday_analytics_pipeline",
                             "Aggressive workload stress matrix")

        # Spawn 100 parallel data ingestion stages to flood queue depths instantly
        batch_size = 100
        ingest_keys = []
        compute_keys = []

        for idx in range(batch_size):
            ingest_key = f"ingest_batch_{idx}"
            compute_key = f"compute_analytics_{idx}"

            # Map parent task functions
            builder.add_job(
                job_or_func=stage_ingest_batch,
                job_id=ingest_key,
                kwargs={"batch_id": idx}
            )
            ingest_keys.append(ingest_key)

            # Map child functions pointing dependencies to ingestion parents
            builder.add_job(
                job_or_func=stage_compute_analytics,
                job_id=compute_key,
                depends_on=[ingest_key],
                # Variable placeholder token string reference mapping
                args=(f"{{#{ingest_key}.result}}",)
            )
            compute_keys.append(compute_key)

        # Add final downstream aggregation milestone bottleneck task depending on ALL children calculations
        builder.add_job(
            job_or_func=stage_aggregate_pipeline,
            job_id="final_aggregation_sink",
            depends_on=compute_keys,
            kwargs={"results_list": compute_keys}
        )

        dag = builder.build()

        print(
            f"\n[2/3] Submitting Workflow Matrix ({len(dag.jobs)} total connected tasks)...")
        dag_id = scheduler.submit_dag(dag)

        print(
            "\n[3/3] Monitoring Real-time Scaling Mechanics and Queue Depth Evictions:")
        print("------------------------------------------------------------------")

        # Actively poll stats to print structural performance changes live in console
        while True:
            stats = scheduler.get_stats()
            pool_stats = pool.get_stats()
            dag_status = scheduler.get_dag_status(dag_id)

            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                  f"Queue Size: {stats['queue']['total_size']} | "
                  f"Pool Workers: {pool_stats.total_workers} (Busy: {pool_stats.busy_workers}) | "
                  f"DAG Status: {dag_status['status']} ({dag_status['progress']*100:.1f}%)")

            if dag_status['status'] in ('completed', 'failed'):
                print(
                    "------------------------------------------------------------------")
                print(
                    f"Simulation workflow terminated successfully. Outcome: {dag_status['status'].upper()}")
                break

            time.sleep(1.5)

    except KeyboardInterrupt:
        print("\nAborting load simulation runs...")
    finally:
        print("\nGracefully cleaning up thread tracking bounds...")
        pool.stop(wait=True)
        scheduler.stop()
        print("Cluster engine successfully stopped. Cleanup completed safely.")


if __name__ == "__main__":
    main()
