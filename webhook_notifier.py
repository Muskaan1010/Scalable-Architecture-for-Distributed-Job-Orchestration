"""
Asynchronous Webhook and Event-Driven Notification Hook Handler.

Integrates with Scheduler callbacks to push real-time diagnostic JSON payloads
to destination monitoring webhooks when jobs complete or fail permanently.
"""

import json
import logging
import urllib.request
import threading
from typing import Any, Dict, Optional

from job_orchestrator.core.job import Job, JobState
from job_orchestrator.scheduler.runner import JobResult

logger = logging.getLogger(__name__)


class WebhookNotificationHandler:
    """Dispatches asynchronous JSON webhook posts upon orchestration event notifications."""

    def __init__(self, endpoint_url: str, cluster_env: str = "production"):
        """
        Initialize the notification hook handler.

        Args:
            endpoint_url: The destination URI endpoint for the webhook payload.
            cluster_env: Name of the active cluster environment layout.
        """
        self.endpoint_url = endpoint_url
        self.cluster_env = cluster_env

    def _send_http_post(self, payload: Dict[str, Any]) -> None:
        """Executes the outbound HTTP request safely in a background fire-and-forget worker thread."""
        def target():
            try:
                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(
                    self.endpoint_url,
                    data=data,
                    headers={"Content-Type": "application/json",
                             "User-Agent": "JobOrchestrator-Notifier/1.0"},
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=5.0) as response:
                    # Flush stream to prevent resource tracking hangs
                    response.read()
            except Exception as exc:
                logger.error(
                    "Failed to route external notification webhook update: %s", exc)

        # Spawn thread immediately to ensure notifications never block critical core runner loops
        threading.Thread(
            target=target, name="webhook-dispatcher", daemon=True).start()

    def on_job_completed(self, job: Job, result: JobResult) -> None:
        """Triggered upon atomic task completion success callbacks."""
        payload = {
            "event": "job.completed",
            "environment": self.cluster_env,
            "timestamp": datetime.utcnow().isoformat() if hasattr(datetime, "utcnow") else None,
            "data": {
                "job_id": str(job.id),
                "name": job.name,
                "execution_time_seconds": job.execution_time,
                "priority": job.priority.name if hasattr(job.priority, "name") else str(job.priority)
            }
        }
        self._send_http_post(payload)

    def on_job_failed_permanently(self, job: Job, result: JobResult) -> None:
        """Triggered when task exhaust attempts and are routed into the Dead Letter Queue."""
        payload = {
            "event": "job.failed_permanently",
            "environment": self.cluster_env,
            "severity": "CRITICAL",
            "data": {
                "job_id": str(job.id),
                "name": job.name,
                "final_attempts_count": job.attempt,
                "error_message": result.error or "Unknown failure outcome",
                "traceback_snapshot": result.traceback[:1000] if result.traceback else None
            }
        }
        self._send_http_post(payload)

    def on_dag_state_finalized(self, dag_id: str, status: str, error: Optional[str] = None) -> None:
        """Triggered upon top-level execution workflow completion milestones."""
        payload = {
            "event": "dag.finalized",
            "environment": self.cluster_env,
            "data": {
                "dag_id": dag_id,
                "outcome_status": status,  # completed, failed, cancelled
                "error_reason": error
            }
        }
        self._send_http_post(payload)
