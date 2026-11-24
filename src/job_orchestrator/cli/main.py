"""
Main CLI entry point for the Job Orchestrator.

Provides subcommands for job management, scheduler control, and
status inspection. Uses argparse for zero external dependencies.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..core.config import OrchestratorConfig
from ..core.job import Job, JobPriority, JobState


def _get_scheduler(config_path: Optional[str] = None):
    """Initialize scheduler from config file or defaults."""
    from ..scheduler.scheduler import Scheduler

    if config_path:
        if config_path.endswith((".yml", ".yaml")):
            config = OrchestratorConfig.from_yaml(config_path)
        elif config_path.endswith(".toml"):
            config = OrchestratorConfig.from_toml(config_path)
        else:
            raise ValueError(f"Unsupported config format: {config_path}")
    else:
        config = OrchestratorConfig.from_env()

    return Scheduler(config)


def _format_table(headers: List[str], rows: List[List[str]], max_width: int = 120) -> str:
    """Format data as an aligned text table."""
    if not rows:
        return "No results."

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(cell)))

    # Truncate columns if total width exceeds max
    total = sum(col_widths) + (len(headers) - 1) * 3
    if total > max_width:
        widest_idx = col_widths.index(max(col_widths))
        col_widths[widest_idx] = max(
            10, col_widths[widest_idx] - (total - max_width))

    header_line = " | ".join(
        h.ljust(col_widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(headers)))

    lines = [header_line, separator]
    for row in rows:
        line = " | ".join(
            str(cell).ljust(col_widths[i])[:col_widths[i]] for i, cell in enumerate(row)
        )
        lines.append(line)

    return "\n".join(lines)


def _format_json(data: Any) -> str:
    """Format data as indented JSON."""
    return json.dumps(data, indent=2, default=str)


def cmd_status(args: argparse.Namespace) -> int:
    """Display scheduler status and statistics."""
    scheduler = _get_scheduler(args.config)

    # 🧠 FIX: Extract state metrics from storage without starting execution cycles
    stats = scheduler.get_stats()

    if args.json:
        print(_format_json(stats))
    else:
        print("=== Job Orchestrator Status ===")
        print(f"  Running:          {stats.get('is_running', False)}")
        print(f"  Jobs submitted:   {stats.get('jobs_submitted', 0)}")
        print(f"  Jobs completed:   {stats.get('jobs_completed', 0)}")
        print(f"  Jobs failed:      {stats.get('jobs_failed', 0)}")
        print(f"  Jobs retried:     {stats.get('jobs_retried', 0)}")
        print(f"  Jobs in DLQ:      {stats.get('jobs_sent_to_dlq', 0)}")
        print(f"  DAGs submitted:   {stats.get('dags_submitted', 0)}")
        print(f"  DAGs completed:   {stats.get('dags_completed', 0)}")
        print(f"  DAGs failed:      {stats.get('dags_failed', 0)}")
        print()

        queue_stats = stats.get("queue", {})
        if queue_stats:
            print("=== Queue ===")
            print(f"  Size:             {queue_stats.get('size', 0)}")
            print(
                f"  Max size:         {queue_stats.get('max_size', 'unlimited')}")
            print(f"  Total pushed:     {queue_stats.get('total_pushed', 0)}")
            print(f"  Total popped:     {queue_stats.get('total_popped', 0)}")
            print()

        dlq_stats = stats.get("dlq", {})
        if dlq_stats:
            print("=== Dead Letter Queue ===")
            print(f"  Pending:          {dlq_stats.get('pending_count', 0)}")
            print(f"  Resolved:         {dlq_stats.get('resolved_count', 0)}")
            print(f"  Discarded:        {dlq_stats.get('discarded_count', 0)}")
            print(f"  Total:            {dlq_stats.get('total_count', 0)}")

    return 0


def cmd_list_jobs(args: argparse.Namespace) -> int:
    """List jobs with optional state filter."""
    scheduler = _get_scheduler(args.config)

    state_filter = None
    if args.state:
        try:
            state_filter = JobState(args.state)
        except ValueError:
            print(f"Error: Invalid state '{args.state}'. Valid states: "
                  f"{', '.join(s.value for s in JobState)}", file=sys.stderr)
            return 1

    jobs = scheduler.list_jobs(
        state=state_filter, limit=args.limit, offset=args.offset)

    if args.json:
        job_dicts = []
        for job in jobs:
            job_dicts.append({
                "id": str(job.id),
                "name": job.name,
                "state": job.state.value,
                "priority": job.priority.name,
                "attempt": job.attempt,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            })
        print(_format_json(job_dicts))
    else:
        headers = ["ID", "Name", "State", "Priority", "Attempt", "Created"]
        rows = []
        for job in jobs:
            created = job.created_at.strftime(
                "%Y-%m-%d %H:%M:%S") if job.created_at else "-"
            rows.append([
                str(job.id)[:8],
                job.name[:30],
                job.state.value,
                job.priority.name,
                str(job.attempt),
                created,
            ])
        print(_format_table(headers, rows))

    return 0


def cmd_inspect_job(args: argparse.Namespace) -> int:
    """Show detailed information about a specific job."""
    scheduler = _get_scheduler(args.config)
    job = scheduler.get_job(args.job_id)

    if not job:
        print(f"Error: Job '{args.job_id}' not found.", file=sys.stderr)
        return 1

    data = {
        "id": str(job.id),
        "name": job.name,
        "state": job.state.value,
        "priority": job.priority.name,
        "attempt": job.attempt,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if hasattr(job, 'started_at') and job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "result": str(job.result) if job.result else None,
        "error": job.error if hasattr(job, 'error') else None,
        "traceback": job.traceback if hasattr(job, 'traceback') else None,
    }

    if args.json:
        print(_format_json(data))
    else:
        print(f"=== Job: {data['name']} ===")
        for key, value in data.items():
            if value is not None:
                print(f"  {key:15s}: {value}")

    return 0


def cmd_cancel_job(args: argparse.Namespace) -> int:
    """Cancel a pending or scheduled job."""
    scheduler = _get_scheduler(args.config)

    try:
        result = scheduler.cancel_job(args.job_id)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if result:
        print(f"Job '{args.job_id}' cancelled.")
    else:
        print(f"Could not cancel job '{args.job_id}' (may already be running or completed).",
              file=sys.stderr)
        return 1

    return 0


def cmd_dlq_list(args: argparse.Namespace) -> int:
    """List entries in the dead letter queue."""
    from ..scheduler.dlq import DLQEntryStatus

    scheduler = _get_scheduler(args.config)

    status_filter = None
    if args.status:
        try:
            status_filter = DLQEntryStatus(args.status)
        except ValueError:
            print(f"Error: Invalid status '{args.status}'. Valid statuses: "
                  f"{', '.join(s.value for s in DLQEntryStatus)}", file=sys.stderr)
            return 1

    entries = scheduler.get_dlq_entries(status=status_filter, limit=args.limit)

    # 🧠 FIX: Completed missing code paths for DLQ inspections
    if args.json:
        entry_dicts = []
        for entry in entries:
            entry_dicts.append({
                "job_id": str(entry.job_id),
                "reason": entry.reason,
                "status": entry.status.value,
                "failed_at": entry.failed_at.isoformat() if entry.failed_at else None
            })
        print(_format_json(entry_dicts))
    else:
        headers = ["Job ID", "Status", "Reason", "Failed At"]
        rows = []
        for entry in entries:
            failed_at = entry.failed_at.strftime(
                "%Y-%m-%d %H:%M:%S") if entry.failed_at else "-"
            rows.append([
                str(entry.job_id)[:8],
                entry.status.value,
                entry.reason[:40],
                failed_at
            ])
        print(_format_table(headers, rows))

    return 0


def cli():
    """Main CLI Parser Router."""
    parser = argparse.ArgumentParser(
        description="Job Orchestrator Command Line Tool")
    parser.add_促销_argument(
        "-c", "--config", help="Path to config file (yaml/toml)")
    parser.add_argument("--json", action="store_true",
                        help="Output results in JSON format")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Status Command
    subparsers.add_parser("status", help="Get engine runtime statistics")

    # List Jobs Command
    list_p = subparsers.add_parser("list", help="List submitted jobs")
    list_p.add_argument("--state", help="Filter by state")
    list_p.add_argument("--limit", type=int, default=50,
                        help="Pagination result limit")
    list_p.add_argument("--offset", type=int, default=0,
                        help="Pagination offset")

    # Inspect Job Command
    inspect_p = subparsers.add_parser(
        "inspect", help="View full details of a specific job")
    inspect_p.add_argument(
        "job_id", help="The UUID string of the targeted job")

    # Cancel Job Command
    cancel_p = subparsers.add_parser(
        "cancel", help="Cancel processing a job queue item")
    cancel_p.add_argument("job_id", help="The UUID string of the targeted job")

    # DLQ Subparser
    dlq_p = subparsers.add_parser(
        "dlq", help="Explore Dead Letter Queue drops")
    dlq_p.add_argument("--status", help="Filter items by resolution state")
    dlq_p.add_argument("--limit", type=int, default=20,
                       help="Record cap limit")

    args = parser.parse_args()

    # Routing map
    commands = {
        "status": cmd_status,
        "list": cmd_list_jobs,
        "inspect": cmd_inspect_job,
        "cancel": cmd_cancel_job,
        "dlq": cmd_dlq_list,
    }

    try:
        sys.exit(commands[args.command](args))
    except Exception as e:
        print(f"Execution Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    cli()
