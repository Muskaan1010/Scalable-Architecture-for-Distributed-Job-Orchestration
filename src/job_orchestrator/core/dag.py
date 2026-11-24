"""
DAG (Directed Acyclic Graph) implementation for the Job Orchestrator.

This module provides data structures for representing job dependencies
as a directed acyclic graph, along with a builder API for easy DAG construction.
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Union
from uuid import UUID, uuid4

from .job import Job, JobState
from .exceptions import CyclicDependencyError, DAGValidationError


@dataclass
class DAGNode:
    """
    A node in the DAG representing a job with its dependencies.
    """
    job_id: UUID
    dependencies: Set[UUID] = field(default_factory=set)
    dependents: Set[UUID] = field(default_factory=set)

    def add_dependency(self, job_id: UUID) -> None:
        """Add an upstream dependency."""
        self.dependencies.add(job_id)

    def add_dependent(self, job_id: UUID) -> None:
        """Add a downstream dependent."""
        self.dependents.add(job_id)

    def remove_dependency(self, job_id: UUID) -> None:
        """Remove an upstream dependency."""
        self.dependencies.discard(job_id)

    def remove_dependent(self, job_id: UUID) -> None:
        """Remove a downstream dependent."""
        self.dependents.discard(job_id)

    @property
    def in_degree(self) -> int:
        """Return the number of dependencies (in-degree)."""
        return len(self.dependencies)

    @property
    def out_degree(self) -> int:
        """Return the number of dependents (out-degree)."""
        return len(self.dependents)

    @property
    def is_root(self) -> bool:
        """Check if this is a root node (no dependencies)."""
        return len(self.dependencies) == 0

    @property
    def is_leaf(self) -> bool:
        """Check if this is a leaf node (no dependents)."""
        return len(self.dependents) == 0


@dataclass
class DAG:
    """
    Directed Acyclic Graph for job dependencies.
    """
    # Identity
    id: UUID = field(default_factory=uuid4)
    name: str = ""
    description: str = ""

    # Graph structure
    nodes: Dict[UUID, DAGNode] = field(default_factory=dict)
    jobs: Dict[UUID, Job] = field(default_factory=dict)

    # State
    state: JobState = JobState.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Execution options
    fail_fast: bool = True
    max_parallel: Optional[int] = None

    # Transaction support
    enable_rollback: bool = False
    rollback_handler: Optional[Callable] = field(default=None, repr=False)

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_node(self, job: Job) -> None:
        """Add a job to the DAG without any dependencies."""
        if job.id not in self.nodes:
            self.nodes[job.id] = DAGNode(job_id=job.id)
            self.jobs[job.id] = job

    def add_edge(self, from_job_id: UUID, to_job_id: UUID) -> None:
        """Add a dependency edge: to_job depends on from_job."""
        if from_job_id not in self.nodes:
            raise KeyError(f"Job {from_job_id} not found in DAG")
        if to_job_id not in self.nodes:
            raise KeyError(f"Job {to_job_id} not found in DAG")

        self.nodes[to_job_id].add_dependency(from_job_id)
        self.nodes[from_job_id].add_dependent(to_job_id)

        if from_job_id not in self.jobs[to_job_id].depends_on:
            self.jobs[to_job_id].depends_on.append(from_job_id)

    def remove_edge(self, from_job_id: UUID, to_job_id: UUID) -> None:
        """Remove a dependency edge."""
        if from_job_id in self.nodes and to_job_id in self.nodes:
            self.nodes[to_job_id].remove_dependency(from_job_id)
            self.nodes[from_job_id].remove_dependent(to_job_id)

            if from_job_id in self.jobs[to_job_id].depends_on:
                self.jobs[to_job_id].depends_on.remove(from_job_id)

    def add_job(self, job: Job, depends_on: Optional[List[UUID]] = None) -> None:
        """Add a job to the DAG with optional dependencies."""
        self.add_node(job)
        if depends_on:
            for dep_id in depends_on:
                if dep_id in self.nodes:
                    self.add_edge(dep_id, job.id)

    def get_root_nodes(self) -> List[UUID]:
        """Get all root nodes."""
        return [node.job_id for node in self.nodes.values() if node.is_root]

    def get_leaf_nodes(self) -> List[UUID]:
        """Get all leaf nodes."""
        return [node.job_id for node in self.nodes.values() if node.is_leaf]

    def get_ready_jobs(self) -> List[Job]:
        """Get all jobs ready for execution."""
        ready = []
        for job_id, node in self.nodes.items():
            job = self.jobs[job_id]
            if job.state != JobState.PENDING:
                continue
            all_deps_completed = all(
                self.jobs[dep_id].state == JobState.COMPLETED
                for dep_id in node.dependencies
            )
            if all_deps_completed:
                ready.append(job)
        return ready

    def is_job_ready(self, job_id: UUID) -> bool:
        """Check if a specific job is ready for execution."""
        if job_id not in self.nodes:
            return False
        job = self.jobs[job_id]
        if job.state != JobState.PENDING:
            return False
        node = self.nodes[job_id]
        return all(
            self.jobs[dep_id].state == JobState.COMPLETED
            for dep_id in node.dependencies
        )

    def topological_sort(self) -> List[Job]:
        """Topological order via Kahn's algorithm."""
        in_degree: Dict[UUID, int] = {
            job_id: node.in_degree for job_id, node in self.nodes.items()}
        queue = deque(job_id for job_id,
                      degree in in_degree.items() if degree == 0)
        result: List[Job] = []

        while queue:
            job_id = queue.popleft()
            result.append(self.jobs[job_id])
            for dependent_id in self.nodes[job_id].dependents:
                in_degree[dependent_id] -= 1
                if in_degree[dependent_id] == 0:
                    queue.append(dependent_id)

        if len(result) != len(self.nodes):
            remaining = [str(job_id)
                         for job_id, degree in in_degree.items() if degree > 0]
            raise CyclicDependencyError(
                message="Cycle detected during topological sort",
                dag_id=self.id,
                cycle_path=remaining
            )
        return result

    def has_cycle(self) -> bool:
        """Check if the DAG contains a cycle."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[UUID, int] = {job_id: WHITE for job_id in self.nodes}

        def dfs(job_id: UUID) -> bool:
            color[job_id] = GRAY
            for dependent_id in self.nodes[job_id].dependents:
                if color[dependent_id] == GRAY:
                    return True
                if color[dependent_id] == WHITE:
                    if dfs(dependent_id):
                        return True
            color[job_id] = BLACK
            return False

        for job_id in self.nodes:
            if color[job_id] == WHITE:
                if dfs(job_id):
                    return True
        return False

    def find_cycle(self) -> Optional[List[UUID]]:
        """Find a cycle in the DAG if one exists."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[UUID, int] = {job_id: WHITE for job_id in self.nodes}
        parent: Dict[UUID, Optional[UUID]] = {
            job_id: None for job_id in self.nodes}

        def dfs(job_id: UUID) -> Optional[UUID]:
            color[job_id] = GRAY
            for dependent_id in self.nodes[job_id].dependents:
                if color[dependent_id] == GRAY:
                    return dependent_id
                if color[dependent_id] == WHITE:
                    parent[dependent_id] = job_id
                    result = dfs(dependent_id)
                    if result is not None:
                        return result
            color[job_id] = BLACK
            return None

        for job_id in self.nodes:
            if color[job_id] == WHITE:
                cycle_node = dfs(job_id)
                if cycle_node is not None:
                    cycle = [cycle_node]
                    current = parent[cycle_node]
                    while current != cycle_node and current is not None:
                        cycle.append(current)
                        current = parent[current]
                    cycle.append(cycle_node)
                    return list(reversed(cycle))
        return None

    def validate(self) -> bool:
        """Validate DAG edges and cycle checks."""
        errors: List[str] = []
        for job_id, node in self.nodes.items():
            for dep_id in node.dependencies:
                if dep_id not in self.nodes:
                    errors.append(
                        f"Job {job_id} has missing dependency: {dep_id}")
        if self.has_cycle():
            cycle = self.find_cycle()
            cycle_str = " -> ".join(str(j) for j in (cycle or []))
            errors.append(f"Cycle detected: {cycle_str}")
        if errors:
            raise DAGValidationError(
                message="DAG validation failed", dag_id=self.id, errors=errors)
        return True

    def get_execution_plan(self) -> List[List[Job]]:
        """Group levels of parallelizable jobs."""
        if not self.nodes:
            return []
        levels: List[List[Job]] = []
        remaining = set(self.nodes.keys())
        in_degree = {
            job_id: self.nodes[job_id].in_degree for job_id in self.nodes}

        while remaining:
            current_level = []
            for job_id in list(remaining):
                if in_degree[job_id] == 0:
                    current_level.append(self.jobs[job_id])
                    remaining.remove(job_id)
            if not current_level:
                raise CyclicDependencyError(
                    message="Cycle detected in plan creation", dag_id=self.id)
            levels.append(current_level)
            for job in current_level:
                for dependent_id in self.nodes[job.id].dependents:
                    if dependent_id in remaining:
                        in_degree[dependent_id] -= 1
        return levels

    def __len__(self) -> int:
        return len(self.nodes)

    def __contains__(self, job_id: UUID) -> bool:
        return job_id in self.nodes

    def __iter__(self) -> Iterator[Job]:
        return iter(self.topological_sort())

    @property
    def is_complete(self) -> bool:
        return all(job.state == JobState.COMPLETED for job in self.jobs.values())

    @property
    def has_failed(self) -> bool:
        return any(job.state == JobState.FAILED for job in self.jobs.values())

    @property
    def progress(self) -> float:
        if not self.jobs:
            return 1.0
        completed = sum(1 for job in self.jobs.values()
                        if job.state == JobState.COMPLETED)
        return completed / len(self.jobs)


class DAGBuilder:
    """Fluent API for building DAGs."""

    def __init__(self, name: str, description: str = ""):
        self.dag = DAG(name=name, description=description)
        self._job_ids: Dict[str, UUID] = {}

    def add_job(self, job_or_func: Union[Job, Callable], job_id: Optional[str] = None, depends_on: Optional[List[str]] = None, **job_kwargs) -> "DAGBuilder":
        if isinstance(job_or_func, Job):
            job = job_or_func
        elif callable(job_or_func):
            from .job import JobPriority
            func_name = getattr(job_or_func, "__name__", "anonymous")
            func_path = f"{job_or_func.__module__}.{job_or_func.__qualname__}"
            job = Job(
                name=job_kwargs.get("name", func_name),
                func=job_or_func,
                func_path=func_path,
                args=job_kwargs.get("args", ()),
                kwargs=job_kwargs.get("kwargs", {}),
                priority=job_kwargs.get("priority", JobPriority.NORMAL),
                timeout=job_kwargs.get("timeout"),
                tags=job_kwargs.get("tags", {}),
            )
        else:
            raise TypeError(
                f"Expected Job or callable, got {type(job_or_func)}")

        key = job_id or str(job.id)
        self._job_ids[key] = job.id
        self.dag.add_node(job)

        if depends_on:
            for dep_key in depends_on:
                if dep_key not in self._job_ids:
                    raise ValueError(f"Dependency '{dep_key}' not found.")
                self.dag.add_edge(self._job_ids[dep_key], job.id)
        return self

    def with_fail_fast(self, enabled: bool = True) -> "DAGBuilder":
        self.dag.fail_fast = enabled
        return self

    def with_max_parallel(self, limit: int) -> "DAGBuilder":
        self.dag.max_parallel = limit
        return self

    def with_rollback(self, handler: Optional[Callable] = None) -> "DAGBuilder":
        self.dag.enable_rollback = True
        if handler:
            self.dag.rollback_handler = handler
        return self

    def with_metadata(self, **metadata) -> "DAGBuilder":
        self.dag.metadata.update(metadata)
        return self

    def build(self) -> DAG:
        self.dag.validate()
        return self.dag

    def build_unchecked(self) -> DAG:
        return self.dag
