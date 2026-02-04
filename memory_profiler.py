"""
Memory profiling utilities for the NK ČR pipeline.

Provides functions to measure and log memory usage at key points
in the pipeline execution.
"""

import logging
import resource
import tracemalloc
from typing import Optional

log = logging.getLogger(__name__)

_tracemalloc_started = False


def get_memory_usage_mb() -> float:
    """
    Get current memory usage of the process in MB.

    Uses resource.getrusage() which returns maxrss in bytes on macOS
    and in KB on Linux.
    """
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    # maxrss is in bytes on macOS, KB on Linux
    import sys
    if sys.platform == 'darwin':
        return rusage.ru_maxrss / (1024 * 1024)  # bytes to MB
    else:
        return rusage.ru_maxrss / 1024  # KB to MB


def start_tracemalloc():
    """Start tracemalloc for detailed memory tracking."""
    global _tracemalloc_started
    if not _tracemalloc_started:
        tracemalloc.start()
        _tracemalloc_started = True


def get_tracemalloc_usage_mb() -> tuple[float, float]:
    """
    Get current and peak memory tracked by tracemalloc in MB.

    Returns:
        Tuple of (current_mb, peak_mb)
    """
    if not _tracemalloc_started:
        return (0.0, 0.0)
    current, peak = tracemalloc.get_traced_memory()
    return (current / (1024 * 1024), peak / (1024 * 1024))


def log_memory(label: str = "", include_tracemalloc: bool = True):
    """
    Log current memory usage with an optional label.

    Args:
        label: Description of the current point in execution
        include_tracemalloc: Whether to include tracemalloc stats
    """
    process_mb = get_memory_usage_mb()

    if include_tracemalloc and _tracemalloc_started:
        current_mb, peak_mb = get_tracemalloc_usage_mb()
        log.info(f"[MEMORY] {label}: Process={process_mb:.1f}MB, "
                 f"Python alloc={current_mb:.1f}MB (peak={peak_mb:.1f}MB)")
    else:
        log.info(f"[MEMORY] {label}: Process={process_mb:.1f}MB")


def log_memory_snapshot(label: str = "", top_n: int = 10):
    """
    Log a detailed memory snapshot showing top allocations.

    Args:
        label: Description of the current point in execution
        top_n: Number of top memory consumers to show
    """
    if not _tracemalloc_started:
        log.warning("[MEMORY] tracemalloc not started, call start_tracemalloc() first")
        return

    snapshot = tracemalloc.take_snapshot()
    stats = snapshot.statistics('lineno')

    log.info(f"[MEMORY SNAPSHOT] {label} - Top {top_n} allocations:")
    for i, stat in enumerate(stats[:top_n], 1):
        log.info(f"  #{i}: {stat.size / (1024*1024):.1f}MB - {stat.traceback}")


def get_object_size_mb(obj, name: str = "object") -> float:
    """
    Estimate size of an object in MB.

    For dicts, includes size of keys and values.
    """
    import sys

    size = sys.getsizeof(obj)

    if isinstance(obj, dict):
        size += sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in obj.items())
        # For nested dicts, add one more level
        for v in obj.values():
            if isinstance(v, dict):
                size += sum(sys.getsizeof(k2) + sys.getsizeof(v2) for k2, v2 in v.items())
            elif isinstance(v, (list, tuple)):
                size += sum(sys.getsizeof(item) for item in v)
    elif isinstance(obj, (list, tuple)):
        size += sum(sys.getsizeof(item) for item in obj)

    size_mb = size / (1024 * 1024)
    log.info(f"[MEMORY] {name} size: {size_mb:.2f}MB ({len(obj) if hasattr(obj, '__len__') else '?'} items)")
    return size_mb


class MemoryTracker:
    """
    Context manager for tracking memory usage of a code block.

    Usage:
        with MemoryTracker("Loading occupations"):
            data = load_occupations()
    """

    def __init__(self, label: str):
        self.label = label
        self.start_mem = 0.0

    def __enter__(self):
        if _tracemalloc_started:
            self.start_mem = get_tracemalloc_usage_mb()[0]
        log.info(f"[MEMORY] Starting: {self.label}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if _tracemalloc_started:
            current_mem = get_tracemalloc_usage_mb()[0]
            delta = current_mem - self.start_mem
            log.info(f"[MEMORY] Finished: {self.label} (+{delta:.1f}MB, total={current_mem:.1f}MB)")
        else:
            log_memory(f"Finished: {self.label}", include_tracemalloc=False)
        return False
