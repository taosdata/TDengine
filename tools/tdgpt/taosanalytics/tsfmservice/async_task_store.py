"""SQLite-based persistent store for async task operations.

Enables sharing task state across multiple Flask worker processes (gunicorn).
Supports automatic cleanup of old completed tasks.
"""

import json
import sqlite3
import time
from pathlib import Path
from threading import Lock
from typing import Dict, Optional

from taosanalytics.log import AppLogger


class AsyncTaskStore:
    """Thread-safe SQLite store for async task operations."""

    def __init__(self, db_path: str = None):
        """Initialize the task store.

        Args:
            db_path: Path to SQLite database file. Defaults to resource directory.
        """
        if db_path is None:
            # Default to resource directory (same level as scripts)
            resource_dir = Path(__file__).resolve().parent.parent.parent / "resource"
            resource_dir.mkdir(parents=True, exist_ok=True)
            db_path = resource_dir / "async_tasks.db"

        self.db_path = db_path
        self._lock = Lock()
        self._init_db()

    def _init_db(self):
        """Initialize database schema if not exists."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS async_tasks (
                    task_id TEXT PRIMARY KEY,
                    model_name TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress INTEGER DEFAULT 0,
                    created_at REAL NOT NULL,
                    started_at REAL,
                    completed_at REAL,
                    error TEXT,
                    metadata TEXT,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_status ON async_tasks(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_model_name ON async_tasks(model_name)
            """)
            conn.commit()
            AppLogger.info("AsyncTaskStore initialized: %s", self.db_path)

    def create_task(self, task_id: str, model_name: str, operation: str, metadata: Dict = None) -> bool:
        """Create a new async task record.

        Args:
            task_id: Unique task identifier
            model_name: Name of the model being operated on
            operation: Operation type (download, start, stop, delete)
            metadata: Optional metadata dict (will be JSON-serialized)

        Returns:
            True if created successfully, False if task already exists
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
                        INSERT INTO async_tasks
                        (task_id, model_name, operation, status, created_at, metadata)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        task_id,
                        model_name,
                        operation,
                        "queued",
                        time.time(),
                        json.dumps(metadata or {})
                    ))
                    conn.commit()
                    AppLogger.info("Task created: %s (%s on %s)", task_id, operation, model_name)
                    return True
            except sqlite3.IntegrityError:
                AppLogger.warning("Task already exists: %s", task_id)
                return False

    def update_task_status(self, task_id: str, status: str, progress: int = None, error: str = None) -> bool:
        """Update task status and optional progress/error.

        Args:
            task_id: Task identifier
            status: New status (queued, running, completed, failed)
            progress: Optional progress percentage (0-100)
            error: Optional error message (for failed status)

        Returns:
            True if updated successfully
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    updates = {"status": status}
                    if progress is not None:
                        updates["progress"] = progress
                    if error is not None:
                        updates["error"] = error

                    # Automatically set started_at when transitioning to running
                    if status == "running":
                        updates["started_at"] = time.time()

                    # Automatically set completed_at when transitioning to completed/failed
                    if status in ("completed", "failed"):
                        updates["completed_at"] = time.time()

                    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
                    values = list(updates.values()) + [task_id]

                    cursor = conn.execute(f"""
                        UPDATE async_tasks
                        SET {set_clause}
                        WHERE task_id = ?
                    """, values)

                    conn.commit()
                    return cursor.rowcount > 0
            except Exception as e:
                AppLogger.error("Error updating task %s: %s", task_id, e)
                return False

    def get_task(self, task_id: str) -> Optional[Dict]:
        """Get task information by ID.

        Args:
            task_id: Task identifier

        Returns:
            Task dict or None if not found
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute(
                        "SELECT * FROM async_tasks WHERE task_id = ?",
                        (task_id,)
                    )
                    row = cursor.fetchone()
                    if row:
                        return dict(row)
            except Exception as e:
                AppLogger.error("Error getting task %s: %s", task_id, e)
            return None

    def cleanup_old_tasks(self, days: int = 7) -> int:
        """Delete completed tasks older than N days.

        Args:
            days: Number of days to retain (default 7)

        Returns:
            Number of rows deleted
        """
        with self._lock:
            try:
                cutoff_time = time.time() - (days * 86400)
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute("""
                        DELETE FROM async_tasks
                        WHERE status IN ('completed', 'failed')
                        AND completed_at < ?
                    """, (cutoff_time,))
                    conn.commit()
                    deleted = cursor.rowcount
                    if deleted > 0:
                        AppLogger.info("Cleaned up %d old tasks", deleted)
                    return deleted
            except Exception as e:
                AppLogger.error("Error cleaning up tasks: %s", e)
                return 0


# Global instance
_task_store = None


def get_task_store() -> AsyncTaskStore:
    """Get or create the global task store instance."""
    global _task_store
    if _task_store is None:
        _task_store = AsyncTaskStore()
    return _task_store
