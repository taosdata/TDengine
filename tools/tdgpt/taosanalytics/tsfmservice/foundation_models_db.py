"""Unified SQLite database for foundation models with MVC architecture.

Consolidates model metadata, state, and async operations into a single database
with clear separation between Model layer (data objects) and DAO layer (data access).

Architecture:
- Model Layer: FoundationModel and AsyncOperation dataclasses
- DAO Layer: FoundationModelsDAO for all database operations
- Thread-safe operations with singleton pattern
"""

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger

# ============================================================================
# MODEL LAYER: Data Objects (what data we store)
# ============================================================================

@dataclass
class FoundationModel:
    """Foundation model metadata and state."""

    name: str  # PRIMARY KEY
    model_id: str
    package: str = ""
    type: str = "forecast"
    description: str = ""
    size_gb: float = 0.0
    status: str = "ready"
    downloaded: bool = False
    running: bool = False
    discovered_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class AsyncOperation:
    """Async task tracking data."""

    task_id: str  # PRIMARY KEY
    operation: str  # enum value from OperationType
    model_name: str
    status: str  # enum value from TaskStatus
    progress: int = 0
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None
    error_code: str = ""
    data: Optional[dict] = None  # JSON serializable
    updated_at: float = field(default_factory=time.time)


# ============================================================================
# DAO LAYER: Database Access Object (how we access data)
# ============================================================================

class FoundationModelsDAO:
    """Data access object for foundation models and async operations.

    Provides thread-safe access to SQLite database with two tables:
    - models: Foundation model metadata and state
    - async_operations: Async task tracking
    """

    def __init__(self, db_path: Optional[str] = None):
        """Initialize DAO with database connection.

        Args:
            db_path: Path to SQLite database file. If None, uses config resource directory.
        """
        if db_path is None:
            config = Configure.get_instance()
            resource_dir = config._conf.get("resource_dir")
            if not resource_dir:
                resource_dir = config.get_model_directory()

            db_path = str(Path(resource_dir) / "foundation_models.db")

        self.db_path = db_path
        self._lock = threading.RLock()

        # Ensure parent directory exists before initializing database
        db_parent = Path(self.db_path).parent
        db_parent.mkdir(parents=True, exist_ok=True)

        self._init_db()
        AppLogger.info("FoundationModelsDAO initialized at: %s", self.db_path)

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection with row factory."""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialize database schema if it doesn't exist."""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Create models table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS models (
                        name TEXT PRIMARY KEY,
                        model_id TEXT NOT NULL,
                        package TEXT,
                        type TEXT,
                        description TEXT,
                        size_gb REAL DEFAULT 0,
                        status TEXT DEFAULT 'ready',
                        downloaded INTEGER DEFAULT 0,
                        running INTEGER DEFAULT 0,
                        discovered_at REAL NOT NULL,
                        updated_at REAL NOT NULL
                    )
                """)

                # Create async_operations table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS async_operations (
                        task_id TEXT PRIMARY KEY,
                        operation TEXT NOT NULL,
                        model_name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        progress INTEGER DEFAULT 0,
                        created_at REAL NOT NULL,
                        started_at REAL,
                        completed_at REAL,
                        error TEXT,
                        error_code TEXT,
                        data TEXT,
                        updated_at REAL NOT NULL
                    )
                """)

                conn.commit()
                AppLogger.debug("Database schema initialized")
            except sqlite3.Error as e:
                AppLogger.error("Failed to initialize database schema: %s", e)
                raise
            finally:
                conn.close()

    # ========================================================================
    # Model CRUD Operations
    # ========================================================================

    def create_model(self, model: FoundationModel) -> bool:
        """Create a new model record.

        Args:
            model: FoundationModel instance

        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT OR REPLACE INTO models
                    (name, model_id, package, type, description, size_gb, status,
                     downloaded, running, discovered_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    model.name, model.model_id, model.package, model.type,
                    model.description, model.size_gb, model.status,
                    1 if model.downloaded else 0, 1 if model.running else 0,
                    model.discovered_at, model.updated_at
                ))
                conn.commit()
                AppLogger.debug("Created model: %s", model.name)
                return True
            except sqlite3.Error as e:
                AppLogger.error("Failed to create model: %s", e)
                return False
            finally:
                conn.close()

    def get_model(self, name: str) -> Optional[FoundationModel]:
        """Retrieve a model by name.

        Args:
            name: Model name

        Returns:
            FoundationModel instance or None if not found
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute("SELECT * FROM models WHERE name = ?", (name,))
                row = cursor.fetchone()
                if not row:
                    return None

                return FoundationModel(
                    name=row["name"],
                    model_id=row["model_id"],
                    package=row["package"],
                    type=row["type"],
                    description=row["description"],
                    size_gb=row["size_gb"],
                    status=row["status"],
                    downloaded=bool(row["downloaded"]),
                    running=bool(row["running"]),
                    discovered_at=row["discovered_at"],
                    updated_at=row["updated_at"]
                )
            except sqlite3.Error as e:
                AppLogger.error("Failed to retrieve model: %s", e)
                return None
            finally:
                conn.close()

    def update_model(self, name: str, **kwargs) -> bool:
        """Update a model record.

        Args:
            name: Model name
            **kwargs: Fields to update (downloaded, running, status, etc.)

        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                # Build update query
                allowed_fields = {"downloaded", "running", "status", "size_gb", "description"}
                update_fields = {
                    k: v
                    for k, v in kwargs.items()
                    if k in allowed_fields and v is not None
                }

                if not update_fields:
                    return False

                # Convert boolean fields
                if "downloaded" in update_fields:
                    update_fields["downloaded"] = 1 if update_fields["downloaded"] else 0
                if "running" in update_fields:
                    update_fields["running"] = 1 if update_fields["running"] else 0

                update_fields["updated_at"] = time.time()

                set_clause = ", ".join([f"{k} = ?" for k in update_fields.keys()])
                values = list(update_fields.values()) + [name]

                cursor.execute(f"UPDATE models SET {set_clause} WHERE name = ?", values)
                conn.commit()
                AppLogger.debug("Updated model: %s", name)
                return True
            except sqlite3.Error as e:
                AppLogger.error("Failed to update model: %s", e)
                return False
            finally:
                conn.close()

    def list_models(self) -> list[FoundationModel]:
        """List all models.

        Returns:
            List of FoundationModel instances
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute("SELECT * FROM models ORDER BY name")
                rows = cursor.fetchall()
                results = []
                for row in rows:
                    results.append(FoundationModel(
                        name=row["name"],
                        model_id=row["model_id"],
                        package=row["package"],
                        type=row["type"],
                        description=row["description"],
                        size_gb=row["size_gb"],
                        status=row["status"],
                        downloaded=bool(row["downloaded"]),
                        running=bool(row["running"]),
                        discovered_at=row["discovered_at"],
                        updated_at=row["updated_at"]
                    ))

                return results
            except sqlite3.Error as e:
                AppLogger.error("Failed to list models: %s", e)
                return []
            finally:
                conn.close()

    def delete_model(self, name: str) -> bool:
        """Delete a model record.

        Args:
            name: Model name

        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute("DELETE FROM models WHERE name = ?", (name,))
                conn.commit()
                AppLogger.debug("Deleted model: %s", name)
                return True
            except sqlite3.Error as e:
                AppLogger.error("Failed to delete model: %s", e)
                return False
            finally:
                conn.close()

    # ========================================================================
    # AsyncOperation CRUD Operations
    # ========================================================================

    def create_async_operation(self, operation: AsyncOperation) -> bool:
        """Create a new async operation record.

        Args:
            operation: AsyncOperation instance

        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                data_json = json.dumps(operation.data) if operation.data else None

                cursor.execute("""
                    INSERT INTO async_operations
                    (task_id, operation, model_name, status, progress, created_at,
                     started_at, completed_at, error, error_code, data, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    operation.task_id, operation.operation, operation.model_name,
                    operation.status, operation.progress, operation.created_at,
                    operation.started_at, operation.completed_at,
                    operation.error, operation.error_code, data_json,
                    operation.updated_at
                ))
                conn.commit()
                AppLogger.debug("Created async operation: %s", operation.task_id)
                return True
            except sqlite3.Error as e:
                AppLogger.error("Failed to create async operation: %s", e)
                return False
            finally:
                conn.close()

    def create_download_operation_if_idle(self, operation: AsyncOperation) -> bool:
        """Create a download operation only if the resource has none in flight.

        The request handlers used to read the active operation and then insert
        theirs -- two statements on two connections, so two gunicorn worker
        processes could both find nothing and both queue a download of the same
        resource (the client then saw two task_ids for one resource). Here the
        check and the insert are a single statement, which SQLite executes
        atomically.

        Args:
            operation: Download-family operation (``download``/``download_start``)

        Returns:
            True when this caller created the row; False when the resource
            already had a queued/running download.
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                data_json = json.dumps(operation.data) if operation.data else None

                cursor.execute("""
                    INSERT INTO async_operations
                    (task_id, operation, model_name, status, progress, created_at,
                     started_at, completed_at, error, error_code, data, updated_at)
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM async_operations
                        WHERE model_name = ?
                          AND operation IN ('download', 'download_start')
                          AND status IN ('queued', 'running')
                    )
                """, (
                    operation.task_id, operation.operation, operation.model_name,
                    operation.status, operation.progress, operation.created_at,
                    operation.started_at, operation.completed_at,
                    operation.error, operation.error_code, data_json,
                    operation.updated_at,
                    operation.model_name,
                ))
                conn.commit()
                created = cursor.rowcount == 1
                if created:
                    AppLogger.debug("Created async operation: %s", operation.task_id)
                return created
            except sqlite3.Error as e:
                AppLogger.error("Failed to create async operation: %s", e)
                return False
            finally:
                conn.close()

    def get_async_operation(self, task_id: str) -> Optional[AsyncOperation]:
        """Retrieve an async operation by task_id.

        Args:
            task_id: Unique task identifier

        Returns:
            AsyncOperation instance or None if not found
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute("SELECT * FROM async_operations WHERE task_id = ?", (task_id,))
                row = cursor.fetchone()
                if not row:
                    return None

                data = json.loads(row["data"]) if row["data"] else None

                return AsyncOperation(
                    task_id=row["task_id"],
                    operation=row["operation"],
                    model_name=row["model_name"],
                    status=row["status"],
                    progress=row["progress"],
                    created_at=row["created_at"],
                    started_at=row["started_at"],
                    completed_at=row["completed_at"],
                    error=row["error"],
                    error_code=row["error_code"],
                    data=data,
                    updated_at=row["updated_at"]
                )
            except sqlite3.Error as e:
                AppLogger.error("Failed to retrieve async operation: %s", e)
                return None
            finally:
                conn.close()

    def update_async_operation(self, task_id: str, **kwargs) -> bool:
        """Update an async operation record.

        Args:
            task_id: Unique task identifier
            **kwargs: Fields to update (status, progress, error, etc.)

        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                # Build update query
                allowed_fields = {"status", "progress", "started_at", "completed_at", "error", "error_code", "data"}
                update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}

                if not update_fields:
                    return False

                # Serialize data if present
                if "data" in update_fields and update_fields["data"] is not None:
                    update_fields["data"] = json.dumps(update_fields["data"])

                update_fields["updated_at"] = time.time()

                set_clause = ", ".join([f"{k} = ?" for k in update_fields.keys()])
                values = list(update_fields.values()) + [task_id]

                cursor.execute(f"UPDATE async_operations SET {set_clause} WHERE task_id = ?", values)
                conn.commit()
                AppLogger.debug("Updated async operation: %s", task_id)
                return True
            except sqlite3.Error as e:
                AppLogger.error("Failed to update async operation: %s", e)
                return False
            finally:
                conn.close()

    def mark_operation_running(self, task_id: str) -> bool:
        """Move an operation to ``running``, keeping the time it first started.

        ``started_at`` doubles as the watermark ``_find_resource_state`` compares
        a resource task file against, so it has to mean "when this operation
        began" and must never be pushed forward. A combined operation runs two
        workers on one row (download, then start) and the second one used to
        reset it -- moving the watermark past the task file the first phase had
        written. That phase then looked like it had reported nothing: the SSE
        stream fell back to its stand-in row and told the client a phase it had
        already seen finish was starting over, and then reported that phase
        finished a second time.

        COALESCE keeps the write first-write-wins without a read-modify-write, so
        two workers racing on one row cannot clobber each other's timestamp.

        Args:
            task_id: Unique task identifier

        Returns:
            True when the row was found and updated.
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                now = time.time()
                cursor.execute("""
                    UPDATE async_operations
                    SET status = 'running',
                        started_at = COALESCE(started_at, ?),
                        updated_at = ?
                    WHERE task_id = ?
                """, (now, now, task_id))
                conn.commit()
                AppLogger.debug("Marked async operation running: %s", task_id)
                return cursor.rowcount == 1
            except sqlite3.Error as e:
                AppLogger.error("Failed to mark async operation running: %s", e)
                return False
            finally:
                conn.close()

    def list_async_operations(self, model_name: Optional[str] = None, active_only: bool = False) -> list[AsyncOperation]:
        """List async operations, optionally filtered by model or status.

        Args:
            model_name: Optional model name to filter
            active_only: If True, only return queued/running tasks (exclude completed/failed)

        Returns:
            List of AsyncOperation instances
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                if model_name and active_only:
                    cursor.execute("""
                        SELECT * FROM async_operations
                        WHERE model_name = ? AND status NOT IN ('completed', 'failed')
                        ORDER BY created_at DESC
                    """, (model_name,))
                elif model_name:
                    cursor.execute("""
                        SELECT * FROM async_operations
                        WHERE model_name = ?
                        ORDER BY created_at DESC
                    """, (model_name,))
                elif active_only:
                    cursor.execute("""
                        SELECT * FROM async_operations
                        WHERE status NOT IN ('completed', 'failed')
                        ORDER BY created_at DESC
                    """)
                else:
                    cursor.execute("SELECT * FROM async_operations ORDER BY created_at DESC")

                rows = cursor.fetchall()
                results = []
                for row in rows:
                    data = json.loads(row["data"]) if row["data"] else None
                    results.append(AsyncOperation(
                        task_id=row["task_id"],
                        operation=row["operation"],
                        model_name=row["model_name"],
                        status=row["status"],
                        progress=row["progress"],
                        created_at=row["created_at"],
                        started_at=row["started_at"],
                        completed_at=row["completed_at"],
                        error=row["error"],
                        error_code=row["error_code"],
                        data=data,
                        updated_at=row["updated_at"]
                    ))

                return results
            except sqlite3.Error as e:
                AppLogger.error("Failed to list async operations: %s", e)
                return []
            finally:
                conn.close()

    def delete_async_operation(self, task_id: str) -> bool:
        """Delete an async operation record.

        Args:
            task_id: Unique task identifier

        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute("DELETE FROM async_operations WHERE task_id = ?", (task_id,))
                conn.commit()
                AppLogger.debug("Deleted async operation: %s", task_id)
                return True
            except sqlite3.Error as e:
                AppLogger.error("Failed to delete async operation: %s", e)
                return False
            finally:
                conn.close()

    def cleanup_download_operations(self, model_name: str) -> int:
        """Delete terminal download operations for a model.

        Active download operations are retained so a concurrent request cannot
        lose track of work that is still queued or running.

        Args:
            model_name: Model whose historical download operations to delete

        Returns:
            Number of deleted operations
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM async_operations
                    WHERE model_name = ?
                      AND operation = 'download'
                      AND status IN ('interrupted', 'failed', 'completed')
                """, (model_name,))
                deleted = cursor.rowcount
                conn.commit()
                if deleted:
                    AppLogger.info(
                        "Cleaned up %d historical download operations for model: %s",
                        deleted,
                        model_name,
                    )
                return deleted
            except sqlite3.Error as e:
                AppLogger.error(
                    "Failed to cleanup download operations for %s: %s",
                    model_name,
                    e,
                )
                return 0
            finally:
                conn.close()

    def fail_stale_running_operations(self) -> int:
        """Reconcile the operations a service restart left behind.

        A restart drops the process-local executor queues, so nothing that was
        only *waiting* can ever start again, and nothing that was *running* can
        report back. One rule per (status, operation) pair:

        - ``queued`` (any): deleted. It never started and the pool that held it is
          gone, so the client re-issues the request if it still wants it. This
          covers a download waiting for the download lock too: the
          ``resource apply`` child is only spawned *after* the lock is taken, so
          a queued download has no detached work behind it.
        - ``running`` download/download_start: ``interrupted``. The archive may
          already be on disk and the detached taosanodectl child may even finish
          after the restart, so the row is kept and the client is told why; the
          resource itself is reconciled by `resource check` (and the row is
          purged by cleanup_download_operations once the model is downloaded
          again).
        - ``running`` delete: resumed -- put back to ``queued`` with a
          ``restart_requeue`` flag that a worker picks up (see
          list_requeued_deletes). Deleting is idempotent (a missing resource
          reports not_found) and it was an explicit client request, so it is
          carried out rather than reported as failed. The task_id is preserved so
          the client's polling continues.
        - ``running`` start/stop: deleted. Whatever worker process exists is not
          tracked by this row -- it is found through the pid files (see the
          liveness check in taosanode_service.py) -- so there is nothing to
          resume and nothing to report.

        Returns:
            Number of operations affected
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                now = time.time()

                # 1. Nothing that was merely waiting/completed can ever start again.
                cursor.execute("DELETE FROM async_operations WHERE status IN ('queued', 'completed')")
                deleted = cursor.rowcount

                # 2. A download that was in flight is reported as stopped.
                cursor.execute("""
                    UPDATE async_operations
                    SET status = 'interrupted', completed_at = ?,
                        error = 'Service restarted while the download was running; re-issue the download to resume'
                    WHERE status = 'running' AND operation IN ('download', 'download_start')
                """, (now,))
                interrupted = cursor.rowcount

                # 3. A delete that was in flight is resumed. Done row by row
                #    because the flag lives inside the JSON `data` column.
                cursor.execute("""
                    SELECT task_id, data FROM async_operations
                    WHERE status = 'running' AND operation = 'delete'
                """)
                resumed = 0
                for row in cursor.fetchall():
                    try:
                        data = json.loads(row["data"]) if row["data"] else {}
                    except (TypeError, ValueError):
                        data = {}
                    if not isinstance(data, dict):
                        data = {}
                    data["restart_requeue"] = True
                    data.pop("waiting_for", None)
                    cursor.execute("""
                        UPDATE async_operations
                        SET status = 'queued', progress = 0, started_at = NULL,
                            completed_at = NULL, error = NULL, error_code = NULL,
                            data = ?, updated_at = ?
                        WHERE task_id = ?
                    """, (json.dumps(data), now, row["task_id"]))
                    resumed += 1

                # 4. start/stop leave nothing to resume.
                cursor.execute("DELETE FROM async_operations WHERE operation IN ('start', 'stop')")
                dropped = cursor.rowcount

                cursor.execute("DELETE FROM async_operations WHERE operation IN ('delete') AND status = 'completed' AND completed_at < ?",
                               (now - 86400,))
                dropped += cursor.rowcount

                conn.commit()
                if deleted or interrupted or resumed or dropped:
                    AppLogger.info(
                        "Startup cleanup: deleted %d queued/completed, dropped %d start/stop, "
                        "interrupted %d downloads, resumed %d deletes",
                        deleted,
                        dropped,
                        interrupted,
                        resumed,
                    )
                return deleted + interrupted + resumed + dropped
            except sqlite3.Error as e:
                AppLogger.error("Failed to clean stale operations: %s", e)
                return 0
            finally:
                conn.close()

    def list_requeued_deletes(self) -> list[AsyncOperation]:
        """Return the delete operations a restart asked to resume.

        Only rows flagged by fail_stale_running_operations are returned; the
        caller must claim each one with claim_requeued_delete before running it,
        so with several gunicorn workers only one of them picks it up.
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM async_operations
                    WHERE operation = 'delete' AND status = 'queued'
                      AND data LIKE '%restart_requeue%'
                """)
                results = []
                for row in cursor.fetchall():
                    try:
                        data = json.loads(row["data"]) if row["data"] else None
                    except (TypeError, ValueError):
                        data = None
                    if not isinstance(data, dict) or data.get("restart_requeue") is not True:
                        continue
                    results.append(AsyncOperation(
                        task_id=row["task_id"],
                        operation=row["operation"],
                        model_name=row["model_name"],
                        status=row["status"],
                        progress=row["progress"],
                        created_at=row["created_at"],
                        started_at=row["started_at"],
                        completed_at=row["completed_at"],
                        error=row["error"],
                        error_code=row["error_code"],
                        data=data,
                        updated_at=row["updated_at"],
                    ))
                return results
            except sqlite3.Error as e:
                AppLogger.error("Failed to list requeued deletes: %s", e)
                return []
            finally:
                conn.close()

    def claim_requeued_delete(self, operation: AsyncOperation) -> bool:
        """Atomically take ownership of a resumable delete.

        The claim is a compare-and-swap on the raw ``data`` value: only one
        caller can match the exact serialized form it read, so a second gunicorn
        worker (or a second call) gets False and leaves the task alone. The flag
        is cleared in the same statement, which also makes the claim idempotent
        across restarts.

        Args:
            operation: Row previously returned by list_requeued_deletes

        Returns:
            True when this caller owns the task and should run it.
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                original = json.dumps(operation.data)
                claimed = dict(operation.data)
                claimed.pop("restart_requeue", None)
                claimed["attempt"] = 0

                cursor.execute("""
                    UPDATE async_operations
                    SET data = ?, updated_at = ?
                    WHERE task_id = ? AND status = 'queued' AND data = ?
                """, (json.dumps(claimed), time.time(), operation.task_id, original))
                conn.commit()
                return cursor.rowcount == 1
            except sqlite3.Error as e:
                AppLogger.error("Failed to claim requeued delete %s: %s", operation.task_id, e)
                return False
            finally:
                conn.close()

    def list_recently_finished_downloads(self, since: float) -> list[AsyncOperation]:
        """Return download-family operations that finished at or after ``since``.

        Used by the SSE stream so a client that subscribes after the fact (a
        reconnected EventSource, a reloaded page) still learns how an operation
        ended: the stream only closes out the phase streams its own connection
        reported, so without this replay the outcome would never reach it.

        Args:
            since: Epoch seconds; operations completed before it are left out

        Returns:
            List of finished download/download_start operations, newest first
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM async_operations
                    WHERE operation IN ('download', 'download_start')
                      AND status IN ('completed', 'failed')
                      AND completed_at >= ?
                    ORDER BY completed_at DESC
                """, (since,))
                rows = cursor.fetchall()
                results = []
                for row in rows:
                    data = json.loads(row["data"]) if row["data"] else None
                    results.append(AsyncOperation(
                        task_id=row["task_id"],
                        operation=row["operation"],
                        model_name=row["model_name"],
                        status=row["status"],
                        progress=row["progress"],
                        created_at=row["created_at"],
                        started_at=row["started_at"],
                        completed_at=row["completed_at"],
                        error=row["error"],
                        error_code=row["error_code"],
                        data=data,
                        updated_at=row["updated_at"],
                    ))
                return results
            except sqlite3.Error as e:
                AppLogger.error("Failed to list recently finished downloads: %s", e)
                return []
            finally:
                conn.close()

    def get_latest_operation(self, model_name: str) -> Optional["AsyncOperation"]:
        """Get the most recent async operation for a model.

        Args:
            model_name: Model name to query

        Returns:
            Most recent AsyncOperation or None
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM async_operations
                    WHERE model_name = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (model_name,))
                row = cursor.fetchone()
                if not row:
                    return None
                data = json.loads(row["data"]) if row["data"] else None
                return AsyncOperation(
                    task_id=row["task_id"],
                    operation=row["operation"],
                    model_name=row["model_name"],
                    status=row["status"],
                    progress=row["progress"],
                    created_at=row["created_at"],
                    started_at=row["started_at"],
                    completed_at=row["completed_at"],
                    error=row["error"],
                    error_code=row["error_code"],
                    data=data,
                    updated_at=row["updated_at"]
                )
            except sqlite3.Error as e:
                AppLogger.error("Failed to get latest operation for %s: %s", model_name, e)
                return None
            finally:
                conn.close()

    def cleanup_old_operations(self, days: int = 7) -> int:
        """Delete completed/failed operations older than specified days.

        Args:
            days: Number of days to keep

        Returns:
            Number of deleted records
        """
        with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                cutoff_time = time.time() - (days * 86400)

                cursor.execute("""
                    DELETE FROM async_operations
                    WHERE status IN ('completed', 'failed') AND completed_at < ?
                """, (cutoff_time,))

                deleted = cursor.rowcount
                conn.commit()
                AppLogger.info("Cleaned up %d old async operations", deleted)
                return deleted
            except sqlite3.Error as e:
                AppLogger.error("Failed to cleanup old operations: %s", e)
                return 0
            finally:
                conn.close()


# ============================================================================
# Singleton Pattern for DAO Access
# ============================================================================

_dao_instance: Optional[FoundationModelsDAO] = None
_dao_lock = threading.Lock()


def get_dao() -> FoundationModelsDAO:
    """Get or create the global DAO instance."""
    global _dao_instance
    if _dao_instance is None:
        with _dao_lock:
            if _dao_instance is None:
                _dao_instance = FoundationModelsDAO()
    return _dao_instance


def init_dao(db_path: Optional[str] = None) -> FoundationModelsDAO:
    """Initialize or reinitialize the global DAO instance.

    Args:
        db_path: Optional path to SQLite database file

    Returns:
        The initialized FoundationModelsDAO instance
    """
    global _dao_instance
    with _dao_lock:
        _dao_instance = FoundationModelsDAO(db_path)
    return _dao_instance
