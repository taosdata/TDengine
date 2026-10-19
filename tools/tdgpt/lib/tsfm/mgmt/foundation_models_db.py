# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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

from lib.conf import Configure
from lib.log import AppLogger

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
    model_state: str = "missing"  # Unified state from ModelState enum
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

# The columns `update_model` / `update_async_operation` may write, and the only
# source of the column names their statements interpolate: the values travel as
# bound parameters, the identifiers come from these tuples. Iterating the
# constant -- rather than filtering the caller's keys -- is what keeps that true
# even if the filter is ever loosened, so an invented field name cannot reach the
# SQL. Adding a column here is the one way to make it writable.
_MODEL_UPDATE_COLUMNS = (
    "downloaded",
    "running",
    "status",
    "size_gb",
    "description",
    "model_state",
)
_OPERATION_UPDATE_COLUMNS = (
    "status",
    "progress",
    "started_at",
    "completed_at",
    "error",
    "error_code",
    "data",
)


def get_resource_dir() -> str:
    """Return the resource directory for model/runtime archives and task state.

    Reads the configured ``resource_dir`` from taosanode.config.py or uses the
    default value. On Linux, this is ``/var/lib/taos/taosanode/resource``;
    on other platforms, it is ``<install_dir>/data/resource``.

    The returned path is guaranteed to be set and non-empty.

    Returns:
        str: Absolute path to the resource directory
    """
    config = Configure.get_instance()
    return config._conf.get("resource_dir", "")


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
            resource_dir = get_resource_dir()
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

    def _with_db(self, callback, error_msg: str, *args, raise_error: bool = False, **kwargs):
        """Execute a callback within a locked database transaction.

        Handles connection, cursor setup, error logging, and cleanup.
        Callback receives (conn, cursor, *args, **kwargs).

        Args:
            callback: Function to execute with (conn, cursor, *args, **kwargs)
            error_msg: Error message format string for logging
            *args: Positional arguments to pass to callback
            raise_error: If True, re-raise sqlite3.Error; if False, return None
            **kwargs: Keyword arguments to pass to callback
        """
        with self._lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                return callback(conn, cursor, *args, **kwargs)
            except sqlite3.Error as e:
                AppLogger.error(error_msg, e)
                if raise_error:
                    raise
                return None
            except Exception as e:  # 非 sqlite 异常也记录后归一化返回，避免上下文丢失
                AppLogger.error(error_msg, e)
                if raise_error:
                    raise
                return None
            finally:
                if conn is not None:
                    conn.close()


    def _init_db(self):
        """Initialize database schema if it doesn't exist."""

        def _create(conn, cursor):
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
                    model_state TEXT DEFAULT 'missing',
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

            # Migrate existing databases that predate the model_state column.
            try:
                cursor.execute(
                    "ALTER TABLE models ADD COLUMN model_state TEXT DEFAULT 'missing'"
                )
                AppLogger.info("Migrated models table: added model_state column")
            except sqlite3.OperationalError:
                pass  # column already exists

            conn.commit()
            AppLogger.debug("Database schema initialized")

        self._with_db(_create, "Failed to initialize database schema: %s", raise_error=True)

    def _async_operation_from_row(self, row: sqlite3.Row) -> AsyncOperation:
        """Construct an AsyncOperation from a database row."""
        try:
            data = json.loads(row["data"]) if row["data"] else None
        except (TypeError, ValueError):
            data = None

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
            updated_at=row["updated_at"],
        )

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
        def _create(conn, cursor, model: FoundationModel):
            cursor.execute("""
                INSERT OR REPLACE INTO models
                (name, model_id, package, type, description, size_gb, status,
                    downloaded, running, model_state, discovered_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                model.name, model.model_id, model.package, model.type,
                model.description, model.size_gb, model.status,
                1 if model.downloaded else 0, 1 if model.running else 0,
                model.model_state, model.discovered_at, model.updated_at
            ))
            conn.commit()
            AppLogger.debug("Created model: %s", model.name)
            return True

        result = self._with_db(_create, f"Failed to create model: {model.name} : %s", model)
        return result if result is not None else False


    def get_model(self, name: str) -> Optional[FoundationModel]:
        """Retrieve a model by name.

        Args:
            name: Model name

        Returns:
            FoundationModel instance or None if not found
        """
        def _get(conn, cursor, name):
            cursor.execute("SELECT * FROM models WHERE name = ?", (name,))
            row = cursor.fetchone()

            if not row:
                return None

            # Handle missing model_state column (for backward compatibility with old DBs)
            try:
                model_state = row["model_state"]
            except IndexError:
                model_state = "missing"

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
                model_state=model_state,
                discovered_at=row["discovered_at"],
                updated_at=row["updated_at"]
            )

        return self._with_db(_get, f"Failed to retrieve model: {name} : %s", name)

    def update_model(self, name: str, **kwargs) -> bool:
        """Update a model record.

        Args:
            name: Model name
            **kwargs: Fields to update (downloaded, running, status, etc.)

        Returns:
            True if successful, False otherwise
        """
        def _update(conn, cursor, name: str, **kwargs):
            # Collect the writable fields this call carries. A None is skipped, so
            # a column can only be set to a real value (see the constants above
            # for why the caller's keys are never iterated into the statement).
            update_fields = {
                column: kwargs[column]
                for column in _MODEL_UPDATE_COLUMNS
                if kwargs.get(column) is not None
            }

            if not update_fields:
                return False

            # Convert boolean fields
            if "downloaded" in update_fields:
                update_fields["downloaded"] = 1 if update_fields["downloaded"] else 0
            if "running" in update_fields:
                update_fields["running"] = 1 if update_fields["running"] else 0

            columns = [column for column in _MODEL_UPDATE_COLUMNS if column in update_fields]
            set_clause = ", ".join(f"{column} = ?" for column in columns) + ", updated_at = ?"
            values = [update_fields[column] for column in columns] + [time.time(), name]

            cursor.execute(f"UPDATE models SET {set_clause} WHERE name = ?", values)
            conn.commit()

            AppLogger.debug("Updated model: %s", name)
            return True

        result = self._with_db(_update, f"Failed to update model: {name} : %s", name, **kwargs)
        return result if result is not None else False

    def list_models(self) -> list[FoundationModel]:
        """List all models.

        Returns:
            List of FoundationModel instances
        """

        def _list_all(conn, cursor):
            cursor.execute("SELECT * FROM models ORDER BY name")
            rows = cursor.fetchall()
            results = []
            for row in rows:
                # Handle missing model_state column (for backward compatibility with old DBs)
                try:
                    model_state = row["model_state"]
                except IndexError:
                    model_state = "missing"

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
                    model_state=model_state,
                    discovered_at=row["discovered_at"],
                    updated_at=row["updated_at"]
                ))

            return results

        result = self._with_db(_list_all, "Failed to list models: %s")
        return result if result is not None else []


    def delete_model(self, name: str) -> bool:
        """Delete a model record.

        Args:
            name: Model name

        Returns:
            True if successful, False otherwise
        """
        def _delete_model(conn, cursor):
            cursor.execute("DELETE FROM models WHERE name = ?", (name,))
            conn.commit()
            AppLogger.debug("Deleted model: %s", name)
            return True

        result = self._with_db(_delete_model, f"Failed to delete model: {name} : %s")
        return result if result is not None else False


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
        def _create(conn, cursor, operation: AsyncOperation, data_json: Optional[str] = None):
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

        try:
            data_json = json.dumps(operation.data) if operation.data else None
        except (TypeError, ValueError) as e:
            AppLogger.error("Failed to serialize async operation data: %s", e)
            return False

        result = self._with_db(_create, f"Failed to create async operation: {operation.task_id} : %s", operation, data_json)
        return result if result is not None else False

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
        def _insert(conn, cursor, operation: AsyncOperation, data_json: Optional[str] = None):
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

        try:
            data_json = json.dumps(operation.data) if operation.data else None
        except (TypeError, ValueError) as e:
            AppLogger.error("Failed to serialize async operation data: %s", e)
            return False

        result = self._with_db(_insert, f"Failed to create async operation: {operation.task_id} : %s", operation, data_json)
        return result if result is not None else False


    def get_async_operation(self, task_id: str) -> Optional[AsyncOperation]:
        """Retrieve an async operation by task_id.

        Args:
            task_id: Unique task identifier

        Returns:
            AsyncOperation instance or None if not found
        """

        def _get(conn, cursor, task_id: str):
            cursor.execute("SELECT * FROM async_operations WHERE task_id = ?", (task_id,))
            row = cursor.fetchone()
            return self._async_operation_from_row(row) if row is not None else None

        return self._with_db(_get, f"Failed to retrieve async operation: {task_id} : %s", task_id)


    def update_async_operation(self, task_id: str, **kwargs) -> bool:
        """Update an async operation record.

        Args:
            task_id: Unique task identifier
            **kwargs: Fields to update (status, progress, error, etc.)

        Returns:
            True if successful, False otherwise
        """
        def _update(conn, cursor, task_id: str, **kwargs):
            # Collect the writable fields this call carries. Unlike update_model,
            # None is a value here: it is how a caller clears `error` or
            # `started_at` (see the constants above for why the caller's keys are
            # never iterated into the statement).
            update_fields = {
                column: kwargs[column]
                for column in _OPERATION_UPDATE_COLUMNS
                if column in kwargs
            }

            if not update_fields:
                return False

            # Serialize data if present
            if update_fields.get("data") is not None:
                try:
                    update_fields["data"] = json.dumps(update_fields["data"])
                except (TypeError, ValueError) as e:
                    AppLogger.error("Failed to serialize async operation data: %s in update_async_operation", e)
                    return False

            columns = [column for column in _OPERATION_UPDATE_COLUMNS if column in update_fields]
            set_clause = ", ".join(f"{column} = ?" for column in columns) + ", updated_at = ?"
            values = [update_fields[column] for column in columns] + [time.time(), task_id]

            cursor.execute(f"UPDATE async_operations SET {set_clause} WHERE task_id = ?", values)
            conn.commit()
            AppLogger.debug("Updated async operation: %s", task_id)
            return True

        result = self._with_db(_update, f"Failed to update async operation: {task_id} : %s", task_id, **kwargs)
        return result if result is not None else False


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
        def _mark(conn, cursor, task_id:str):
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

        result = self._with_db(_mark, f"Failed to mark async operation running: {task_id} : %s", task_id)
        return result if result is not None else False

    def list_async_operations(self, model_name: Optional[str] = None, active_only: bool = False) -> list[AsyncOperation]:
        """List async operations, optionally filtered by model or status.

        Args:
            model_name: Optional model name to filter
            active_only: If True, only return queued/running tasks (exclude completed/failed)

        Returns:
            List of AsyncOperation instances
        """
        def _list_operations(conn, cursor, model_name: Optional[str], active_only: bool):
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
            return [self._async_operation_from_row(row) for row in rows]

        result = self._with_db(_list_operations, f"Failed to list async operations: {model_name} : %s", model_name, active_only)
        return result if result is not None else []


    def delete_async_operation(self, task_id: str) -> bool:
        """Delete an async operation record.

        Args:
            task_id: Unique task identifier

        Returns:
            True if successful, False otherwise
        """
        def _delete(conn, cursor, task_id:str):
            cursor.execute("DELETE FROM async_operations WHERE task_id = ?", (task_id,))
            conn.commit()
            AppLogger.debug("Deleted async operation: %s", task_id)
            return True

        result = self._with_db(_delete, f"Failed to delete async operation: {task_id} : %s", task_id)
        return result if result is not None else False

    def cleanup_download_operations(self, model_name: str) -> int:
        """Delete terminal download-family operations for a model.

        Both ``download`` and ``download_start`` rows are cleared. They share one
        per-model namespace -- the de-duplication in
        create_download_operation_if_idle treats them as one family -- and the
        newest row of either name is what get_latest_operation reports as the
        resource's ``operation_status``. A terminal row left behind therefore
        keeps claiming a state the resource is no longer in, and ``interrupted``
        needs this purge in particular: cleanup_old_operations only sweeps
        ``completed``/``failed``, so nothing else would ever remove it.

        Active download operations are retained so a concurrent request cannot
        lose track of work that is still queued or running.

        Args:
            model_name: Model whose historical download operations to delete

        Returns:
            Number of deleted operations
        """
        def _cleanup(conn, cursor, model_name:str):
            cursor.execute("""
                DELETE FROM async_operations
                WHERE model_name = ?
                    AND operation IN ('download', 'download_start')
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

        result = self._with_db(_cleanup, f"Failed to cleanup download operations for {model_name}", model_name)
        return result if result is not None else 0


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
        - ``completed``/``failed`` (any): deleted. The client already knows the
          outcome, so the row is just clutter.
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

        def _fail_stale(conn, cursor):
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

                try:
                    data_json = json.dumps(data, sort_keys=True)
                except (TypeError, ValueError) as e:
                    AppLogger.error("Failed to serialize async operation data: %s in fail_stale_running_operations", e)
                    continue

                cursor.execute("""
                    UPDATE async_operations
                    SET status = 'queued', progress = 0, started_at = NULL,
                        completed_at = NULL, error = NULL, error_code = NULL,
                        data = ?, updated_at = ?
                    WHERE task_id = ?
                """, (data_json, now, row["task_id"]))
                resumed += 1

            # 4. start/stop leave nothing to resume.
            cursor.execute("DELETE FROM async_operations WHERE operation IN ('start', 'stop')")
            dropped = cursor.rowcount

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

        result = self._with_db(_fail_stale, "Failed to clean stale operations: %s")
        return result if result is not None else 0


    def list_requeued_deletes(self) -> list[AsyncOperation]:
        """Return the delete operations a restart asked to resume.

        Only rows flagged by fail_stale_running_operations are returned; the
        caller must claim each one with claim_requeued_delete before running it,
        so with several gunicorn workers only one of them picks it up.
        """

        def _list_deletes(conn, cursor):
            cursor.execute("""
                SELECT * FROM async_operations
                WHERE operation = 'delete' AND status = 'queued'
                  AND data LIKE '%restart_requeue%'
            """)
            rows = cursor.fetchall()
            results = []
            for row in rows:
                try:
                    data = json.loads(row["data"]) if row["data"] else None
                except (TypeError, ValueError):
                    data = None
                if not isinstance(data, dict) or data.get("restart_requeue") is not True:
                    continue

                results.append(self._async_operation_from_row(row))
            return results

        result = self._with_db(_list_deletes, "Failed to list requeued deletes: %s")
        return result if result is not None else []


    def claim_requeued_delete(self, operation: AsyncOperation) -> bool:
        """Atomically take ownership of a resumable delete.

        The claim is a compare-and-swap on the row's version token
        (``updated_at``): only one caller can match the value it read, so a
        second gunicorn worker (or a second call) gets False and leaves the task
        alone. The flag is cleared in the same statement, which also makes the
        claim idempotent across restarts.

        The token is the row version, not the raw ``data`` text: the value to
        compare against has to be re-serialized here, and JSON text is not a
        stable representation of a payload. The requeue writer dumps in insertion
        order while this method dumps with ``sort_keys``, so a delete that had
        been deferred before the restart carried
        ``attempt, wait_since, retry_after, restart_requeue`` -- the two strings
        could only agree by luck. The task was then listed as resumable, silently
        dropped (a False here is indistinguishable from another worker winning
        the race) and left ``queued`` for good.

        ``updated_at`` is bumped by every writer that touches a delete row, so it
        changes exactly when the row does. The status must stay ``queued``: the
        worker that runs the task is resubmitted only while it is still queued
        (see _resubmit_service_task), which is why a status flip cannot be the
        token.

        Args:
            operation: Row previously returned by list_requeued_deletes

        Returns:
            True when this caller owns the task and should run it.
        """

        def _claim(conn, cursor, operation: AsyncOperation):
            claimed = dict(operation.data) if operation.data else {}
            claimed.pop("restart_requeue", None)
            claimed["attempt"] = 0

            try:
                claimed_json = json.dumps(claimed, sort_keys=True)
            except (TypeError, ValueError) as e:
                AppLogger.error("Failed to serialize async operation data: %s in claim_requeued_delete", e)
                return False

            cursor.execute("""
                UPDATE async_operations
                SET data = ?, updated_at = ?
                WHERE task_id = ? AND status = 'queued' AND updated_at = ?
            """, (claimed_json, time.time(), operation.task_id, operation.updated_at))
            conn.commit()
            return cursor.rowcount == 1

        result = self._with_db(_claim, "Failed to claim requeued delete %s: %s", operation)
        return result if result is not None else False


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
        def _list_downloads(conn, cursor, since: float):
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
                results.append(self._async_operation_from_row(row))
            return results

        result = self._with_db(_list_downloads, "Failed to list recently finished downloads: %s", since)
        return result if result is not None else []

    def get_latest_operation(self, model_name: str) -> Optional["AsyncOperation"]:
        """Get the most recent async operation for a model.

        Args:
            model_name: Model name to query

        Returns:
            Most recent AsyncOperation or None
        """
        def _get_latest(conn, cursor, model_name: str) -> Optional[AsyncOperation]:
            cursor.execute("""
                SELECT * FROM async_operations
                WHERE model_name = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (model_name,))
            row = cursor.fetchone()
            if not row:
                return None

            return self._async_operation_from_row(row)

        return self._with_db(_get_latest, f"Failed to get latest operation {model_name}: %s", model_name)


    def cleanup_old_operations(self, days: int = 7) -> int:
        """Delete completed/failed operations older than specified days.

        Args:
            days: Number of days to keep

        Returns:
            Number of deleted records
        """
        def _cleanup(conn, cursor, days: int) -> int:
            cutoff_time = time.time() - (days * 86400)

            cursor.execute("""
                DELETE FROM async_operations
                WHERE status IN ('completed', 'failed') AND completed_at < ?
            """, (cutoff_time,))

            deleted = cursor.rowcount
            conn.commit()

            AppLogger.info("Cleaned up %d old async operations", deleted)
            return deleted

        result = self._with_db(_cleanup, "Failed to cleanup old operations: %s", days)
        return result if result is not None else 0


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
