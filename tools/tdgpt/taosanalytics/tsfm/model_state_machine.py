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

"""Model state machine: unified state management for foundation models.

Defines a single source of truth for model lifecycle states, replacing the
complexity of managing multiple fields (service_status, state, downloaded,
running, operation_status). Provides clear state transitions and validation.

State diagram:
    MISSING ─→ DOWNLOADING ─→ IDLE ─┬→ STARTING ─→ STARTED ─→ STOPPING ─┐
                                    │                                   │
                                    └─→ DELETING ───────────────────────┴─→ IDLE or MISSING
"""

from enum import Enum
from typing import Optional, Set, Tuple


class ModelState(Enum):
    """Unified model lifecycle states."""
    MISSING = "missing"
    DOWNLOADING = "downloading"
    IDLE = "idle"
    STARTING = "starting"
    STARTED = "started"
    STOPPING = "stopping"
    DELETING = "deleting"


class ModelStateTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""
    pass


class ModelStateManager:
    """Manages model state transitions and calculations."""

    # Define all valid state transitions
    VALID_TRANSITIONS: dict[ModelState, Set[ModelState]] = {
        ModelState.MISSING: {ModelState.DOWNLOADING, ModelState.DELETING},
        ModelState.DOWNLOADING: {ModelState.IDLE, ModelState.MISSING},
        ModelState.IDLE: {ModelState.STARTING, ModelState.DELETING},
        ModelState.STARTING: {ModelState.STARTED, ModelState.IDLE},
        ModelState.STARTED: {ModelState.STOPPING},
        ModelState.STOPPING: {ModelState.IDLE, ModelState.STARTED},
        ModelState.DELETING: {ModelState.MISSING, ModelState.IDLE},
    }

    # Map (operation, operation_status) to next model_state
    STATE_TRANSITIONS: dict[tuple[str, str], ModelState] = {
        # Download operation
        ("download", "queued"): ModelState.DOWNLOADING,
        ("download", "running"): ModelState.DOWNLOADING,
        ("download", "deferred"): ModelState.DOWNLOADING,
        ("download", "completed"): ModelState.IDLE,
        ("download", "failed"): ModelState.MISSING,
        ("download", "interrupted"): ModelState.MISSING,

        # Download-start operation (composite: download then start)
        ("download_start", "queued"): ModelState.DOWNLOADING,
        ("download_start", "running"): ModelState.DOWNLOADING,
        ("download_start", "deferred"): ModelState.DOWNLOADING,
        ("download_start", "completed"): ModelState.STARTED,
        ("download_start", "failed"): ModelState.MISSING,
        ("download_start", "interrupted"): ModelState.MISSING,

        # Start operation
        ("start", "queued"): ModelState.STARTING,
        ("start", "running"): ModelState.STARTING,
        ("start", "deferred"): ModelState.STARTING,
        ("start", "completed"): ModelState.STARTED,
        ("start", "failed"): ModelState.IDLE,
        ("start", "interrupted"): ModelState.IDLE,

        # Stop operation
        ("stop", "queued"): ModelState.STOPPING,
        ("stop", "running"): ModelState.STOPPING,
        ("stop", "deferred"): ModelState.STOPPING,
        ("stop", "completed"): ModelState.IDLE,
        ("stop", "failed"): ModelState.STARTED,
        ("stop", "interrupted"): ModelState.STARTED,

        # Delete operation
        ("delete", "queued"): ModelState.DELETING,
        ("delete", "running"): ModelState.DELETING,
        ("delete", "deferred"): ModelState.DELETING,
        ("delete", "completed"): ModelState.MISSING,
        ("delete", "failed"): ModelState.IDLE,
        ("delete", "interrupted"): ModelState.IDLE,
    }

    # Composite operations that skip intermediate states; exempt from VALID_TRANSITIONS check
    COMPOSITE_OPERATIONS: Set[str] = {"download_start"}

    # Actions allowed from each state
    ALLOWED_ACTIONS: dict[str, Set[str]] = {
        ModelState.MISSING.value: {"download", "download_start", "delete"},
        ModelState.DOWNLOADING.value: set(),
        ModelState.IDLE.value: {"start", "delete", "download_start"},
        ModelState.STARTING.value: set(),
        ModelState.STARTED.value: {"stop"},
        ModelState.STOPPING.value: set(),
        ModelState.DELETING.value: set(),
    }

    @staticmethod
    def calculate_next_state(
        operation: Optional[str],
        operation_status: Optional[str],
        current_state: str,
    ) -> str:
        """Calculate next model state based on operation and its status.

        Given an operation (download, start, stop, delete) and its current
        status (queued, running, completed, failed, deferred, interrupted),
        determines what the model's next state should be.

        Args:
            operation: The async operation type, or None if no active operation
            operation_status: The current status of the operation
            current_state: The current model_state value

        Returns:
            The next model_state value

        Raises:
            ValueError: If operation/status combination is unknown
        """

        # No active operation - state remains unchanged
        if operation is None or operation_status is None:
            return current_state

        # Look up the target state
        key = (operation, operation_status)
        next_state = ModelStateManager.STATE_TRANSITIONS.get(key)

        if next_state is None:
            raise ValueError(
                f"Unknown operation/status combination: {operation}/{operation_status}"
            )

        if operation not in ModelStateManager.COMPOSITE_OPERATIONS:
            is_valid, error = ModelStateManager.can_transition(current_state, next_state.value)
            if not is_valid:
                raise ModelStateTransitionError(
                    f"Operation '{operation}/{operation_status}' targets state "
                    f"'{next_state.value}' but {error}"
                )

        return next_state.value

    @staticmethod
    def can_transition(
        current_state: str,
        next_state: str,
    ) -> Tuple[bool, Optional[str]]:
        """Check if a state transition is valid.

        Args:
            current_state: Current model_state value
            next_state: Proposed next model_state value

        Returns:
            (is_valid, error_message_if_invalid)
        """

        try:
            current_enum = ModelState(current_state)
        except ValueError:
            return False, f"Unknown current state: {current_state}"

        try:
            next_enum = ModelState(next_state)
        except ValueError:
            return False, f"Unknown next state: {next_state}"

        allowed = ModelStateManager.VALID_TRANSITIONS.get(current_enum, set())

        if next_enum in allowed:
            return True, None

        return False, (
            f"Invalid transition: {current_state} → {next_state}. "
            f"Allowed transitions: {', '.join(s.value for s in allowed)}"
        )

    @staticmethod
    def can_perform_action(
        model_state: str,
        action: str,
    ) -> Tuple[bool, Optional[str]]:
        """Check if an action is allowed from the current state.

        Args:
            model_state: Current model_state value
            action: Desired action (download, start, stop, delete)

        Returns:
            (is_allowed, reason_if_not_allowed)
        """

        allowed_actions = ModelStateManager.ALLOWED_ACTIONS.get(model_state, set())

        if action in allowed_actions:
            return True, None

        return False, f"Cannot {action} model in '{model_state}' state"

    @staticmethod
    def infer_state_from_legacy_fields(
        downloaded: bool,
        running: bool,
    ) -> str:
        """Infer model_state from legacy fields (for migration).

        Used to initialize model_state for existing models based on their
        current downloaded/running status.

        Args:
            downloaded: Value of the downloaded field
            running: Value of the running field

        Returns:
            The inferred model_state value
        """

        if not downloaded:
            return ModelState.MISSING.value
        elif running:
            return ModelState.STARTED.value
        else:
            return ModelState.IDLE.value


# Convenient reference for use in other modules
__all__ = [
    "ModelState",
    "ModelStateTransitionError",
    "ModelStateManager",
]
