"""Unit tests for ModelStateManager and ModelState."""

import pytest

from lib.tsfm.mgmt.model_state_machine import ModelState, ModelStateManager


class TestCalculateNextState:
    """calculate_next_state maps (operation, status) to a model_state string."""

    @pytest.mark.parametrize("operation,status,current,expected", [
        # download lifecycle (from missing or idle)
        ("download", "queued",      "missing", "downloading"),
        ("download", "running",     "missing", "downloading"),
        ("download", "deferred",    "missing", "downloading"),
        ("download", "completed",   "downloading", "idle"),
        ("download", "failed",      "downloading", "missing"),
        ("download", "interrupted", "downloading", "missing"),
        # start lifecycle (from idle)
        ("start", "queued",      "idle", "starting"),
        ("start", "running",     "idle", "starting"),
        ("start", "deferred",    "idle", "starting"),
        ("start", "completed",   "starting", "started"),
        ("start", "failed",      "starting", "idle"),
        ("start", "interrupted", "starting", "idle"),
        # stop lifecycle (from started)
        ("stop", "queued",      "started", "stopping"),
        ("stop", "running",     "started", "stopping"),
        ("stop", "deferred",    "started", "stopping"),
        ("stop", "completed",   "stopping", "idle"),
        ("stop", "failed",      "stopping", "started"),
        ("stop", "interrupted", "stopping", "started"),
        # delete lifecycle (from idle or started)
        ("delete", "queued",      "idle", "deleting"),
        ("delete", "running",     "idle", "deleting"),
        ("delete", "deferred",    "idle", "deleting"),
        ("delete", "completed",   "deleting", "missing"),
        ("delete", "failed",      "deleting", "idle"),
        ("delete", "interrupted", "deleting", "idle"),
    ])
    def test_known_transition(self, operation, status, current, expected):
        result = ModelStateManager.calculate_next_state(
            operation=operation,
            operation_status=status,
            current_state=current,
        )
        assert result == expected

    def test_no_operation_returns_current_state(self):
        assert ModelStateManager.calculate_next_state(
            operation=None, operation_status="completed", current_state="started"
        ) == "started"

    def test_no_status_returns_current_state(self):
        assert ModelStateManager.calculate_next_state(
            operation="start", operation_status=None, current_state="idle"
        ) == "idle"

    def test_both_none_returns_current_state(self):
        assert ModelStateManager.calculate_next_state(
            operation=None, operation_status=None, current_state="downloading"
        ) == "downloading"

    def test_unknown_combination_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown operation/status"):
            ModelStateManager.calculate_next_state(
                operation="start", operation_status="bogus", current_state="idle"
            )

    def test_unknown_operation_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown operation/status"):
            ModelStateManager.calculate_next_state(
                operation="reboot", operation_status="completed", current_state="idle"
            )


class TestCanTransition:
    """can_transition validates state graph edges."""

    @pytest.mark.parametrize("current,next_state", [
        ("missing",     "downloading"),
        ("downloading", "idle"),
        ("downloading", "missing"),
        ("idle",        "starting"),
        ("idle",        "deleting"),
        ("starting",    "started"),
        ("starting",    "idle"),
        ("started",     "stopping"),
        ("stopping",    "idle"),
        ("deleting",    "missing"),
        ("deleting",    "idle"),
    ])
    def test_valid_transitions(self, current, next_state):
        ok, err = ModelStateManager.can_transition(current, next_state)
        assert ok is True
        assert err is None

    @pytest.mark.parametrize("current,next_state", [
        ("missing",  "started"),
        ("idle",     "stopping"),
        # A delete during a download is refused by the request layer, so the
        # graph deliberately has no edge for it.
        ("downloading", "deleting"),
        ("started",  "deleting"),
        ("started",  "idle"),
    ])
    def test_invalid_transitions(self, current, next_state):
        ok, err = ModelStateManager.can_transition(current, next_state)
        assert ok is False
        assert err is not None

    def test_unknown_current_state(self):
        ok, err = ModelStateManager.can_transition("limbo", "idle")
        assert ok is False
        assert "Unknown current state" in err

    def test_unknown_next_state(self):
        ok, err = ModelStateManager.can_transition("idle", "limbo")
        assert ok is False
        assert "Unknown next state" in err


class TestCanPerformOperation:
    """can_perform_operation answers the transition question without writing."""

    @pytest.mark.parametrize("operation,state", [
        ("start",  "idle"),
        ("stop",   "started"),
        ("delete", "idle"),
        ("delete", "missing"),
        # composite operations skip the intermediate states
        ("download_start", "started"),
        ("download_start", "missing"),
    ])
    def test_allowed(self, operation, state):
        ok, reason = ModelStateManager.can_perform_operation(operation, state)
        assert ok is True
        assert reason is None

    @pytest.mark.parametrize("operation,state", [
        ("start",  "started"),
        ("start",  "starting"),
        ("start",  "missing"),
        ("stop",   "idle"),
        ("delete", "starting"),
        ("delete", "stopping"),
        ("delete", "downloading"),
    ])
    def test_refused(self, operation, state):
        ok, reason = ModelStateManager.can_perform_operation(operation, state)
        assert ok is False
        assert reason is not None

    def test_unknown_operation_is_not_judged(self):
        ok, reason = ModelStateManager.can_perform_operation("reboot", "idle")
        assert ok is True
        assert reason is None


class TestCanPerformAction:
    """can_perform_action gates user-facing operations by model_state."""

    @pytest.mark.parametrize("state,action", [
        ("missing",  "download"),
        ("missing",  "download_start"),
        ("idle",     "start"),
        ("idle",     "delete"),
        ("idle",     "download_start"),
        ("started",  "stop"),
    ])
    def test_allowed_actions(self, state, action):
        ok, reason = ModelStateManager.can_perform_action(state, action)
        assert ok is True
        assert reason is None

    @pytest.mark.parametrize("state,action", [
        ("missing",     "start"),
        ("missing",     "stop"),
        ("downloading", "download"),
        ("downloading", "start"),
        ("starting",    "start"),
        ("started",     "start"),
        ("started",     "delete"),
        ("stopping",    "stop"),
        ("deleting",    "delete"),
    ])
    def test_blocked_actions(self, state, action):
        ok, reason = ModelStateManager.can_perform_action(state, action)
        assert ok is False
        assert reason is not None


class TestInferStateFromLegacyFields:
    """infer_state_from_legacy_fields covers migration from old downloaded/running columns."""

    def test_not_downloaded_is_missing(self):
        assert ModelStateManager.infer_state_from_legacy_fields(
            downloaded=False, running=False
        ) == "missing"

    def test_downloaded_and_running_is_started(self):
        assert ModelStateManager.infer_state_from_legacy_fields(
            downloaded=True, running=True
        ) == "started"

    def test_downloaded_and_not_running_is_idle(self):
        assert ModelStateManager.infer_state_from_legacy_fields(
            downloaded=True, running=False
        ) == "idle"

    def test_not_downloaded_but_running_is_missing(self):
        # Inconsistent legacy state: downloaded=False takes priority.
        assert ModelStateManager.infer_state_from_legacy_fields(
            downloaded=False, running=True
        ) == "missing"


class TestModelStateEnum:
    """ModelState enum values match the strings used throughout the codebase."""

    def test_all_values_are_lowercase_strings(self):
        for state in ModelState:
            assert state.value == state.value.lower()

    def test_enum_round_trips_from_string(self):
        for state in ModelState:
            assert ModelState(state.value) is state
