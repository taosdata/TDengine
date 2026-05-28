"""
Local conftest for UDF tests.

Kills orphan taosudf processes during teardown to prevent
IPC conflicts when taosd is force-killed and its child udfd survives.
By killing udfd BEFORE the parent conftest kills taosd, we ensure
the next test class gets a fresh taosd with no orphan udfd interference.
"""
import os
import platform

import pytest


@pytest.fixture(scope="class", autouse=True)
def kill_orphan_udfd(request):
    """Kill taosudf processes during teardown so next test starts clean."""
    yield
    _kill_all_udfd()


def _kill_all_udfd():
    if platform.system().lower() == "windows":
        os.system("taskkill /f /im taosudf.exe >nul 2>&1")
    else:
        os.system("pkill -9 taosudf 2>/dev/null || true")
