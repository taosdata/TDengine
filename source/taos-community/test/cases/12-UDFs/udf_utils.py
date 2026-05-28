"""Shared utilities for UDF test cases — cross-platform DLL/SO discovery."""
import os
import platform


def _get_project_root():
    """Walk upward from this file to find the project root (contains 'debug/' dir)."""
    path = os.path.dirname(os.path.realpath(__file__))
    while path and path != os.path.dirname(path):
        if os.path.isdir(os.path.join(path, "debug")):
            return path
        path = os.path.dirname(path)
    return path


def find_udf_lib(name, proj_path=None):
    """Find a UDF library named *name* under the build tree.

    On Windows looks for ``<name>.dll``, on Linux for ``lib<name>.so``.
    Prefers paths containing ``build``.
    Returns the absolute path string, or ``""`` if not found.
    """
    if proj_path is None:
        proj_path = _get_project_root()

    is_win = platform.system().lower() == "windows"
    filename = f"{name}.dll" if is_win else f"lib{name}.so"

    fallback = ""
    for root, _dirs, files in os.walk(proj_path):
        if filename in files:
            full = os.path.join(root, filename)
            if "build" in full:
                return full
            if not fallback:
                fallback = full
    return fallback
