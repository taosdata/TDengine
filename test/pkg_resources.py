# -*- coding: utf-8 -*-

# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""Minimal ``pkg_resources`` compatibility shim for the Python 3.12 test venv.

setuptools 82.0.0 removed the bundled ``pkg_resources`` package, so the
Windows-only ``wexpect`` dependency (pinned by the index at 4.0.0) fails at
import time: its ``wexpect/__init__.py`` performs a top-level
``import pkg_resources`` and later calls
``pkg_resources.require("wexpect")[0].version``.  The compatibility pin on
setuptools 80.9.0 was the previous workaround; it can no longer be used once
setuptools is upgraded past the advisories fixed in 84.1.0.

The Windows CI runs pytest as ``python -m pytest`` with the working directory
set to ``source/taos-community/test`` (see ``ci/run_windows_ci.py`` and
``ci/run_win_cases.py``), so this module is importable as ``pkg_resources``
from the test environment without changing the venv or the CI scripts.
It implements only the small API surface wexpect uses, on top of the
standard library's ``importlib.metadata``.  See also
``.gitlab/docker/tsdb-ci/python312-validation-20261026.md``.
"""

from importlib import metadata as _metadata

__all__ = [
    "DistributionNotFound",
    "RequirementParseError",
    "get_distribution",
    "require",
]

# Longest operators first so "==" is not shadowed by "<" or "=" handling.
_SPECIFIER_SEPARATORS = ("===", "==", "~=", "!=", "<=", ">=", "<", ">")


class ResolutionError(Exception):
    """Base error raised by this shim."""


class DistributionNotFound(ResolutionError):
    """Raised when a requested distribution is not installed."""


class RequirementParseError(ResolutionError):
    """Raised when a requirement string cannot be parsed."""


def _project_name(requirement):
    """Extract the project name from a PEP 508 style requirement string."""
    if not isinstance(requirement, str):
        raise RequirementParseError(f"unsupported requirement: {requirement!r}")
    name = requirement.split(";", 1)[0].split("[", 1)[0]
    for separator in _SPECIFIER_SEPARATORS:
        name = name.split(separator, 1)[0]
    name = name.strip()
    if not name:
        raise RequirementParseError(f"empty requirement: {requirement!r}")
    return name


class _Distribution:
    """``importlib.metadata.Distribution`` wrapper mirroring pkg_resources."""

    def __init__(self, distribution):
        self._distribution = distribution
        self.project_name = distribution.metadata["Name"] or distribution.name
        self.version = distribution.version

    def __getattr__(self, name):
        return getattr(self._distribution, name)


def get_distribution(dist):
    """Return the installed distribution for ``dist`` (name or requirement)."""
    name = dist if isinstance(dist, str) else getattr(dist, "project_name", str(dist))
    try:
        return _Distribution(_metadata.distribution(_project_name(name)))
    except _metadata.PackageNotFoundError as error:
        raise DistributionNotFound(str(error)) from error


def require(*requirements):
    """Return the installed distributions matching ``requirements``."""
    return [get_distribution(requirement) for requirement in requirements]
