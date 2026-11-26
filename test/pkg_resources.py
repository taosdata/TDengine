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

Shadowing order: that directory is ``sys.path[0]`` for those processes, so
this file is imported instead of any ``pkg_resources`` that may still be
installed (e.g. a developer venv with setuptools < 82).  The module is
deliberately scoped to the test process and must not be copied into
production code or packaged.  Only the small API surface used by the test
suite is implemented, on top of the standard library's ``importlib.metadata``.
The exception hierarchy is an approximation of the real ``pkg_resources`` one
(``RequirementParseError`` does not derive from ``ResolutionError`` there).
See also ``.gitlab/docker/tsdb-ci/python312-validation-20261026.md``.
"""

from importlib import metadata as _metadata

__all__ = [
    "DistributionNotFound",
    "RequirementParseError",
    "ResolutionError",
    "VersionConflict",
    "get_distribution",
    "require",
]

# Longest operators first so "==" is not shadowed by "<" or "=" handling.
_SPECIFIER_OPERATORS = ("===", "==", "~=", "!=", "<=", ">=", "<", ">")


class ResolutionError(Exception):
    """Base error raised by this shim."""


class DistributionNotFound(ResolutionError):
    """Raised when a requested distribution is not installed."""


class RequirementParseError(ResolutionError):
    """Raised when a requirement string cannot be parsed."""


class VersionConflict(ResolutionError):
    """Raised when an installed distribution does not satisfy a specifier."""


def _parse_requirement(requirement):
    """Split a PEP 508 style requirement into ``(project_name, specifier)``.

    Extras and environment markers are accepted and ignored.  ``specifier`` is
    the raw operator string (``""`` when unpinned), ready for
    ``packaging.specifiers.SpecifierSet``.
    """
    if not isinstance(requirement, str):
        raise RequirementParseError(f"unsupported requirement: {requirement!r}")
    base = requirement.split(";", 1)[0].strip()
    name, _, extras = base.partition("[")
    if extras:
        specifier = extras.partition("]")[2].strip()
    else:
        name, specifier = base, ""
    for operator in _SPECIFIER_OPERATORS:
        if operator in name:
            name, _, tail = name.partition(operator)
            specifier = operator + tail.strip()
            break
    name = name.strip()
    if not name:
        raise RequirementParseError(f"empty requirement: {requirement!r}")
    return name, specifier


def _check_specifier(project_name, version, specifier):
    """Raise ``VersionConflict`` when ``version`` violates ``specifier``."""
    if not specifier:
        return
    try:
        from packaging.specifiers import InvalidSpecifier, SpecifierSet
    except ImportError:  # packaging is a pytest dependency; tolerate minimal envs
        return
    try:
        matches = SpecifierSet(specifier).contains(version)
    except InvalidSpecifier as error:
        raise RequirementParseError(str(error)) from error
    if not matches:
        raise VersionConflict(
            f"{project_name} {version} does not satisfy {specifier!r}"
        )


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
    if isinstance(dist, str):
        name, specifier = _parse_requirement(dist)
    else:
        name = getattr(dist, "project_name", str(dist))
        specifier = ""
    try:
        distribution = _Distribution(_metadata.distribution(name))
    except _metadata.PackageNotFoundError as error:
        raise DistributionNotFound(str(error)) from error
    _check_specifier(distribution.project_name, distribution.version, specifier)
    return distribution


def require(*requirements):
    """Return the installed distributions matching ``requirements``."""
    return [get_distribution(requirement) for requirement in requirements]
