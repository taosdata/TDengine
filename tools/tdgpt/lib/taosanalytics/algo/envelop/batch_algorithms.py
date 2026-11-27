# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Discovery and parameter contract for user-provided batch envelope algorithms.

Plugins live under ``<custom_algo_dir>/batch`` and are imported through the
synthetic package built by :func:`lib.custom_algorithms.module_prefix`, so the
user directory is never added to ``sys.path``. A plugin is a class whose name
starts with ``_`` and which exposes:

* ``name`` -- the algorithm name accepted by the batch endpoint;
* ``build(ts, values, windows, params)`` -- returns ``(center, lower, upper)``.

Declaring the accepted parameters is optional. The schema is read from a sidecar
``<module>.json`` placed next to the plugin, or from a ``param_spec`` class
attribute; when neither exists the request parameters are passed through
without validation.

Plugins are intentionally kept out of ``ServiceRegistry.services``: the members
of that mapping are contracted to expose ``type`` and the ``get_*`` accessors
used by ``/list``, and a batch plugin does not implement them.
"""

import hashlib
import importlib
import inspect
import json
from pathlib import Path

from lib.log import AppLogger

# Category directory under ``custom_algo_dir`` holding the batch plugins.
KIND = "batch"

# The built-in pipeline. It stays the default algorithm and is never overridden
# by a plugin with the same name.
DEFAULT_ALGO = "golden"

_MISSING = object()

_TYPE_CHECKS = {
    "number": lambda value: isinstance(value, (int, float)) and not isinstance(value, bool),
    "integer": lambda value: isinstance(value, int) and not isinstance(value, bool),
    "boolean": lambda value: isinstance(value, bool),
    "string": lambda value: isinstance(value, str),
    "array": lambda value: isinstance(value, (list, tuple)),
}

_SPEC_KEYS = frozenset(
    ("type", "minimum", "maximum", "enum", "default", "required", "description")
)


class BatchAlgoError(Exception):
    """Base class for problems reported back to the caller."""


class BatchAlgoParamsError(BatchAlgoError):
    """The request parameters violate the declared schema."""


class ParamSpec:
    """One declared parameter, normalised from a sidecar or ``param_spec``."""

    __slots__ = ("name", "type", "minimum", "maximum", "enum", "default", "required", "description")

    def __init__(self, name, raw):
        self.name = name
        self.type = raw.get("type")
        self.minimum = raw.get("minimum")
        self.maximum = raw.get("maximum")
        self.enum = raw.get("enum")
        self.default = raw.get("default", _MISSING)
        self.required = bool(raw.get("required", False))
        self.description = raw.get("description", "")

    def describe(self):
        item = {"required": self.required}
        if self.type:
            item["type"] = self.type
        if self.minimum is not None:
            item["minimum"] = self.minimum
        if self.maximum is not None:
            item["maximum"] = self.maximum
        if self.enum is not None:
            item["enum"] = list(self.enum)
        if self.default is not _MISSING:
            item["default"] = self.default
        if self.description:
            item["description"] = self.description
        return item

    def check(self, algo, value):
        """Return the accepted value, or raise with the violated constraint."""
        if self.type:
            checker = _TYPE_CHECKS[self.type]
            if not checker(value):
                raise BatchAlgoParamsError(
                    f"parameter '{self.name}' of algorithm '{algo}' must be "
                    f"{self.type}, got {type(value).__name__}: {value!r}"
                )

        if self.enum is not None and value not in self.enum:
            raise BatchAlgoParamsError(
                f"parameter '{self.name}' of algorithm '{algo}' must be one of "
                f"{list(self.enum)}, got {value!r}"
            )

        if self.minimum is not None or self.maximum is not None:
            # `enum` and the numeric bounds are mutually exclusive in practice;
            # comparing a non-number here would raise TypeError instead of the
            # intended message, so the bound check is limited to numbers.
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return value
            if self.minimum is not None and value < self.minimum:
                raise BatchAlgoParamsError(
                    f"parameter '{self.name}' of algorithm '{algo}' must be >= "
                    f"{self.minimum}, got {value!r}"
                )
            if self.maximum is not None and value > self.maximum:
                raise BatchAlgoParamsError(
                    f"parameter '{self.name}' of algorithm '{algo}' must be <= "
                    f"{self.maximum}, got {value!r}"
                )

        return value


class BatchPlugin:
    """A discovered plugin: its class plus the metadata used for reporting."""

    __slots__ = ("name", "cls", "description", "version", "entry_sha256", "params", "source")

    def __init__(self, name, cls, description, version, entry_sha256, params, source):
        self.name = name
        self.cls = cls
        self.description = description
        self.version = version
        self.entry_sha256 = entry_sha256
        self.params = params
        self.source = source

    def describe(self):
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "entry_sha256": self.entry_sha256,
            "builtin": False,
            "params": {name: spec.describe() for name, spec in self.params.items()},
        }

    def trace(self):
        """Fields returned with every result so the code that produced it is known."""
        return {
            "algo": self.name,
            "version": self.version,
            "entry_sha256": self.entry_sha256,
        }


def resolve_params(plugin, raw):
    """Return the mapping handed to ``build()``.

    Unknown keys are refused rather than dropped: a silently ignored parameter
    looks accepted to the caller. Plugins that declare no schema get the request
    mapping unchanged, which is the pre-sidecar behaviour of "bring your own
    parameters".
    """
    if raw is None:
        # An omitted `config` still yields the declared defaults, otherwise a
        # plugin would receive an empty mapping where its schema defines one.
        raw = {}
    if not isinstance(raw, dict):
        raise BatchAlgoParamsError(
            f"'config' must be an object, got {type(raw).__name__}"
        )
    if not plugin.params:
        return dict(raw)

    unknown = sorted(set(raw) - set(plugin.params))
    if unknown:
        raise BatchAlgoParamsError(
            f"unknown parameter(s) for algorithm '{plugin.name}': {', '.join(unknown)}; "
            f"accepted: {', '.join(sorted(plugin.params))}"
        )

    resolved = {}
    for name, spec in plugin.params.items():
        if name in raw:
            resolved[name] = spec.check(plugin.name, raw[name])
        elif spec.default is not _MISSING:
            resolved[name] = spec.default
        elif spec.required:
            raise BatchAlgoParamsError(
                f"missing required parameter '{name}' for algorithm '{plugin.name}'"
            )
    return resolved


def discover_plugins(directory, namespace, reserved=()):
    """Import the modules under ``directory`` and return ``{name: BatchPlugin}``.

    ``namespace`` is the synthetic package returned by ``module_prefix`` for the
    custom algorithm root, so modules are imported as
    ``<namespace>.batch.<module>`` and relative imports keep working.
    """
    directory = Path(directory)
    plugins = {}
    taken = {str(name) for name in reserved}

    if not directory.is_dir():
        return plugins

    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.suffix != ".py" or path.name == "__init__.py":
            continue

        module = _import_module(namespace, path)
        if module is None:
            continue

        for class_name, cls in inspect.getmembers(module, inspect.isclass):
            if not class_name.startswith("_") or not class_name.strip("_"):
                continue
            if cls.__module__ != module.__name__:
                AppLogger.warning(
                    "class %s in %s is imported from another module, skipping",
                    class_name,
                    path,
                )
                continue

            plugin = _make_plugin(cls, path)
            if plugin is None:
                continue
            if plugin.name in taken:
                AppLogger.error(
                    "batch algorithm '%s' from %s conflicts with an existing "
                    "algorithm, skipping",
                    plugin.name,
                    path,
                )
                continue

            taken.add(plugin.name)
            plugins[plugin.name] = plugin
            AppLogger.info(
                "load batch algorithm:%s from %s (version=%s)",
                plugin.name,
                path,
                plugin.version,
            )

    return plugins


def _import_module(namespace, path):
    module_name = f"{namespace}.{KIND}.{path.stem}"
    try:
        return importlib.import_module(module_name)
    except Exception as exc:
        AppLogger.error(
            "failed to import batch algorithm module %s (%s): %s, skipping",
            module_name,
            path,
            exc,
        )
        return None


def _make_plugin(cls, path):
    name = getattr(cls, "name", None)
    if not isinstance(name, str) or not name.strip():
        AppLogger.warning(
            "batch algorithm class %s in %s has no usable 'name' attribute, skipping",
            cls.__name__,
            path,
        )
        return None

    if not callable(getattr(cls, "build", None)):
        AppLogger.warning(
            "batch algorithm '%s' in %s has no callable build(), skipping", name, path
        )
        return None

    if hasattr(cls, "type"):
        # `type` belongs to the `ServiceRegistry.services` contract, so the class
        # probably inherits a service base class. It is still registered here,
        # where `/list` does not look, and flagged so the inheritance is visible.
        AppLogger.warning(
            "batch algorithm '%s' in %s carries a 'type' attribute, which belongs "
            "to the service registry contract; the plugin is registered for the "
            "batch endpoint only and is not listed by /list",
            name,
            path,
        )

    sidecar, sidecar_error = _read_sidecar(path)
    if sidecar_error:
        AppLogger.error(
            "ignoring sidecar for batch algorithm '%s' (%s): %s",
            name,
            path.with_suffix(".json"),
            sidecar_error,
        )

    description = (
        (sidecar.get("description") if sidecar else None)
        or getattr(cls, "description", "")
        or ""
    )
    version = sidecar.get("version") if sidecar else None
    digest = _entry_digest(path)
    declared = sidecar.get("entry_sha256") if sidecar else None
    if declared and declared != digest:
        AppLogger.warning(
            "sidecar declares entry_sha256 %s for batch algorithm '%s' but the "
            "file is %s",
            declared,
            name,
            digest,
        )

    raw_params = (sidecar.get("params") if sidecar else None)
    if raw_params is None:
        raw_params = getattr(cls, "param_spec", None)
    params = _normalise_params(name, raw_params, path)

    return BatchPlugin(
        name=name.strip(),
        cls=cls,
        description=description,
        version=version,
        entry_sha256=digest,
        params=params,
        source=str(path),
    )


def _read_sidecar(path):
    """Return ``(mapping, error)``; a missing sidecar is not an error."""
    sidecar_path = path.with_suffix(".json")
    if not sidecar_path.is_file():
        return {}, None
    try:
        with sidecar_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError) as exc:
        return {}, str(exc)
    if not isinstance(data, dict):
        return {}, f"expected a JSON object, got {type(data).__name__}"
    return data, None


def _entry_digest(path):
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        AppLogger.warning("unable to hash %s: %s", path, exc)
        return None


def _normalise_params(algo, raw, path):
    """Validate the declared schema itself; a broken schema means no schema."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        AppLogger.error(
            "parameter schema for batch algorithm '%s' in %s must be an object, "
            "got %s; parameters will not be validated",
            algo,
            path,
            type(raw).__name__,
        )
        return {}

    params = {}
    for name, spec in raw.items():
        if not isinstance(name, str) or not name:
            AppLogger.error(
                "parameter name %r for batch algorithm '%s' in %s is not a "
                "string, parameters will not be validated",
                name,
                algo,
                path,
            )
            return {}
        if not isinstance(spec, dict):
            AppLogger.error(
                "parameter '%s' of batch algorithm '%s' in %s must be declared "
                "as an object, parameters will not be validated",
                name,
                algo,
                path,
            )
            return {}

        unknown = sorted(set(spec) - _SPEC_KEYS)
        if unknown:
            AppLogger.error(
                "parameter '%s' of batch algorithm '%s' in %s uses unsupported "
                "key(s) %s, parameters will not be validated",
                name,
                algo,
                path,
                ", ".join(unknown),
            )
            return {}

        declared_type = spec.get("type")
        if declared_type is not None and declared_type not in _TYPE_CHECKS:
            AppLogger.error(
                "parameter '%s' of batch algorithm '%s' in %s has unsupported "
                "type %r, parameters will not be validated",
                name,
                algo,
                path,
                declared_type,
            )
            return {}

        enum = spec.get("enum")
        if enum is not None and not isinstance(enum, (list, tuple)):
            AppLogger.error(
                "parameter '%s' of batch algorithm '%s' in %s declares 'enum' "
                "that is not a list, parameters will not be validated",
                name,
                algo,
                path,
            )
            return {}

        minimum = spec.get("minimum")
        maximum = spec.get("maximum")
        for label, bound in (("minimum", minimum), ("maximum", maximum)):
            # A string bound (a frequent sidecar typo put in quotes) would reach
            # `ParamSpec.check()` and raise TypeError from `value < bound`, which
            # is not a BatchAlgoParamsError and would escape the handler as a 500.
            if bound is not None and (
                isinstance(bound, bool) or not isinstance(bound, (int, float))
            ):
                AppLogger.error(
                    "parameter '%s' of batch algorithm '%s' in %s declares "
                    "'%s' that is not a number (%r), parameters will not be "
                    "validated",
                    name,
                    algo,
                    path,
                    label,
                    bound,
                )
                return {}

        if minimum is not None and maximum is not None and minimum > maximum:
            AppLogger.error(
                "parameter '%s' of batch algorithm '%s' in %s declares 'minimum' "
                "%r greater than 'maximum' %r, parameters will not be validated",
                name,
                algo,
                path,
                minimum,
                maximum,
            )
            return {}

        param = ParamSpec(name, spec)
        if param.default is not _MISSING:
            # The default is handed to build() without going through check(), so
            # it has to be checked once here: an invalid default would otherwise
            # be accepted silently at request time.
            try:
                param.check(algo, param.default)
            except Exception as exc:  # noqa: BLE001 - a schema that cannot be
                # checked cannot be trusted; the policy is to disable validation,
                # never to fail the request or the plugin load.
                AppLogger.error(
                    "parameter '%s' of batch algorithm '%s' in %s declares an "
                    "invalid 'default' (%s), parameters will not be validated",
                    name,
                    algo,
                    path,
                    exc,
                )
                return {}

            try:
                json.dumps(param.default)
            except (TypeError, ValueError) as exc:
                # The discovery endpoint serialises defaults as part of the
                # schema, so a default that cannot be encoded would turn
                # /batch/algos into a 500.
                AppLogger.error(
                    "parameter '%s' of batch algorithm '%s' in %s declares a "
                    "'default' that cannot be serialised (%s), parameters will "
                    "not be validated",
                    name,
                    algo,
                    path,
                    exc,
                )
                return {}

        params[name] = param

    return params
