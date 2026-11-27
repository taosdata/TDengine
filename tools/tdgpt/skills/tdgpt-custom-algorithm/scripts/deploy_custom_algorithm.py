#!/usr/bin/env python3

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
"""Deploy a user-defined algorithm to TDgpt.

Supported algorithm types:

  ad             - anomaly detection under <custom_algo_dir>/ad
  fc             - forecast under <custom_algo_dir>/fc
  classification - classification under <custom_algo_dir>/classification
  regression     - regression under <custom_algo_dir>/regression
  batch          - batch envelope plugin under <custom_algo_dir>/batch

Supported deployment forms:

  local   - TDgpt installed natively on this host
  docker  - TDgpt running in a Docker container
  remote  - TDgpt installed natively on a host reachable over SSH
            (optionally a Docker container on that host with --container)

Standard library only.
"""

import argparse
import ast
import base64
import hashlib
import json
import math
import os
import platform
import posixpath
import re
import shlex
import shutil
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

EXIT_OK = 0
EXIT_VERIFY = 1
EXIT_DEPS = 2
EXIT_PREFLIGHT = 3

ALGORITHM_TYPES = ("ad", "fc", "classification", "regression", "batch")
SERVICE_TYPES = {
    "ad": "anomaly-detection",
    "fc": "forecast",
    "classification": "classification",
    "regression": "regression",
}
DEFAULT_SCHEMA = [["ts", "TIMESTAMP", 8], ["val", "DOUBLE", 8]]
DEFAULT_CUSTOM_DIR = "/var/lib/taos/taosanode/custom"
PYTHON_CANDIDATES = [
    "/var/lib/taos/taosanode/venv/bin/python3",
    "/usr/local/taos/taosanode/venv/bin/python3",
    "/usr/local/taos/taosanode/venv/bin/python",
]
CONFIG_CANDIDATES = [
    "/etc/taos/taosanode.config.py",
    "/usr/local/taos/taosanode/cfg/taosanode.config.py",
]
INTERNAL_MODULES = {"lib"}
PIP_NAMES = {
    "sklearn": "scikit-learn",
    "cv2": "opencv-python",
    "PIL": "pillow",
    "yaml": "PyYAML",
    "dateutil": "python-dateutil",
    "bs4": "beautifulsoup4",
    "Crypto": "pycryptodome",
    "OpenSSL": "pyOpenSSL",
    "pkg_resources": "setuptools",
    "mpl_toolkits": "matplotlib",
    "alibi_detect": "alibi-detect",
    "skmultiflow": "scikit-multiflow",
}

RESOLVE_CODE = r"""
import json, os, runpy, sys
conf_path = os.environ.get('TDGPT_CONF') or ''
conf = {}
read_error = None
if conf_path and os.path.isfile(conf_path):
    try:
        conf = runpy.run_path(conf_path)
    except Exception as exc:
        read_error = str(exc)
override = os.environ.get('TDGPT_CUSTOM_ALGO_DIR') or ''
if override:
    custom = override
elif 'custom_algo_dir' in conf:
    custom = conf['custom_algo_dir']
elif 'data_dir' in conf:
    custom = os.path.join(str(conf['data_dir']), 'custom')
else:
    custom = %s
print(json.dumps({
    'custom_dir': str(custom),
    'config': conf_path,
    'config_error': read_error,
    'env_override': override,
    'python': sys.executable,
    'version': sys.version.split()[0],
}))
"""

DEPS_CODE = r"""
import importlib.util, json, os, sys
# `python -c` leaves '' (the caller's cwd) on sys.path. Drop empty and
# relative entries so the probe only sees the target venv and explicit
# target-side paths, not the execution host's working directory.
sys.path[:] = [p for p in sys.path if p and os.path.isabs(p)]
for path in %s:
    if path and path not in sys.path:
        sys.path.insert(0, path)
modules = %s
missing = []
for name in modules:
    try:
        found = importlib.util.find_spec(name) is not None
    except BaseException:
        found = False
    if not found:
        missing.append(name)
print(json.dumps({
    'missing': missing,
    'python': sys.executable,
    'version': sys.version.split()[0],
}))
"""


class DeployError(RuntimeError):
    """Preflight or deployment failure."""

    exit_code = EXIT_PREFLIGHT


class MissingDependencies(DeployError):
    """Dependencies are missing from the TDgpt main venv."""

    exit_code = EXIT_DEPS

    def __init__(self, modules, packages, hint):
        self.modules = modules
        self.packages = packages
        self.hint = hint
        message = "missing modules in the TDgpt main venv: %s\ninstall command: %s" % (
            ", ".join(modules),
            hint,
        )
        super().__init__(message)


class Runner:
    """Run commands on a local host, inside a container, or over SSH."""

    def __init__(self, mode, container=None, ssh=None, sudo=False):
        self.mode = mode
        self.container = container
        self.ssh = ssh
        self.sudo = sudo

    def _exec(self, argv, input_text=None, timeout=120, check=False):
        try:
            proc = subprocess.run(
                argv,
                input=input_text,
                text=True,
                capture_output=True,
                timeout=timeout,
            )
        except FileNotFoundError as exc:
            raise DeployError("required command not found: %s" % exc)
        except subprocess.TimeoutExpired:
            raise DeployError("command timed out after %ss: %s" % (timeout, " ".join(argv)))
        if check and proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise DeployError(
                "command failed (%s): %s\n%s" % (proc.returncode, " ".join(argv), detail)
            )
        return proc

    def run(self, script, input_text=None, timeout=120, check=False):
        if self.mode == "local":
            # Elevate the shell, not the first command: redirections and the
            # commands after && must also run with elevated privileges.
            argv = (
                ["sudo", "-n", "sh", "-c", script]
                if self.sudo
                else ["sh", "-c", script]
            )
        elif self.mode == "docker":
            argv = ["docker", "exec", "-i", self.container, "sh", "-c", script]
            if self.sudo:
                argv = ["sudo", "-n"] + argv
        elif self.mode == "remote":
            if self.container:
                remote = "docker exec -i %s sh -c %s" % (
                    shlex.quote(self.container),
                    shlex.quote(script),
                )
                if self.sudo:
                    remote = "sudo -n " + remote
            else:
                remote = script
                if self.sudo:
                    remote = "sudo -n sh -c %s" % shlex.quote(script)
            argv = ["ssh", "-o", "BatchMode=yes", self.ssh, remote]
        else:
            raise DeployError("unknown deploy mode: %s" % self.mode)
        return self._exec(argv, input_text=input_text, timeout=timeout, check=check)

    def host(self, command, input_text=None, timeout=120, check=False):
        """Run a command on the Docker host filesystem or the local host."""
        if self.mode == "remote":
            remote = command
            if self.sudo:
                remote = "sudo -n sh -c %s" % shlex.quote(command)
            argv = ["ssh", "-o", "BatchMode=yes", self.ssh, remote]
        else:
            argv = (
                ["sudo", "-n", "sh", "-c", command]
                if self.sudo
                else ["sh", "-c", command]
            )
        return self._exec(
            argv, input_text=input_text, timeout=timeout, check=check
        )

    def exists(self, path):
        proc = self.run("test -e %s && echo yes || echo no" % shlex.quote(path))
        return proc.stdout.strip() == "yes"

    def python(self, code, python, env=None, timeout=180, check=False):
        payload = base64.b64encode(code.encode("utf-8")).decode("ascii")
        expr = "import base64;exec(base64.b64decode('%s').decode())" % payload
        prefix = ""
        for key, value in (env or {}).items():
            prefix += "%s=%s " % (key, shlex.quote(str(value)))
        # -I keeps the caller's cwd and user site-packages off sys.path, so a
        # same-named local file cannot shadow the target venv or the stdlib.
        return self.run(
            "%s%s -I -c \"%s\"" % (prefix, shlex.quote(python), expr),
            timeout=timeout,
            check=check,
        )

    def sha256(self, path, host=False):
        run = self.host if host else self.run
        proc = run("sha256sum %s 2>/dev/null | cut -d' ' -f1" % shlex.quote(path))
        return proc.stdout.strip()


def normalize_url(url):
    url = url.strip().rstrip("/")
    if not re.match(r"^https?://", url):
        url = "http://" + url
    return url


def http_json(base, path, payload=None, timeout=30, insecure=False):
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        base + path,
        data=body,
        method="POST" if body is not None else "GET",
    )
    if body is not None:
        request.add_header("Content-Type", "application/json")
    context = ssl._create_unverified_context() if insecure else None
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise DeployError("HTTP %s from %s%s: %s" % (exc.code, base, path, exc.reason))
    except urllib.error.URLError as exc:
        raise DeployError("cannot reach %s%s: %s" % (base, path, exc.reason))


def parse_last_json(text):
    for line in reversed((text or "").strip().splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise DeployError("target python did not return JSON:\n%s" % (text or "").strip())


def parse_env(text):
    result = {}
    try:
        tokens = shlex.split(text or "")
    except ValueError:
        tokens = (text or "").split()
    for token in tokens:
        if "=" not in token:
            continue
        key, _, value = token.partition("=")
        result[key] = value
    return result


def detect_mode(args):
    if args.deploy_mode != "auto":
        return args.deploy_mode
    if args.ssh:
        return "remote"
    if args.container:
        return "docker"
    if shutil.which("docker"):
        proc = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Image}}"],
            text=True,
            capture_output=True,
        )
        matches = []
        for line in proc.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            name, image = parts
            if re.search(r"tdgpt|taosanode", image, re.IGNORECASE):
                matches.append(name)
        if len(matches) == 1:
            args.container = matches[0]
            return "docker"
        if len(matches) > 1:
            raise DeployError(
                "multiple TDgpt containers found (%s); pass --container"
                % ", ".join(matches)
            )
    return "local"


def pick_service(runner, preferred):
    if preferred:
        return preferred
    for unit in ("taosanoded", "taosanode"):
        proc = runner.run(
            "systemctl list-unit-files %s.service >/dev/null 2>&1 && echo yes || echo no"
            % unit
        )
        if proc.stdout.strip() == "yes":
            return unit
    return None


def service_env(runner, unit):
    if not unit:
        return {}
    proc = runner.run(
        "systemctl show %s -p Environment --value 2>/dev/null || true" % shlex.quote(unit)
    )
    return parse_env(proc.stdout)


def container_env(runner, container):
    if not container:
        return {}
    proc = runner.host(
        "docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' %s"
        % shlex.quote(container)
    )
    return parse_env(proc.stdout)


def inspect_mounts(runner, container):
    proc = runner.host(
        "docker inspect --format '{{json .Mounts}}' %s" % shlex.quote(container)
    )
    if proc.returncode != 0:
        raise DeployError(
            "failed to inspect container %s: %s"
            % (container, (proc.stderr or proc.stdout or "").strip())
        )
    try:
        entries = json.loads(proc.stdout or "[]")
    except ValueError:
        raise DeployError("failed to parse docker inspect output for %s" % container)
    return entries if isinstance(entries, list) else []


def find_mount(mounts, custom_dir):
    """Return the most specific mount whose destination contains custom_dir."""
    best = None
    best_length = -1
    for entry in mounts:
        destination = (entry.get("Destination") or "").rstrip("/")
        if not destination:
            continue
        if custom_dir == destination or custom_dir.startswith(destination + "/"):
            if len(destination) > best_length:
                best = entry
                best_length = len(destination)
    return best


def resolve_deploy_location(runner, runtime, container, allow_ephemeral):
    """Decide where the algorithm file must be written.

    For Docker, a file only survives container recreation when it lives on a
    mount. Bind mounts are written from the host side; named volumes are
    written through ``docker exec``. An unmounted container path is refused
    unless ``--allow-ephemeral`` is set.
    """
    if not container:
        return {
            "kind": "native",
            "custom_dir": runtime["custom_dir"],
            "container_custom_dir": None,
            "mount": None,
        }

    custom_dir = runtime["custom_dir"].rstrip("/")
    mounts = inspect_mounts(runner, container)
    mount = find_mount(mounts, custom_dir)

    if mount is None:
        if not allow_ephemeral:
            raise DeployError(
                "custom_algo_dir %s is not backed by a Docker mount. A file "
                "written inside the container is lost when the container is "
                "recreated. Add a bind mount, for example:\n"
                "  docker run -v /data/tdgpt-custom:%s ...\n"
                "then redeploy. Use --allow-ephemeral only for a throwaway test."
                % (custom_dir, custom_dir)
            )
        print(
            "[warn] --allow-ephemeral: %s has no mount; the algorithm will be "
            "lost when the container is recreated" % custom_dir
        )
        return {
            "kind": "container",
            "custom_dir": custom_dir,
            "container_custom_dir": custom_dir,
            "mount": None,
        }

    mount_type = (mount.get("Type") or "").lower()
    destination = (mount.get("Destination") or "").rstrip("/")
    source = (mount.get("Source") or "").rstrip("/")

    if mount_type == "bind" and source:
        relative = posixpath.relpath(custom_dir, destination)
        host_dir = source if relative in (".", "") else posixpath.join(source, relative)
        print(
            "[check] bind mount: container %s <- host %s" % (custom_dir, host_dir)
        )
        print(
            "[check] deploy target is the host side of the bind mount, so a "
            "recreated container keeps the algorithm"
        )
        return {
            "kind": "host",
            "custom_dir": host_dir,
            "container_custom_dir": custom_dir,
            "mount": mount,
        }

    print(
        "[check] mount type=%s destination=%s: writing through docker exec; the "
        "data persists in the mount" % (mount_type or "unknown", destination)
    )
    return {
        "kind": "container",
        "custom_dir": custom_dir,
        "container_custom_dir": custom_dir,
        "mount": mount,
    }


def resolve_config_path(runner, explicit):
    if explicit:
        if not runner.exists(explicit):
            raise DeployError("configuration file not found on target: %s" % explicit)
        return explicit
    for candidate in CONFIG_CANDIDATES:
        if runner.exists(candidate):
            return candidate
    return ""


def resolve_runtime(runner, args, override):
    python = args.venv_python
    if not python:
        for candidate in PYTHON_CANDIDATES:
            if runner.exists(candidate):
                python = candidate
                break
    if not python:
        raise DeployError(
            "TDgpt main venv python not found; pass --venv-python with the "
            "interpreter used by the service"
        )
    if not runner.exists(python):
        raise DeployError("venv python does not exist on target: %s" % python)

    config = resolve_config_path(runner, args.config)
    env = {}
    if config:
        env["TDGPT_CONF"] = config
    if override:
        env["TDGPT_CUSTOM_ALGO_DIR"] = override

    proc = runner.python(RESOLVE_CODE % json.dumps(DEFAULT_CUSTOM_DIR), python, env=env, check=True)
    data = parse_last_json(proc.stdout)
    custom_dir = str(data.get("custom_dir") or "")
    if not custom_dir.startswith("/"):
        raise DeployError("custom_algo_dir must be an absolute path: %r" % custom_dir)
    if data.get("config_error"):
        raise DeployError(
            "failed to read configuration %s: %s" % (config, data["config_error"])
        )

    normalized = custom_dir.rstrip("/")
    if normalized == "/lib" or normalized.endswith("/lib"):
        raise DeployError(
            "custom_algo_dir points into the TDgpt program lib/ directory: %s"
            % custom_dir
        )
    if (
        normalized.startswith("/usr/local/taos/taosanode")
        and "/data/" not in normalized + "/"
        and not override
    ):
        print(
            "[warn] custom_algo_dir is inside the program tree and not under data/: %s"
            % custom_dir
        )

    return {
        "python": python,
        "custom_dir": custom_dir,
        "config": config,
        "env_override": override,
        "version": data.get("version"),
    }


def base_name(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def node_name(node):
    """Return the declared string value of a class attribute ``name``."""
    for stmt in node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "name":
                    if isinstance(stmt.value, ast.Constant) and isinstance(
                        stmt.value.value, str
                    ):
                        return stmt.value.value
        elif (
            isinstance(stmt, ast.AnnAssign)
            and isinstance(stmt.target, ast.Name)
            and stmt.target.id == "name"
        ):
            if isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
                return stmt.value.value
    return None


def method_names(node):
    return {
        stmt.name
        for stmt in node.body
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def detect_algorithm_type(tree):
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        bases = [base_name(base) for base in node.bases]
        if any("AnomalyDetectionService" in name for name in bases):
            return "ad"
        if any("ForecastService" in name for name in bases):
            return "fc"
        if any("ClassificationService" in name for name in bases):
            return "classification"
        if any("RegressionService" in name for name in bases):
            return "regression"
        if "build" in method_names(node) and node_name(node):
            return "batch"
    return None


def protocol_classes(tree, algorithm_type):
    result = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        bases = [base_name(base) for base in node.bases]
        methods = method_names(node)
        if algorithm_type == "ad":
            if any("AnomalyDetectionService" in name for name in bases):
                result.append((node, bases, methods))
        elif algorithm_type == "fc":
            if any("ForecastService" in name for name in bases):
                result.append((node, bases, methods))
        elif algorithm_type == "classification":
            if any("ClassificationService" in name for name in bases):
                result.append((node, bases, methods))
        elif algorithm_type == "regression":
            if any("RegressionService" in name for name in bases):
                result.append((node, bases, methods))
        elif algorithm_type == "batch":
            if "build" in methods and node_name(node):
                result.append((node, bases, methods))
    return result


def validate_source(path, model_name, requested_type):
    text = Path(path).read_text(encoding="utf-8")
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as exc:
        raise DeployError("script has a syntax error: %s" % exc)

    stem = Path(path).stem
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", stem):
        raise DeployError(
            "file name %r is not a valid Python module name; rename the file" % Path(path).name
        )
    if stem == "__init__":
        raise DeployError("algorithm file must not be __init__.py")

    detected = detect_algorithm_type(tree)
    if requested_type != "auto":
        if detected and detected != requested_type:
            raise DeployError(
                "script looks like a %r algorithm but --algorithm-type is %r"
                % (detected, requested_type)
            )
        algorithm_type = requested_type
    else:
        if detected is None:
            raise DeployError(
                "cannot detect the algorithm type. Expected a class inheriting "
                "AbstractAnomalyDetectionService, AbstractForecastService, "
                "AbstractClassificationService or AbstractRegressionService, or a "
                "class with build(ts, values, windows, params) for batch. "
                "Pass --algorithm-type explicitly."
            )
        algorithm_type = detected

    if stem != model_name:
        print(
            "[warn] file name %r differs from algorithm name %r; this works but "
            "makes the deployed file harder to find" % (stem, model_name)
        )

    candidates = protocol_classes(tree, algorithm_type)
    if not candidates:
        expectations = {
            "ad": "a module-level class subclassing AbstractAnomalyDetectionService",
            "fc": "a module-level class subclassing AbstractForecastService",
            "classification": "a module-level class subclassing AbstractClassificationService",
            "regression": "a module-level class subclassing AbstractRegressionService",
            "batch": "a module-level class with a name attribute and build(ts, values, windows, params)",
        }
        raise DeployError(
            "no %s was found in %s" % (expectations[algorithm_type], path)
        )

    problems = []
    for node, bases, methods in candidates:
        if not node.name.startswith("_"):
            problems.append("class %s must start with '_'" % node.name)

        declared = node_name(node)
        if declared is None:
            problems.append("class %s does not define a string 'name'" % node.name)
        elif declared != model_name:
            problems.append(
                "class %s declares name=%r, expected %r"
                % (node.name, declared, model_name)
            )

        if algorithm_type in ("ad", "classification", "regression") and "execute" not in methods:
            problems.append("class %s must define execute()" % node.name)
        if algorithm_type == "fc" and not ({"execute", "_fit_model"} & methods):
            problems.append(
                "class %s must define execute() or _fit_model()" % node.name
            )
        if algorithm_type == "batch" and "build" not in methods:
            problems.append(
                "class %s must define build(ts, values, windows, params)" % node.name
            )
        if algorithm_type == "batch" and any("Service" in name for name in bases):
            print(
                "[warn] batch class %s inherits a service base class; batch plugins "
                "should be standalone classes" % node.name
            )

        init = next(
            (
                stmt
                for stmt in node.body
                if isinstance(stmt, ast.FunctionDef) and stmt.name == "__init__"
            ),
            None,
        )
        if init is not None and init.args.vararg is None:
            positional = init.args.posonlyargs + init.args.args
            params = positional[1:]
            defaults = len(init.args.defaults)
            required = params[: len(params) - defaults] if defaults else params
            if required:
                problems.append(
                    "class %s __init__ requires arguments (%s); the registry "
                    "instantiates it with no arguments"
                    % (node.name, ", ".join(arg.arg for arg in required))
                )

    if problems:
        raise DeployError("\n".join(problems))
    return text, algorithm_type


def collect_imports(text, source_path):
    tree = ast.parse(text)
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                continue
            if node.module:
                modules.add(node.module.split(".")[0])

    result = []
    for module in sorted(modules):
        if module in INTERNAL_MODULES or module == "__future__":
            continue
        if module == source_path.stem:
            continue
        result.append(module)
    return result


def bundled_module_names(source_path, asset_plans):
    """Return helper module names bundled next to the entry or via --asset."""
    names = set()
    parent = source_path.parent
    if parent.is_dir():
        for entry in parent.iterdir():
            if entry.is_dir():
                names.add(entry.name)
            elif entry.suffix == ".py":
                names.add(entry.stem)
    for asset, _kind in asset_plans:
        if asset.is_dir():
            names.add(asset.name)
        elif asset.suffix == ".py":
            names.add(asset.stem)
    names.discard("__init__")
    names.discard(source_path.stem)
    return names


def collect_bundled_imports(source_path, text, asset_plans, into=None):
    """Collect absolute imports of names bundled next to the source or assets.

    The result is a candidate list. Whether an import is actually invalid is
    decided after the target interpreter check: a name that is importable from
    site-packages or the standard library is a legitimate absolute import.
    """
    names = bundled_module_names(source_path, asset_plans)
    findings = into if into is not None else {}
    if not names:
        return findings

    tree = ast.parse(text)
    for node in ast.walk(tree):
        candidates = []
        if isinstance(node, ast.Import):
            candidates = [alias.name.split(".")[0] for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level or not node.module:
                continue
            candidates = [node.module.split(".")[0]]
        for top in candidates:
            if top in names and top not in INTERNAL_MODULES:
                findings.setdefault(top, set()).add(
                    "%s:%d" % (source_path, node.lineno)
                )
    return findings


def host_install_hint(runner, command):
    if runner.mode == "docker":
        return "docker exec %s %s" % (runner.container, command)
    if runner.mode == "remote":
        if runner.container:
            return "ssh %s 'docker exec %s %s'" % (runner.ssh, runner.container, command)
        return "ssh %s %s" % (runner.ssh, command)
    if hasattr(os, "geteuid") and os.geteuid() != 0 and not runner.sudo:
        return "sudo " + command
    return command


def check_dependencies(
    runner,
    runtime,
    modules,
    bundled_imports,
    install_missing,
    assume_yes,
):
    if not modules:
        print("[check] no third-party imports detected")
        return

    # The loader imports custom algorithms through a synthetic package and
    # never adds custom_algo_dir to sys.path. Only the TDgpt install root is
    # importable here, for `import lib`; adding custom_dir would produce a
    # false "resolved" result for leftover helper modules and defer the
    # failure until the post-deployment verification step.
    extra_paths = ["/usr/local/taos/taosanode"]
    code = DEPS_CODE % (json.dumps(extra_paths), json.dumps(modules))
    proc = runner.python(code, runtime["python"], check=True)
    data = parse_last_json(proc.stdout)
    missing = data.get("missing") or []

    bundled_missing = [name for name in missing if name in bundled_imports]
    if bundled_missing:
        details = []
        for name in bundled_missing:
            for location in sorted(bundled_imports[name]):
                details.append("  %s imports %s" % (location, name))
        raise DeployError(
            "absolute import of a bundled helper was found, and it is not "
            "importable in the TDgpt main venv:\n%s\n"
            "TDgpt imports this file as an isolated submodule, so "
            "custom_algo_dir is not on sys.path. Use a relative import "
            "(for example `from .my_algo_impl.user_model import load_model`) "
            "and deploy the helper with --asset." % "\n".join(details)
        )

    if not missing:
        print(
            "[check] all %d imported module(s) resolve in the main venv (python %s)"
            % (len(modules), data.get("version"))
        )
        return

    packages = sorted({PIP_NAMES.get(module, module) for module in missing})
    install = "%s -m pip install %s" % (
        shlex.quote(runtime["python"]),
        " ".join(shlex.quote(package) for package in packages),
    )
    hint = host_install_hint(runner, install)

    print("[check] missing modules: %s" % ", ".join(missing))
    print("[check] pip packages to install: %s" % ", ".join(packages))
    print("[action] install them into the TDgpt main venv, not the TSFM venv or another python:")
    print("         " + hint)

    if not install_missing:
        raise MissingDependencies(missing, packages, hint)
    if not assume_yes:
        raise DeployError(
            "refusing to install packages without --yes; rerun with "
            "--install-missing --yes after the user agrees"
        )

    print("[action] installing missing packages")
    proc = runner.run(install, timeout=1800)
    if proc.returncode != 0:
        raise DeployError(
            "pip install failed (%s):\n%s" % (proc.returncode, (proc.stderr or proc.stdout).strip())
        )

    proc = runner.python(code, runtime["python"], check=True)
    remaining = parse_last_json(proc.stdout).get("missing") or []
    if remaining:
        raise MissingDependencies(remaining, packages, hint)
    print("[check] dependencies installed and importable")


SKIP_ASSET_DIRS = {"__pycache__", ".git", ".hg", ".svn", ".idea", ".vscode"}
SKIP_ASSET_SUFFIXES = (".pyc", ".pyo")


def plan_assets(source_path, assets):
    """Validate ``--asset`` inputs and return (path, kind) pairs."""
    plans = []
    seen = set()
    for value in assets:
        path = Path(value).resolve()
        if not path.exists():
            raise DeployError("asset not found: %s" % path)
        name = path.name
        if not name:
            raise DeployError("asset has no file name: %s" % path)
        if name in seen:
            raise DeployError(
                "duplicate asset name %r; rename one of the assets" % name
            )
        seen.add(name)
        if path.is_file():
            if name == source_path.name:
                raise DeployError(
                    "asset file %s conflicts with the entry file name" % path
                )
            plans.append((path, "file"))
        elif path.is_dir():
            plans.append((path, "dir"))
        else:
            raise DeployError("asset is neither a file nor a directory: %s" % path)
    return plans


def iter_py_files(root):
    for current, dirs, files in os.walk(root):
        dirs[:] = sorted(name for name in dirs if name not in SKIP_ASSET_DIRS)
        for name in sorted(files):
            if name.endswith(SKIP_ASSET_SUFFIXES):
                continue
            if name.endswith(".py"):
                yield Path(current) / name


def write_bytes(run, path, data):
    """Write bytes on the target through base64, supporting binary assets."""
    temporary = path + ".tmp-" + uuid.uuid4().hex
    parent = posixpath.dirname(path) or "."
    encoded = base64.b64encode(data).decode("ascii")
    run("mkdir -p %s" % shlex.quote(parent), check=True)
    run(
        "base64 -d > %s && chmod 0640 %s && mv -f %s %s"
        % (
            shlex.quote(temporary),
            shlex.quote(temporary),
            shlex.quote(temporary),
            shlex.quote(path),
        ),
        input_text=encoded + "\n",
        check=True,
    )


def deploy_assets(run, type_dir, custom_dir, algorithm_type, plans):
    backup_root = posixpath.join(custom_dir, ".deploy-backups")
    stamp = time.strftime("%Y%m%d%H%M%S")
    deployed = []
    for path, kind in plans:
        target = posixpath.join(type_dir, path.name)
        exists = (
            run("test -e %s && echo yes || echo no" % shlex.quote(target))
            .stdout.strip()
            == "yes"
        )

        if kind == "file":
            if exists:
                backup = posixpath.join(
                    backup_root, "%s-%s.%s" % (algorithm_type, path.name, stamp)
                )
                run(
                    "mkdir -p %s && cp -p %s %s"
                    % (
                        shlex.quote(backup_root),
                        shlex.quote(target),
                        shlex.quote(backup),
                    ),
                    check=True,
                )
                print("[deploy] asset backup: %s" % backup)
            write_bytes(run, target, path.read_bytes())
            print("[deploy] asset file: %s -> %s" % (path, target))
            deployed.append(target)
            continue

        if exists:
            backup = posixpath.join(
                backup_root, "%s-%s-%s" % (algorithm_type, path.name, stamp)
            )
            run(
                "mkdir -p %s && cp -a %s %s"
                % (
                    shlex.quote(backup_root),
                    shlex.quote(target),
                    shlex.quote(backup),
                ),
                check=True,
            )
            run("rm -rf %s" % shlex.quote(target), check=True)
            print("[deploy] asset backup: %s" % backup)

        run("mkdir -p %s" % shlex.quote(target), check=True)
        for current, dirs, files in os.walk(path):
            dirs[:] = sorted(name for name in dirs if name not in SKIP_ASSET_DIRS)
            relative_dir = Path(current).relative_to(path)
            destination_dir = (
                posixpath.join(target, *relative_dir.parts)
                if relative_dir.parts
                else target
            )
            run("mkdir -p %s" % shlex.quote(destination_dir), check=True)
            for name in sorted(files):
                if name.endswith(SKIP_ASSET_SUFFIXES):
                    continue
                source_file = Path(current) / name
                destination = posixpath.join(destination_dir, name)
                write_bytes(run, destination, source_file.read_bytes())
        print("[deploy] asset directory: %s -> %s" % (path, target))
        deployed.append(target)
    return deployed


def deploy(runner, location, algorithm_type, source_path, asset_plans=None, dry_run=False):
    content = source_path.read_text(encoding="utf-8")
    custom_dir = location["custom_dir"]
    type_dir = posixpath.join(custom_dir, algorithm_type)
    target = posixpath.join(type_dir, source_path.name)

    if location["kind"] == "host":
        run = runner.host
        label = "host side of the container bind mount"
    elif location["kind"] == "container":
        run = runner.run
        label = "container path"
    else:
        run = runner.run
        label = "local path"

    print("[deploy] algorithm type: %s" % algorithm_type)
    print("[deploy] algorithm file (%s): %s" % (label, target))
    for path, kind in asset_plans or []:
        print(
            "[deploy] asset %s: %s -> %s/%s"
            % (kind, path, algorithm_type, path.name)
        )
    if dry_run:
        print("[deploy] dry run: no files written")
        return target

    run("mkdir -p %s" % shlex.quote(type_dir), check=True)
    run("chmod 0750 %s" % shlex.quote(type_dir), check=True)

    if (
        run("test -L %s && echo yes || echo no" % shlex.quote(target))
        .stdout.strip()
        == "yes"
    ):
        raise DeployError(
            "target is a symlink, refusing to overwrite: %s; remove it manually" % target
        )

    exists = run("test -e %s && echo yes || echo no" % shlex.quote(target)).stdout.strip() == "yes"
    if exists:
        backup_dir = posixpath.join(custom_dir, ".deploy-backups")
        backup = posixpath.join(
            backup_dir,
            "%s.%s.%s.py"
            % (algorithm_type, source_path.stem, time.strftime("%Y%m%d%H%M%S")),
        )
        run(
            "mkdir -p %s && cp -p %s %s"
            % (
                shlex.quote(backup_dir),
                shlex.quote(target),
                shlex.quote(backup),
            ),
            check=True,
        )
        print("[deploy] existing file backed up to %s" % backup)

    temporary = target + ".tmp-" + uuid.uuid4().hex
    run("cat > %s" % shlex.quote(temporary), input_text=content, check=True)
    run(
        "chmod 0640 %s && mv -f %s %s"
        % (shlex.quote(temporary), shlex.quote(temporary), shlex.quote(target)),
        check=True,
    )

    expected = hashlib.sha256(content.encode("utf-8")).hexdigest()
    actual = runner.sha256(target, host=location["kind"] == "host")
    if actual and actual != expected:
        raise DeployError(
            "deployed file hash mismatch: expected %s, got %s" % (expected, actual)
        )
    print("[deploy] sha256: %s" % (actual or expected))
    print("[deploy] deployed %d bytes to %s" % (len(content.encode("utf-8")), target))

    deployed_assets = deploy_assets(
        run, type_dir, custom_dir, algorithm_type, asset_plans or []
    )
    if deployed_assets:
        print("[deploy] deployed %d asset(s)" % len(deployed_assets))
    return target


def restart(runner, args):
    if args.restart == "none":
        if runner.mode == "docker":
            print("[restart] required: docker restart %s" % args.container)
        else:
            print("[restart] required: sudo systemctl restart taosanoded")
            print("[restart] or: taosanodectl service stop && taosanodectl service start")
        print("[restart] the new algorithm is not visible until the service restarts")
        return False

    if args.restart == "docker" and runner.mode != "docker":
        raise DeployError("--restart docker requires a Docker deployment")

    if runner.mode == "docker":
        if not args.container:
            raise DeployError("--container is required to restart a Docker deployment")
        print("[restart] docker restart %s" % args.container)
        runner.host("docker restart %s" % shlex.quote(args.container), timeout=300, check=True)
        return True

    unit = pick_service(runner, args.service)
    if unit:
        print("[restart] systemctl restart %s" % unit)
        runner.run("systemctl restart %s" % shlex.quote(unit), timeout=300, check=True)
        return True

    has_ctl = (
        runner.run("command -v taosanodectl >/dev/null 2>&1 && echo yes || echo no")
        .stdout.strip()
        == "yes"
    )
    if has_ctl:
        print("[restart] taosanodectl service stop/start")
        runner.run("taosanodectl service stop || true; taosanodectl service start", timeout=300, check=True)
        return True

    raise DeployError(
        "no systemd unit or taosanodectl found; restart the TDgpt service manually"
    )


def wait_ready(base, timeout, insecure):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = http_json(base, "/status", timeout=5, insecure=insecure)
            if status.get("status") == "ready":
                return True
        except DeployError:
            pass
        time.sleep(2)
    return False


def timestamps(count):
    return [1577808000000 + index * 1000 for index in range(count)]


def payload_rows(payload):
    """Return the input record count of a column-oriented request payload.

    A classification or regression response derives ``rows`` from the
    prediction list itself, so comparing the two proves nothing. The request
    is the only source of the expected sample count.
    """
    columns = payload.get("data")
    if isinstance(columns, list) and columns and isinstance(columns[0], list):
        return len(columns[0])
    return None


def find_service(listing, group_type, model_name):
    for group in listing.get("details", []):
        if group.get("type") != group_type:
            continue
        for item in group.get("algo", []):
            if item.get("name") == model_name:
                return item
    return None


def load_sample(args):
    if not args.sample_file:
        return None
    path = Path(args.sample_file)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise DeployError("cannot read --sample-file %s: %s" % (path, exc))
    try:
        sample = json.loads(text)
    except ValueError as exc:
        raise DeployError("--sample-file is not valid JSON (%s): %s" % (path, exc))
    if not isinstance(sample, (dict, list)):
        raise DeployError(
            "--sample-file must contain a JSON object or a value list: %s" % path
        )
    return sample


def values_from_args(args):
    if not args.values:
        return None
    values = []
    for item in args.values.split(","):
        if not item.strip():
            continue
        try:
            values.append(float(item))
        except ValueError:
            raise DeployError("--values contains a non-numeric item: %r" % item)
    return values


def binary_labels(values):
    """Split values at their median so both classes appear in the payload.

    A median split only works when the series has two distinct sides. A
    constant series, and a series whose median equals its maximum, would label
    every row 1, and several classifiers then fail to fit. Fall back to
    alternating labels by position so the verification data always carries two
    classes.
    """
    if not values:
        return []
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        threshold = ordered[middle]
    else:
        threshold = (ordered[middle - 1] + ordered[middle]) / 2.0
    labels = [1 if value >= threshold else 0 for value in values]
    if len(set(labels)) == 1:
        return [index % 2 for index in range(len(values))]
    return labels


def sample_numbers(values, label):
    """Convert a JSON value list to floats, reporting failures as deploy errors."""
    if not isinstance(values, list) or not values:
        raise DeployError("%s must be a non-empty list of numbers" % label)
    converted = []
    for item in values:
        try:
            converted.append(float(item))
        except (TypeError, ValueError):
            raise DeployError("%s contains a non-numeric item: %r" % (label, item))
    return converted


def sample_columns(sample, path, minimum):
    """Return numeric columns from a sample payload after shape checks.

    Every column-based builder reads the row count from the first column, so a
    missing, empty, ragged or non-numeric column must fail here instead of
    during payload assembly. ``minimum`` follows the request contract of each
    algorithm type, not the example template: anomaly detection needs its
    timestamp column, forecast needs a timestamp and a target, while
    classification and regression accept a single feature column.
    """
    data = sample.get("data")
    if not isinstance(data, list) or len(data) < minimum:
        raise DeployError(
            "--sample-file data must hold at least %d columns: %s" % (minimum, path)
        )
    columns = []
    for index, column in enumerate(data):
        columns.append(sample_numbers(column, "--sample-file column %d" % index))
    lengths = {len(column) for column in columns}
    if len(lengths) != 1:
        raise DeployError("--sample-file columns must have the same length: %s" % path)
    return columns


def sample_schema(columns, template):
    """Describe the sample columns, keeping template names when the width matches."""
    if len(columns) == len(template):
        return template
    return [["col_%d" % (index + 1), "DOUBLE", 8] for index in range(len(columns))]


def build_ad_payload(args, model_name):
    sample = load_sample(args)
    if sample is not None:
        if isinstance(sample, list):
            values = sample_numbers(sample, "--sample-file values")
            sample = {
                "schema": DEFAULT_SCHEMA,
                "data": [timestamps(len(values)), values],
                "rows": len(values),
            }
        columns = sample_columns(sample, args.sample_file, 2)
        sample["data"] = columns
        sample.setdefault("schema", DEFAULT_SCHEMA)
        sample.setdefault("rows", len(columns[0]))
        sample["algo"] = model_name
        sample.setdefault("wncheck", 0)
        if args.option:
            sample["option"] = args.option
        return sample

    values = values_from_args(args)
    if values is None:
        values = [10.0 + 0.05 * (index % 5) for index in range(60)]
        values[40] = 40.0
    if len(values) < 10:
        raise DeployError("verification data needs at least 10 points")

    payload = {
        "schema": DEFAULT_SCHEMA,
        "data": [timestamps(len(values)), values],
        "rows": len(values),
        "algo": model_name,
        "wncheck": 0,
    }
    if args.option:
        payload["option"] = args.option
    return payload


def build_fc_payload(args, model_name):
    sample = load_sample(args)
    if sample is not None:
        if isinstance(sample, list):
            values = sample_numbers(sample, "--sample-file values")
            sample = {
                "schema": DEFAULT_SCHEMA,
                "data": [timestamps(len(values)), values],
                "rows": len(values),
            }
        columns = sample_columns(sample, args.sample_file, 2)
        # Keep every supplied column: a forecast request may carry
        # past_dynamic_real / dynamic_real columns that the schema indexes by
        # position, so rewriting data to two columns breaks the call.
        series = columns[0]
        sample["data"] = columns
        sample.setdefault("schema", DEFAULT_SCHEMA)
        sample["algo"] = model_name
        sample.setdefault("wncheck", 0)
        sample.setdefault("forecast_rows", args.forecast_rows)
        sample.setdefault("every", args.every)
        sample.setdefault("start", series[-1] + args.every)
        sample.setdefault("conf", 0.95)
        sample.setdefault("return_conf", 1)
        sample.setdefault("prec", "ms")
        if args.option:
            sample["option"] = args.option
        return sample

    values = values_from_args(args)
    if values is None:
        values = [
            10.0 + 0.05 * index + math.sin(index / 5.0) for index in range(80)
        ]
    if len(values) < 10:
        raise DeployError("verification data needs at least 10 points")

    series = timestamps(len(values))
    payload = {
        "schema": DEFAULT_SCHEMA,
        "data": [series, values],
        "rows": len(values),
        "algo": model_name,
        "forecast_rows": args.forecast_rows,
        "start": series[-1] + args.every,
        "every": args.every,
        "conf": 0.95,
        "return_conf": 1,
        "prec": "ms",
        "wncheck": 0,
    }
    if args.option:
        payload["option"] = args.option
    return payload


def build_classification_payload(args, model_name):
    schema = [["feature", "DOUBLE", 8], ["label", "TINYINT", 1]]
    sample = load_sample(args)
    if sample is not None:
        if isinstance(sample, list):
            features = sample_numbers(sample, "--sample-file values")
            labels = binary_labels(features)
            sample = {
                "schema": schema,
                "data": [features, labels],
                "rows": len(features),
            }
        columns = sample_columns(sample, args.sample_file, 1)
        sample["data"] = columns
        sample.setdefault("schema", sample_schema(columns, schema))
        sample.setdefault("rows", len(columns[0]))
        sample["algo"] = model_name
        if args.option:
            sample["option"] = args.option
        return sample

    values = values_from_args(args)
    if values is None:
        values = [float(index) for index in range(60)]
    if len(values) < 10:
        raise DeployError("verification data needs at least 10 points")

    labels = binary_labels(values)
    payload = {
        "schema": schema,
        "data": [values, labels],
        "rows": len(values),
        "algo": model_name,
    }
    if args.option:
        payload["option"] = args.option
    return payload


def build_regression_payload(args, model_name):
    schema = [["x", "DOUBLE", 8], ["y", "DOUBLE", 8]]
    sample = load_sample(args)
    if sample is not None:
        if isinstance(sample, list):
            features = sample_numbers(sample, "--sample-file values")
            targets = [2.0 * value + 1.0 for value in features]
            sample = {
                "schema": schema,
                "data": [features, targets],
                "rows": len(features),
            }
        columns = sample_columns(sample, args.sample_file, 1)
        sample["data"] = columns
        sample.setdefault("schema", sample_schema(columns, schema))
        sample.setdefault("rows", len(columns[0]))
        sample["algo"] = model_name
        if args.option:
            sample["option"] = args.option
        return sample

    values = values_from_args(args)
    if values is None:
        values = [float(index) for index in range(60)]
    if len(values) < 10:
        raise DeployError("verification data needs at least 10 points")

    targets = [2.0 * value + 1.0 for value in values]
    payload = {
        "schema": schema,
        "data": [values, targets],
        "rows": len(values),
        "algo": model_name,
    }
    if args.option:
        payload["option"] = args.option
    return payload


def build_batch_payload(args, model_name):
    config = None
    if args.config_json:
        try:
            config = json.loads(args.config_json)
        except ValueError as exc:
            raise DeployError("--config-json is not valid JSON: %s" % exc)

    sample = load_sample(args)
    if sample is not None:
        if isinstance(sample, list):
            data = sample_numbers(sample, "--sample-file values")
            series = timestamps(len(data))
            sample = {
                "ts": series,
                "data": data,
                "window": [[series[0], series[-1]]],
            }
        sample["data"] = sample_numbers(
            sample.get("data"), "--sample-file data"
        )
        sample["algo"] = model_name
        sample.setdefault("prec", "ms")
        sample.setdefault("protocol", 1.0)
        if config is not None:
            sample["config"] = config
        return sample

    values = values_from_args(args)
    if values is None:
        values = [10.0 + 0.05 * index for index in range(60)]
    if len(values) < 10:
        raise DeployError("verification data needs at least 10 points")

    series = timestamps(len(values))
    payload = {
        "algo": model_name,
        "ts": series,
        "data": values,
        "window": [[series[0], series[-1]]],
        "prec": "ms",
        "protocol": 1.0,
    }
    if config is not None:
        payload["config"] = config
    return payload


def build_payload(args, model_name, algorithm_type):
    if algorithm_type == "ad":
        return build_ad_payload(args, model_name)
    if algorithm_type == "fc":
        return build_fc_payload(args, model_name)
    if algorithm_type == "classification":
        return build_classification_payload(args, model_name)
    if algorithm_type == "regression":
        return build_regression_payload(args, model_name)
    return build_batch_payload(args, model_name)


def verify(base, algorithm_type, model_name, payload, timeout, insecure):
    if algorithm_type == "batch":
        listing = http_json(
            base, "/api/v1/analysis/batch/algos", timeout=timeout, insecure=insecure
        )
        info = next(
            (item for item in listing.get("algo", []) if item.get("name") == model_name),
            None,
        )
        builtin_key = "builtin"
        endpoint = "/api/v1/analysis/batch"
    else:
        listing = http_json(base, "/list", timeout=timeout, insecure=insecure)
        info = find_service(listing, SERVICE_TYPES[algorithm_type], model_name)
        builtin_key = "builtins"
        endpoint = {
            "ad": "/anomaly-detect",
            "fc": "/forecast",
            "classification": "/classification",
            "regression": "/regression",
        }[algorithm_type]

    if info is None:
        print(
            "[verify] FAILED: %s algorithm %r is not registered"
            % (algorithm_type, model_name)
        )
        print("         check that the service was restarted and the log contains")
        print("         'load algorithm:' or 'failed to import module'")
        return False

    print(
        "[verify] registered: type=%s name=%s builtin=%s status=%s params=%s"
        % (
            algorithm_type,
            info.get("name"),
            info.get(builtin_key),
            info.get("status"),
            json.dumps(info.get("params", {}), ensure_ascii=False),
        )
    )

    curl = "curl -s -X POST %s%s -H 'Content-Type: application/json' -d '%s'" % (
        base,
        endpoint,
        json.dumps(payload, separators=(",", ":")),
    )
    print("[verify] request: %s" % curl)

    response = http_json(base, endpoint, payload, timeout=timeout, insecure=insecure)
    rows = response.get("rows")
    if response.get("msg") or rows is None or rows == -1:
        print("[verify] FAILED: %s" % json.dumps(response, ensure_ascii=False))
        return False

    if algorithm_type == "fc":
        result = response.get("res") or []
        if rows < 1 or len(result) < 2 or any(len(item) != rows for item in result[:2]):
            print(
                "[verify] FAILED: forecast response is inconsistent: %s"
                % json.dumps(response, ensure_ascii=False)
            )
            return False
    if algorithm_type == "batch":
        if any(response.get(key) is None for key in ("center", "lower", "upper")):
            print(
                "[verify] FAILED: batch response is missing center/lower/upper: %s"
                % json.dumps(response, ensure_ascii=False)
            )
            return False
    if algorithm_type in ("classification", "regression"):
        result = response.get("res")
        expected = payload_rows(payload)
        if (
            not isinstance(result, list)
            or len(result) != rows
            or (expected is not None and len(result) != expected)
        ):
            print(
                "[verify] FAILED: %s returned %s predictions, expected %s input rows: %s"
                % (
                    algorithm_type,
                    len(result) if isinstance(result, list) else None,
                    expected if expected is not None else rows,
                    json.dumps(response, ensure_ascii=False),
                )
            )
            return False

    detail = ""
    if algorithm_type == "fc":
        detail = " res_lists=%d" % len(response.get("res") or [])
    elif algorithm_type in ("classification", "regression"):
        detail = " predictions=%d" % len(response.get("res") or [])
    print(
        "[verify] call ok: algo=%s rows=%s%s"
        % (response.get("algo"), rows, detail)
    )
    if rows == 0:
        print(
            "[verify] WARN: the call succeeded but produced no output points; "
            "adjust --option, --config-json, --values or --sample-file"
        )
    return True


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Deploy a user-defined TDgpt algorithm (ad, fc, classification, regression or batch)."
    )
    parser.add_argument("--tdgpt-url", required=True, help="TDgpt base URL, e.g. http://127.0.0.1:6035")
    parser.add_argument("--model-name", required=True, help="algorithm name, must match class attribute name")
    parser.add_argument("--script", required=True, help="path to the algorithm script")
    parser.add_argument(
        "--algorithm-type",
        choices=["auto"] + list(ALGORITHM_TYPES),
        default="auto",
        help="algorithm type; auto detects from the script",
    )
    parser.add_argument(
        "--deploy-mode",
        choices=["auto", "local", "docker", "remote"],
        default="auto",
        help="how TDgpt runs; auto probes --ssh, --container, then docker ps",
    )
    parser.add_argument("--container", help="TDgpt container name")
    parser.add_argument("--ssh", help="SSH target, e.g. root@192.168.1.20")
    parser.add_argument("--venv-python", help="TDgpt main venv python on the target")
    parser.add_argument("--custom-dir", help="override custom_algo_dir on the target")
    parser.add_argument("--config", help="TDgpt configuration file on the target")
    parser.add_argument(
        "--restart",
        choices=["auto", "none", "systemd", "docker"],
        default="none",
        help="restart after deployment; default only prints the command",
    )
    parser.add_argument("--service", help="systemd unit name (taosanoded or taosanode)")
    parser.add_argument(
        "--install-missing",
        action="store_true",
        help="install missing packages after user confirmation (requires --yes)",
    )
    parser.add_argument("--yes", action="store_true", help="confirm package installation")
    parser.add_argument("--check", action="store_true", help="preflight only; do not deploy")
    parser.add_argument("--verify-only", action="store_true", help="only run registration and call checks")
    parser.add_argument("--dry-run", action="store_true", help="resolve and validate but do not write")
    parser.add_argument(
        "--allow-ephemeral",
        action="store_true",
        help="allow writing into an unmounted container path (lost on container recreation)",
    )
    parser.add_argument(
        "--asset",
        action="append",
        default=[],
        help="additional file or directory to deploy next to the entry (repeatable)",
    )
    parser.add_argument(
        "--option",
        default="",
        help="option string for ad/fc/classification/regression, e.g. window=20",
    )
    parser.add_argument("--config-json", help="batch config object as JSON, e.g. '{\"factor\":3}'")
    parser.add_argument("--forecast-rows", type=int, default=5, help="fc verification forecast rows")
    parser.add_argument("--every", type=int, default=1000, help="fc verification time step")
    parser.add_argument("--values", default="", help="comma-separated verification values")
    parser.add_argument("--sample-file", help="JSON file with a verification payload or a value list")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--insecure", action="store_true", help="skip TLS certificate verification")
    parser.add_argument(
        "--sudo",
        action="store_true",
        help="run local and SSH commands through sudo -n sh -c",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    base = normalize_url(args.tdgpt_url)
    source_path = Path(args.script).resolve()

    if not source_path.is_file():
        print("[error] script not found: %s" % source_path)
        return EXIT_PREFLIGHT
    if not re.match(r"^[A-Za-z0-9_.-]+$", args.model_name):
        print("[error] invalid algorithm name: %r" % args.model_name)
        return EXIT_PREFLIGHT

    try:
        status = http_json(base, "/status", timeout=args.timeout, insecure=args.insecure)
        print("[check] TDgpt %s status=%s" % (base, status.get("status")))

        if args.verify_only:
            _text, algorithm_type = validate_source(
                source_path, args.model_name, args.algorithm_type
            )
            payload = build_payload(args, args.model_name, algorithm_type)
            return EXIT_OK if verify(
                base, algorithm_type, args.model_name, payload, args.timeout, args.insecure
            ) else EXIT_VERIFY

        mode = detect_mode(args)
        if platform.system().lower() == "windows" and mode in ("local", "docker"):
            raise DeployError(
                "this deployment script requires a POSIX shell and coreutils "
                "(sh, chmod, sha256sum, base64). Windows native and local "
                "Docker deployments are not supported. Run it on Linux/macOS, "
                "or use --deploy-mode remote with an SSH target."
            )
        runner = Runner(mode, container=args.container, ssh=args.ssh, sudo=args.sudo)
        print(
            "[check] deploy mode=%s%s%s"
            % (
                mode,
                " container=%s" % args.container if args.container else "",
                " ssh=%s" % args.ssh if args.ssh else "",
            )
        )

        unit = None
        override = ""
        if mode in ("local", "remote") and not args.container:
            unit = pick_service(runner, args.service)
            override = service_env(runner, unit).get("TDGPT_CUSTOM_ALGO_DIR", "")
        if mode == "docker" or args.container:
            override = container_env(runner, args.container).get("TDGPT_CUSTOM_ALGO_DIR", "")
            if not override:
                override = ""
        if args.custom_dir:
            override = args.custom_dir

        runtime = resolve_runtime(runner, args, override)
        print("[check] main venv python: %s (python %s)" % (runtime["python"], runtime["version"]))
        print("[check] configuration: %s" % (runtime["config"] or "<defaults>"))
        print("[check] custom_algo_dir: %s" % runtime["custom_dir"])

        location = resolve_deploy_location(
            runner, runtime, args.container, args.allow_ephemeral
        )

        text, algorithm_type = validate_source(
            source_path, args.model_name, args.algorithm_type
        )
        print("[check] algorithm type: %s" % algorithm_type)
        print("[check] script contract is valid: %s" % source_path)

        asset_plans = plan_assets(source_path, args.asset)
        for path, kind in asset_plans:
            print(
                "[check] asset %s: %s -> %s/%s"
                % (kind, path, algorithm_type, path.name)
            )

        bundled_imports = collect_bundled_imports(source_path, text, asset_plans)
        modules = set(collect_imports(text, source_path))
        for path, kind in asset_plans:
            if kind == "file":
                if path.suffix == ".py":
                    asset_text = path.read_text(encoding="utf-8")
                    collect_bundled_imports(
                        path, asset_text, asset_plans, bundled_imports
                    )
                    modules.update(collect_imports(asset_text, path))
            else:
                for file_path in iter_py_files(path):
                    asset_text = file_path.read_text(encoding="utf-8")
                    collect_bundled_imports(
                        file_path, asset_text, asset_plans, bundled_imports
                    )
                    modules.update(collect_imports(asset_text, file_path))

        check_dependencies(
            runner,
            runtime,
            sorted(modules),
            bundled_imports,
            args.install_missing,
            args.yes,
        )

        if args.check:
            print("[check] preflight passed; nothing was written and the service was not restarted")
            return EXIT_OK

        target = deploy(
            runner,
            location,
            algorithm_type,
            source_path,
            asset_plans=asset_plans,
            dry_run=args.dry_run,
        )
        if args.dry_run:
            return EXIT_OK

        restarted = restart(runner, args)
        if not restarted:
            print(
                "[done] deployment finished, restart pending; after restarting run:\n"
                "       %s --verify-only"
                % " ".join([sys.executable, __file__] + sys.argv[1:])
            )
            return EXIT_OK

        if not wait_ready(base, 90, args.insecure):
            print("[warn] service did not report ready within 90s; check the service log")
        payload = build_payload(args, args.model_name, algorithm_type)
        return EXIT_OK if verify(
            base, algorithm_type, args.model_name, payload, args.timeout, args.insecure
        ) else EXIT_VERIFY

    except MissingDependencies as exc:
        print("[error] %s" % exc)
        return EXIT_DEPS
    except DeployError as exc:
        print("[error] %s" % exc)
        return EXIT_PREFLIGHT


if __name__ == "__main__":
    sys.exit(main())
