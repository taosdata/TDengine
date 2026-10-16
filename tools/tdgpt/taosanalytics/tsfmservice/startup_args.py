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

"""Startup argument parsing for the TSFM model servers.

Deliberately its own stdlib-only module: the servers import torch and
transformers, so keeping the rules here is what makes them unit-testable, and a
mistake in them is otherwise invisible. A known flag that lost its value used to
be dropped through the same path as an unknown one, so the server quietly stayed
on its default -- and since the caller passes the configured port, the mismatch
only showed up as "the service is not listening where it should".
"""

# The flags `taosanode_service._build_model_args` sends to a model server.
PORT_FLAG = "--port"
MODEL_FOLDER_FLAG = "--model-folder"


class StartupArgsError(ValueError):
    """A known flag was used without a usable value.

    Distinct from an unknown flag, which is dropped on purpose: the same argv
    shape goes to every model server, so flags that belong to another one must not
    stop this one from starting.
    """


def parse_startup_args(argv, value_flags, defaults=None):
    """Split argv into the known flag values and the arguments to leave alone.

    Args:
        argv: Argument vector, ``argv[0]`` being the program name
        value_flags: Flags this server understands, each of which takes a value
        defaults: Values to start from, keyed by flag

    Returns:
        tuple: (values, remaining), where ``values`` maps every flag in
            `value_flags` to what was passed or to its default, and ``remaining``
            is ``argv[0]`` plus every argument that is not one of those pairs.

    Raises:
        StartupArgsError: A known flag is the last argument, its value is itself a
            flag, or an inline ``--flag=`` value is empty. Every one of those used
            to fall through to the unknown-flag path and be dropped without a
            word.
    """
    values = dict(defaults or {})
    remaining = list(argv[:1])
    index = 1
    while index < len(argv):
        token = argv[index]
        flag, separator, inline = token.partition("=")

        if flag not in value_flags:
            remaining.append(token)
            index += 1
            continue

        if separator:
            if not inline:
                raise StartupArgsError(f"{flag} needs a value, but got {token!r}")
            values[flag] = inline
            index += 1
            continue

        if index + 1 >= len(argv):
            raise StartupArgsError(f"{flag} needs a value, but it is the last argument")

        value = argv[index + 1]
        if value.partition("=")[0] in value_flags:
            raise StartupArgsError(f"{flag} needs a value, but got {value!r}")

        values[flag] = value
        index += 2

    return values, remaining


def int_value(flag, value, minimum=1, maximum=65535):
    """Return `value` as an int, or raise StartupArgsError naming the flag.

    The range matters: a port outside it is rejected here, with the flag named,
    instead of failing obscurely at bind time.
    """
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise StartupArgsError(f"{flag} expects an integer, got {value!r}") from None

    if not minimum <= parsed <= maximum:
        raise StartupArgsError(
            f"{flag} must be between {minimum} and {maximum}, got {value!r}"
        )
    return parsed


def parse_model_server_args(argv, default_port):
    """Parse the startup flags a TSFM model server may receive.

    Args:
        argv: Argument vector, ``argv[0]`` being the program name
        default_port: Port used when ``--port`` is absent

    Returns:
        tuple: (port, model_folder, remaining argv) -- what the server passes to
            its model loader and to ``sys.argv``.

    Raises:
        StartupArgsError: See parse_startup_args and int_value.
    """
    values, remaining = parse_startup_args(
        argv,
        (PORT_FLAG, MODEL_FOLDER_FLAG),
        {PORT_FLAG: str(default_port)},
    )
    return int_value(PORT_FLAG, values[PORT_FLAG]), values.get(MODEL_FOLDER_FLAG), remaining
