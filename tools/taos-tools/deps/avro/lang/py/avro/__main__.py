#!/usr/bin/env python3

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Command line utility for reading and writing Avro files."""
import argparse
import csv
import functools
import itertools
import json
import sys
import warnings
from pathlib import Path
from typing import (
    IO,
    Any,
    AnyStr,
    Callable,
    Collection,
    Dict,
    Generator,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import avro
import avro.datafile
import avro.errors
import avro.io
import avro.schema


def print_json(row: object) -> None:
    print(json.dumps(row))


def print_json_pretty(row: object) -> None:
    """Pretty print JSON"""
    # Need to work around https://bugs.python.org/issue16333
    # where json.dumps leaves trailing spaces.
    result = json.dumps(row, sort_keys=True, indent=4).replace(" \n", "\n")
    print(result)


_write_row = csv.writer(sys.stdout).writerow


def print_csv(row: Mapping[str, object]) -> None:
    # We sort the keys to the fields will be in the same place
    # FIXME: Do we want to do it in schema order?
    _write_row(row[key] for key in sorted(row))


def select_printer(format_: str) -> Callable[[Mapping[str, object]], None]:
    return {"json": print_json, "json-pretty": print_json_pretty, "csv": print_csv}[format_]


def record_match(expr: Any, record: Any) -> Callable[[Mapping[str, object]], bool]:
    warnings.warn(avro.errors.AvroWarning("There is no way to safely type check this."))
    return cast(Callable[[Mapping[str, object]], bool], eval(expr, None, {"r": record}))


def _passthrough_keys_filter(obj: Mapping[str, object]) -> Dict[str, object]:
    return {**obj}


def field_selector(fields: Collection[str]) -> Callable[[Mapping[str, object]], Dict[str, object]]:
    if fields:

        def keys_filter(obj: Mapping[str, object]) -> Dict[str, object]:
            return {k: obj[k] for k in obj.keys() & set(fields)}

        return keys_filter
    return _passthrough_keys_filter


def print_avro(
    data_file_reader: avro.datafile.DataFileReader,
    header: bool,
    format_: str,
    filter_: str,
    skip: int = 0,
    count: int = 0,
    fields: Collection[str] = "",
) -> None:
    if header and format_ != "csv":
        raise avro.errors.UsageError("--header applies only to CSV format")
    predicate = functools.partial(record_match, filter_) if filter_ else None
    avro_filtered = filter(predicate, data_file_reader)
    avro_slice = itertools.islice(avro_filtered, skip, (skip + count) if count else None)
    fs = field_selector(fields)
    avro_fields = (fs(cast(Mapping[str, object], r)) for r in avro_slice)
    avro_enum = enumerate(avro_fields)
    printer = select_printer(format_)
    for i, record in avro_enum:
        if header and i == 0:
            _write_row(sorted(record))
        printer(record)


def print_schema(data_file_reader: avro.datafile.DataFileReader) -> None:
    print(json.dumps(json.loads(data_file_reader.schema), indent=4))


def cat(
    files: Sequence[IO[AnyStr]], print_schema_: bool, header: bool, format_: str, filter_: str, skip: int, count: int, fields: Collection[str]
) -> int:
    datum_reader = avro.io.DatumReader()
    for file_ in files:
        with avro.datafile.DataFileReader(file_, datum_reader) as data_file_reader:
            if print_schema_:
                print_schema(data_file_reader)
                continue
            print_avro(data_file_reader, header, format_, filter_, skip, count, fields)
    return 0


def iter_json(info: Iterable[AnyStr], _: Any) -> Generator[object, None, None]:
    for i in info:
        row = (i if isinstance(i, str) else cast(bytes, i).decode()).strip()
        if row:
            yield json.loads(row)


def convert(value: str, field: avro.schema.Field) -> Union[int, float, str, bytes, bool, None]:
    type_ = field.type.type
    if type_ in ("int", "long"):
        return int(value)
    if type_ in ("float", "double"):
        return float(value)
    if type_ == "string":
        return value
    if type_ == "bytes":
        return value.encode()
    if type_ == "boolean":
        return value.lower() in ("1", "t", "true")
    if type_ == "null":
        return None
    if type_ == "union":
        return convert_union(value, field)
    raise avro.errors.UsageError("No valid conversion type")


def convert_union(value: str, field: avro.schema.Field) -> Union[int, float, str, bytes, bool, None]:
    for name in (s.name for s in field.type.schemas):
        try:
            return convert(value, name)
        except ValueError:
            continue
    raise avro.errors.UsageError("Exhausted Union Schema without finding a match")


def iter_csv(info: IO[AnyStr], schema: avro.schema.RecordSchema) -> Generator[Dict[str, object], None, None]:
    header = [field.name for field in schema.fields]
    for row in csv.reader(getattr(i, "decode", lambda: i)() for i in info):
        values = [convert(v, f) for v, f in zip(row, schema.fields)]
        yield dict(zip(header, values))


def guess_input_type(io_: IO[AnyStr]) -> str:
    ext = Path(io_.name).suffixes[0].lower()
    extensions = {".json": "json", ".js": "json", ".csv": "csv"}
    try:
        return extensions[ext]
    except KeyError as e:
        raise avro.errors.UsageError("Can't guess input file type (not .json or .csv)") from e


def write(schema: avro.schema.RecordSchema, output: IO[AnyStr], input_files: Sequence[IO[AnyStr]], input_type: Optional[str] = None) -> int:
    input_type = input_type or guess_input_type(cast(IO[AnyStr], next(iter(input_files), sys.stdin)))
    iter_records = {"json": iter_json, "csv": iter_csv}[input_type]
    with avro.datafile.DataFileWriter(output, avro.io.DatumWriter(), schema) as writer:
        for file_ in input_files:
            for record in iter_records(file_, schema):
                writer.append(record)
    return 0


def csv_arg(arg: Optional[str]) -> Optional[Tuple[str, ...]]:
    return tuple(f.strip() for f in arg.split(",") if f.strip()) if (arg and arg.strip()) else None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Display/write for Avro files")
    parser.add_argument("--version", action="version", version=avro.__version__)
    subparsers = parser.add_subparsers(required=True, dest="command") if sys.version_info >= (3, 7) else parser.add_subparsers(dest="command")
    subparser_cat = subparsers.add_parser("cat")
    subparser_cat.add_argument("--version", action="version", version=avro.__version__)
    subparser_cat.add_argument(
        "--count",
        "-n",
        default=None,
        type=int,
        help="number of records to print",
    )
    subparser_cat.add_argument(
        "--skip",
        "-s",
        type=int,
        default=0,
        help="number of records to skip",
    )
    subparser_cat.add_argument(
        "--format",
        "-f",
        default="json",
        choices=("json", "csv", "json-pretty"),
        help="record format",
    )
    subparser_cat.add_argument(
        "--header",
        "-H",
        default=False,
        action="store_true",
        help="print CSV header",
    )
    subparser_cat.add_argument(
        "--filter",
        "-F",
        default=None,
        help="filter records (e.g. r['age']>1)",
    )
    subparser_cat.add_argument(
        "--print-schema",
        "-p",
        action="store_true",
        default=False,
        help="print schema",
    )
    subparser_cat.add_argument("--fields", type=csv_arg, default=None, help="fields to show, comma separated (show all by default)")
    subparser_cat.add_argument("files", nargs="*", type=argparse.FileType("rb"), help="Files to read", default=(sys.stdin,))
    subparser_write = subparsers.add_parser("write")
    subparser_write.add_argument("--version", action="version", version=avro.__version__)
    subparser_write.add_argument("--schema", "-s", type=avro.schema.from_path, required=True, help="schema JSON file (required)")
    subparser_write.add_argument(
        "--input-type",
        "-f",
        choices=("json", "csv"),
        default=None,
        help="input file(s) type (json or csv)",
    )
    subparser_write.add_argument("--output", "-o", help="output file", type=argparse.FileType("wb"), default=sys.stdout)
    subparser_write.add_argument("input_files", nargs="*", type=argparse.FileType("r"), help="Files to read", default=(sys.stdin,))
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.command == "cat":
        return cat(args.files, args.print_schema, args.header, args.format, args.filter, args.skip, args.count, args.fields)
    if args.command == "write":
        return write(args.schema, args.output, args.input_files, args.input_type)
    raise avro.errors.UsageError(f"Unknown command - {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
