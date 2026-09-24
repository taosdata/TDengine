#!/usr/bin/env python3
"""Generate the static SQL-dimension ID manifest from the DS chapter 8."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


FRAG_RE = re.compile(r"(?<![A-Z0-9_])(FRAG_[A-Z0-9_]+)(?![A-Z0-9_])")
VAL_RE = re.compile(r"(?<![A-Z0-9_])(VAL_[A-Z0-9_]+)(?![A-Z0-9_])")
RELATION_TOKEN_RE = re.compile(
    r"RUNTIME_OBJECT\([a-z_]+\)|FRAG_[A-Z0-9_]+|VAL_[A-Z0-9_]+|[!\[\]{}|]"
)
RELATION_START = "### 8.3 片段关系总览"
RELATION_END = "#### 8.3.1 生成维度和组合覆盖"
INVALID_VALUES = {"VAL_", "VAL_INTEGER_"}
LEGACY_VALUE_IDS = (
    "LITERAL_ONE",
    "LITERAL_ZERO",
    "QUERY_ROOT",
    "QUERY_STMT_ABNORMAL",
    "QUERY_STMT_ABNORMAL_SYNTAX",
    "QUERY_STMT_ABNORMAL_TABLE",
    "QUERY_STMT_AGGREGATE",
    "QUERY_STMT_COUNT",
    "QUERY_STMT_FILTER",
    "QUERY_STMT_JOIN",
    "QUERY_STMT_NULLS",
    "QUERY_STMT_ORDER",
    "QUERY_STMT_SELECT",
    "QUERY_STMT_SUBQUERY",
    "QUERY_STMT_TYPES",
    "QUERY_TERMINATOR_EMPTY",
    "QUERY_TOP_LEVEL_STMT",
)
RUNTIME_OBJECT_KINDS = {
    "source": "QST_RUNTIME_OBJECT_SOURCE",
    "database": "QST_RUNTIME_OBJECT_DATABASE",
    "table": "QST_RUNTIME_OBJECT_TABLE",
    "column": "QST_RUNTIME_OBJECT_COLUMN",
    "table_reference": "QST_RUNTIME_OBJECT_TABLE_REFERENCE",
    "external_table_suffix": "QST_RUNTIME_OBJECT_EXTERNAL_TABLE_SUFFIX",
    "column_reference": "QST_RUNTIME_OBJECT_COLUMN_REFERENCE",
    "qualified_asterisk": "QST_RUNTIME_OBJECT_QUALIFIED_ASTERISK",
    "qualified_tbname_pseudocolumn": "QST_RUNTIME_OBJECT_QUALIFIED_TBNAME_PSEUDOCOLUMN",
    "rollup_tag_placeholder": "QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER",
}
DIALECT_MASKS = {
    "tdengine": "QST_GENERATOR_DIALECT_MASK_TDENGINE",
    "mysql": "QST_GENERATOR_DIALECT_MASK_MYSQL",
    "postgresql": "QST_GENERATOR_DIALECT_MASK_POSTGRESQL",
    "influxql": "QST_GENERATOR_DIALECT_MASK_INFLUXQL",
}
INTERNAL_FRAGMENT_BASE = 1000000
INTERNAL_VALUE_BASE = 2000000
COMPILED_LOOP_MAX = "QST_GENERATOR_SQL_DIMENSION_LOOP_ITERATIONS"
NORMAL_WEIGHT = 950000
ABNORMAL_WEIGHT = 50000
LOW_NORMAL_WEIGHT = 50000
NORMAL_SEED_WEIGHT = NORMAL_WEIGHT
ZERO_WEIGHT_ABNORMAL_FRAGMENTS = {"FRAG_RUNTIME_TABLE_REFERENCE"}
FOREIGN_DIALECT_MARKERS = (
    "PostgreSQL",
    "MySQL",
    "Oracle",
    "SQL Server",
    "BigQuery",
    "Snowflake",
    "DuckDB",
    "外部方言",
)
TEXT_OVERRIDES = {
    "VAL_LEXER_INVALID_UNTERMINATED_STRING": "'unterminated",
    "VAL_LEXER_INVALID_STRING_ESCAPE_EOF": "'abc\\",
    "VAL_LEXER_INVALID_EMPTY_QUOTED_IDENTIFIER": "`",
    "VAL_LEXER_INVALID_EMPTY_QUOTED_IDENTIFIER_SUFFIX": "``x",
    "VAL_IDENTIFIER_QUOTED_ESCAPED": "`a``b`",
    "VAL_UDF_AGG_AF_TYPED_NUMERIC": "af_min_int",
}


def extract_relations(document: str) -> dict[str, str]:
    section = document[document.index(RELATION_START) : document.index(RELATION_END)]
    relations: dict[str, str] = {}
    name: str | None = None
    lines: list[str] = []

    for line in section.splitlines():
        match = re.fullmatch(r"(FRAG_[A-Z0-9_]+):", line.strip())
        if match:
            if name is not None:
                relations[name] = " ".join(lines)
            name = match.group(1)
            lines = []
        elif name is not None:
            if not line.strip() or line.strip() == "```" or line.startswith("# "):
                relations[name] = " ".join(lines)
                name = None
                lines = []
            else:
                lines.append(line.strip())
    if name is not None:
        relations[name] = " ".join(lines)
    return relations


def extract_value_rows(document: str) -> set[str]:
    return set(re.findall(r"^\|\s*`?(VAL_[A-Z0-9_]+)`?\s*\|", document, re.M))


def split_markdown_row(line: str) -> list[str]:
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for character in line.strip()[1:-1]:
        if escaped:
            current.append(character)
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == "|":
            cells.append("".join(current).strip())
            current = []
        else:
            current.append(character)
    if escaped:
        current.append("\\")
    cells.append("".join(current).strip())
    return cells


def extract_value_texts(document: str, values: list[str]) -> dict[str, str]:
    texts: dict[str, str] = {}
    for line in document.splitlines():
        if not line.startswith("| "):
            continue
        cells = split_markdown_row(line)
        if len(cells) != 3:
            continue
        name = cells[0].strip("`")
        if name not in values:
            continue
        cell = cells[2]
        if cell == "空文本":
            texts[name] = ""
            continue
        if cell.startswith("`") and cell.endswith("`") and len(cell) >= 2:
            texts[name] = cell[1:-1]
            continue
        code = re.search(r"`([^`]*)`", cell)
        if code is not None:
            texts[name] = code.group(1)
        elif cell:
            texts[name] = cell
    texts.update(TEXT_OVERRIDES)
    missing = sorted(set(values) - set(texts))
    if missing:
        raise SystemExit(f"VAL text extraction mismatch missing={missing}")
    return texts


def extract_low_probability_values(document: str, values: list[str]) -> set[str]:
    """Find explicitly foreign-dialect values without filtering them."""
    value_names = set(values)
    low_probability: set[str] = set()
    for line in document.splitlines():
        if not line.startswith("| "):
            continue
        cells = split_markdown_row(line)
        if len(cells) != 3:
            continue
        name = cells[0].strip("`")
        if name in value_names and any(marker in cells[1]
                                       for marker in FOREIGN_DIALECT_MARKERS):
            low_probability.add(name)
    return low_probability


def find_required_fragments(relations: dict[str, str]) -> set[str]:
    """Find fragments referenced outside optional and loop alternatives."""
    required: set[str] = set()

    def visit(node: tuple, optional: bool = False) -> None:
        kind = node[0]
        if kind in {"sequence", "alternation"}:
            for child in node[1]:
                visit(child, optional)
        elif kind in {"optional", "ordered_optional", "loop"}:
            visit(node[1], True)
        elif kind == "fragment" and not optional:
            required.add(node[1])

    for relation in relations.values():
        visit(RelationParser(relation).parse())
    return required


class RelationParser:
    def __init__(self, relation: str) -> None:
        self.relation = relation
        self.tokens = RELATION_TOKEN_RE.findall(relation)
        consumed = RELATION_TOKEN_RE.sub("", relation)
        if consumed.strip():
            raise ValueError(f"unparsed relation text: {consumed!r}")
        self.index = 0

    def parse(self) -> tuple:
        expression = self.parse_alternation()
        if self.index != len(self.tokens):
            raise ValueError(f"unexpected token: {self.tokens[self.index]}")
        return expression

    def parse_alternation(self) -> tuple:
        choices = [self.parse_sequence()]
        while self.index < len(self.tokens) and self.tokens[self.index] == "|":
            self.index += 1
            choices.append(self.parse_sequence())
        return ("alternation", tuple(choices)) if len(choices) > 1 else choices[0]

    def parse_sequence(self) -> tuple:
        items = []
        while self.index < len(self.tokens) and self.tokens[self.index] not in "]}|":
            token = self.tokens[self.index]
            self.index += 1
            if token == "[":
                nested = self.parse_alternation()
                self.expect("]")
                items.append(("optional", nested))
            elif token == "!":
                self.expect("[")
                nested = self.parse_alternation()
                self.expect("]")
                items.append(("ordered_optional", nested))
            elif token == "{":
                nested = self.parse_alternation()
                self.expect("}")
                items.append(("loop", nested))
            elif token.startswith("FRAG_"):
                items.append(("fragment", token))
            elif token.startswith("VAL_"):
                items.append(("value", token))
            elif token.startswith("RUNTIME_OBJECT("):
                items.append(("runtime", token[15:-1]))
            else:
                raise ValueError(f"unexpected token: {token}")
        if not items:
            raise ValueError("empty sequence")
        return ("sequence", tuple(items))

    def expect(self, token: str) -> None:
        if self.index >= len(self.tokens) or self.tokens[self.index] != token:
            actual = "end" if self.index >= len(self.tokens) else self.tokens[self.index]
            raise ValueError(f"expected {token}, got {actual}")
        self.index += 1


def validate_relations(relations: dict[str, str], values: list[str]) -> None:
    fragment_names = set(relations)
    value_names = set(values)

    def validate_node(node: tuple) -> None:
        kind = node[0]
        if kind in {"sequence", "alternation"}:
            for child in node[1]:
                validate_node(child)
        elif kind in {"optional", "ordered_optional", "loop"}:
            validate_node(node[1])
        elif kind == "fragment" and node[1] not in fragment_names:
            raise SystemExit(f"unknown FRAG reference: {node[1]}")
        elif kind == "value" and node[1] not in value_names:
            raise SystemExit(f"unknown VAL reference: {node[1]}")
        elif kind == "runtime" and node[1] not in RUNTIME_OBJECT_KINDS:
            raise SystemExit(f"unknown RUNTIME_OBJECT kind: {node[1]}")
        elif kind not in {"sequence", "alternation", "optional", "loop", "fragment", "value", "runtime"}:
            raise SystemExit(f"unknown relation node: {kind}")

    for name, relation in relations.items():
        try:
            validate_node(RelationParser(relation).parse())
        except ValueError as error:
            raise SystemExit(f"invalid relation {name}: {error}") from error


def write_header(path: Path, fragments: list[str], values: list[str]) -> None:
    lines = [
        "/* Generated from DS chapter 8. Do not edit by hand. */",
        "",
        "#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_SQL_DIMENSION_IDS_H_",
        "#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_SQL_DIMENSION_IDS_H_",
        "",
        "#define SQL_DIMENSION_CONFIG_VERSION 3U",
        "#define SQL_DIMENSION_INTERNAL_FRAGMENT_BASE 1000000U",
        "#define SQL_DIMENSION_INTERNAL_VALUE_BASE 2000000U",
        "",
    ]
    for index, name in enumerate(fragments, 1):
        lines.append(f"#define SQL_DIMENSION_{name} {index}U")
    lines.append("")
    for index, name in enumerate(values, 1):
        lines.append(f"#define SQL_DIMENSION_{name} {index}U")
    lines.extend(
        [
            "",
            "/* Transitional whole-statement candidates; not DS VAL definitions. */",
        ]
    )
    for index, name in enumerate(LEGACY_VALUE_IDS, 1):
        lines.append(f"#define SQL_DIMENSION_VAL_{name} {1000000 + index}U")
    lines.extend(
        [
            "",
            f"#define SQL_DIMENSION_FRAG_SOURCE_COUNT {len(fragments)}U",
            f"#define SQL_DIMENSION_VAL_SOURCE_COUNT {len(values)}U",
            "",
            "#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_SQL_DIMENSION_IDS_H_",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="ascii")


def c_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def weight_for_value(value: dict) -> int:
    """Returns the configured fixed-point weight for one generated value."""
    if (value.get("class", "QST_GENERATOR_CLASS_NORMAL") ==
            "QST_GENERATOR_CLASS_ABNORMAL"):
        if "weight" in value:
            return value["weight"]
        return ABNORMAL_WEIGHT
    if value.get("low_probability", False):
        return LOW_NORMAL_WEIGHT
    return NORMAL_WEIGHT


def is_explicit_abnormal_name(name: str) -> bool:
    """Classifies only DS-declared invalid/mutation branches as ABNORMAL."""
    return "INVALID" in name


def dialect_mask_for_fragment(name: str) -> str:
    """Returns an ABI dialect mask only where DS identifies one unambiguously."""
    if "MYSQL" in name:
        return DIALECT_MASKS["mysql"]
    if "POSTGRESQL" in name or "POSTGRES" in name:
        return DIALECT_MASKS["postgresql"]
    if "INFLUX" in name:
        return DIALECT_MASKS["influxql"]
    return "0"


def scope_action_for_fragment(name: str) -> str:
    """Returns the DS 8.4.2 scope transition performed by one child call."""
    if name in {"FRAG_QUERY_BODY", "FRAG_QUERY_BODY_NO_SET"}:
        return "QST_GENERATOR_SCOPE_QUERY_BLOCK"
    if name == "FRAG_TABLE_ALIAS":
        return "QST_GENERATOR_SCOPE_BIND_TABLE_ALIAS"
    return "QST_GENERATOR_SCOPE_NONE"


class CatalogCompiler:
    """Compiles the DS relation AST into generator values and internal nodes."""

    def __init__(self, fragments: list[str], texts: dict[str, str],
                 low_probability_values: set[str],
                 low_probability_empty_fragments: set[str]) -> None:
        self.fragment_ids = {name: f"SQL_DIMENSION_{name}" for name in fragments}
        self.texts = texts
        self.low_probability_values = low_probability_values
        self.low_probability_empty_fragments = low_probability_empty_fragments
        self.fragments: list[dict] = []
        self.next_fragment_id = INTERNAL_FRAGMENT_BASE
        self.next_value_id = INTERNAL_VALUE_BASE

    def compile(self, relations: dict[str, str], fragments: list[str]) -> list[dict]:
        for name in fragments:
            self.current_fragment_name = name
            values = self.compile_expression(RelationParser(relations[name]).parse())
            dialect_mask = dialect_mask_for_fragment(name)
            for value in values:
                value["dialect_mask"] = dialect_mask
            if name == "FRAG_QUERY_ROOT":
                for value in values:
                    value["class"] = "QST_GENERATOR_CLASS_ABNORMAL"
                    value["weight"] = ABNORMAL_WEIGHT
                values.append(
                    {
                        "id": self.allocate_value_id(),
                        "steps": [
                            ("inline", "SQL_DIMENSION_VAL_CLAUSE_SELECT", "SELECT"),
                            ("inline", "SQL_DIMENSION_VAL_INTEGER_1", "1"),
                            ("inline", "SQL_DIMENSION_VAL_QUERY_TERMINATOR_SEMICOLON", ";"),
                        ],
                        "runtime": None,
                        "return_state": "QST_GENERATOR_RETURN_NON_EMPTY",
                        "class": "QST_GENERATOR_CLASS_NORMAL",
                        "weight": NORMAL_SEED_WEIGHT,
                        "dialect_mask": dialect_mask,
                    }
                )
            # DS 8.2/8.3.1 requires deletion mutations at every occurrence.
            # Represent each deletion by whitespace: it removes the dimension
            # token while preserving a nonempty payload for the publication
            # protocol, including paths where every selected dimension is cut.
            # The root has no surrounding SQL. Its deletion must remain an
            # executable negative statement instead of becoming whitespace.
            deletion_value_id = self.allocate_value_id()
            deletion_weight = (0 if name in ZERO_WEIGHT_ABNORMAL_FRAGMENTS
                               else ABNORMAL_WEIGHT)
            values.append(
                {
                    "id": deletion_value_id,
                    "steps": [],
                    "text": "?" if name == "FRAG_QUERY_ROOT" else " ",
                    "runtime": None,
                    "return_state": "QST_GENERATOR_RETURN_EMPTY_RESULT",
                    "class": "QST_GENERATOR_CLASS_ABNORMAL",
                    "weight": deletion_weight,
                    "dialect_mask": dialect_mask,
                }
            )
            self.fragments.append(
                {
                    "id": self.fragment_ids[name],
                    "name": name,
                    "values": values,
                }
            )
        self.mark_recursive_edges()
        return self.fragments

    def mark_recursive_edges(self) -> None:
        fragments_by_id = {fragment["id"]: fragment for fragment in self.fragments}
        state = {fragment_id: 0 for fragment_id in fragments_by_id}

        def visit(fragment_id: str) -> None:
            state[fragment_id] = 1
            fragment = fragments_by_id[fragment_id]
            for value in fragment["values"]:
                steps = []
                for step in value["steps"]:
                    if step[0] not in {"call", "call_loop"}:
                        steps.append(step)
                        continue
                    recursion_cost = 0
                    child_state = state[step[1]]
                    if child_state == 1:
                        # Only DFS back edges consume depth. This removes every
                        # zero-cost cycle without making non-recursive grammar
                        # directions unavailable at depth zero.
                        recursion_cost = 1
                    elif child_state == 0:
                        visit(step[1])
                    steps.append((*step, recursion_cost))
                value["steps"] = steps
            state[fragment_id] = 2

        for fragment_id in fragments_by_id:
            if state[fragment_id] == 0:
                visit(fragment_id)

    def allocate_value_id(self) -> int:
        value_id = self.next_value_id
        self.next_value_id += 1
        return value_id

    def add_synthetic_fragment(self, expression: tuple, purpose: str,
                               include_empty: bool,
                               empty_first: bool = True) -> str:
        fragment_id = str(self.next_fragment_id) + "U"
        self.next_fragment_id += 1
        values = []
        if include_empty and empty_first:
            values.append(
                {
                    "id": self.allocate_value_id(), "steps": [], "runtime": None,
                    "return_state": "QST_GENERATOR_RETURN_EMPTY_RESULT",
                }
            )
        values.extend(self.compile_expression(expression))
        if include_empty and not empty_first:
            values.append(
                {
                    "id": self.allocate_value_id(), "steps": [], "runtime": None,
                    "return_state": "QST_GENERATOR_RETURN_EMPTY_RESULT",
                }
            )
        self.fragments.append(
            {"id": fragment_id, "name": purpose, "values": values}
        )
        return fragment_id

    def compile_expression(self, expression: tuple) -> list[dict]:
        if expression[0] == "alternation":
            result: list[dict] = []
            for choice in expression[1]:
                result.extend(self.compile_expression(choice))
            return result
        if expression[0] != "sequence":
            raise ValueError(f"expected sequence or alternation, got {expression[0]}")
        steps: list[tuple] = []
        runtime_kind = None
        generation_class = "QST_GENERATOR_CLASS_NORMAL"
        for item in expression[1]:
            kind = item[0]
            if kind == "fragment":
                if is_explicit_abnormal_name(item[1]):
                    generation_class = "QST_GENERATOR_CLASS_ABNORMAL"
                steps.append(("call", self.fragment_ids[item[1]],
                              "QST_GENERATOR_CALL_REQUIRED",
                              scope_action_for_fragment(item[1])))
            elif kind == "value":
                if is_explicit_abnormal_name(item[1]):
                    generation_class = "QST_GENERATOR_CLASS_ABNORMAL"
                steps.append(("inline", f"SQL_DIMENSION_{item[1]}", self.texts[item[1]]))
            elif kind == "runtime":
                if len(expression[1]) != 1:
                    raise ValueError("RUNTIME_OBJECT must be the only sequence item")
                runtime_kind = RUNTIME_OBJECT_KINDS[item[1]]
            elif kind == "optional":
                child = self.add_synthetic_fragment(item[1], "optional", True)
                steps.append(("call", child, "QST_GENERATOR_CALL_REQUIRED",
                              "QST_GENERATOR_SCOPE_NONE"))
            elif kind == "ordered_optional":
                child = self.add_synthetic_fragment(
                    item[1], "ordered_optional", True, False)
                steps.append(("call", child, "QST_GENERATOR_CALL_REQUIRED",
                              "QST_GENERATOR_SCOPE_NONE"))
            elif kind == "loop":
                # A loop body needs an explicit empty terminal when validation
                # examines the internal fragment independently. It represents
                # the DS loop controller's zero/stop path, not a DS VAL.
                child = self.add_synthetic_fragment(item[1], "loop", True)
                steps.append(("call_loop", child, "QST_GENERATOR_CALL_LOOP",
                              "QST_GENERATOR_SCOPE_NONE"))
            else:
                raise ValueError(f"unexpected sequence item: {kind}")
        if runtime_kind is not None:
            return [{"id": self.allocate_value_id(), "steps": [], "runtime": runtime_kind,
                     "return_state": "QST_GENERATOR_RETURN_NON_EMPTY",
                     "class": generation_class}]
        value = {"id": self.allocate_value_id(), "steps": steps, "runtime": None,
                 "return_state": "QST_GENERATOR_RETURN_NON_EMPTY",
                 "class": generation_class}
        if any(
            step[0] == "inline" and (
                step[1][len("SQL_DIMENSION_"):] in self.low_probability_values or
                (step[1][len("SQL_DIMENSION_"):] == "VAL_EMPTY" and
                 self.current_fragment_name in
                 self.low_probability_empty_fragments))
            for step in steps
        ):
            value["low_probability"] = True
        return [value]


def write_executable_catalog(lines: list[str], fragments: list[dict]) -> None:
    step_index = 0
    value_array_names: list[str] = []
    for fragment_index, fragment in enumerate(fragments):
        value_array_name = f"kCatalogValues{fragment_index}"
        value_array_names.append(value_array_name)
        step_names: list[str | None] = []
        for value in fragment["values"]:
            if not value["steps"]:
                step_names.append(None)
                continue
            step_name = f"kCatalogSteps{step_index}"
            step_index += 1
            step_names.append(step_name)
            lines.append(f"static const SQstGeneratorStep {step_name}[] = {{")
            for step in value["steps"]:
                if step[0] == "inline":
                    lines.append(
                        f"    {{QST_GENERATOR_STEP_INLINE_VALUE, {step[1]}, {c_string(step[2])}, {{0}}}},"
                    )
                else:
                    loop = step[0] == "call_loop"
                    call = (
                        f"{{{step[1]}, {step[2]}, {step[4]}, "
                        f"{0 if not loop else 0}, {0 if not loop else 1}, "
                        f"{0 if not loop else COMPILED_LOOP_MAX}, {step[3]}}}"
                    )
                    lines.append(
                        f"    {{QST_GENERATOR_STEP_CALL, 0, NULL, {call}}},"
                    )
            lines.extend(["};", ""])
        lines.append(f"static const SQstGeneratorValue {value_array_name}[] = {{")
        for value, step_name in zip(fragment["values"], step_names):
            dialect_mask = value.get("dialect_mask", "0")
            weight = weight_for_value(value)
            if value["runtime"] is not None:
                lines.append(
                    f"    {{{value['id']}U, QST_GENERATOR_VALUE_ATOMIC, "
                    f"{value.get('class', 'QST_GENERATOR_CLASS_NORMAL')}, {value['return_state']}, {weight}, 0, "
                    f"{value['runtime']}, NULL, \"\", 0, NULL, {dialect_mask}}},"
                )
            elif step_name is None:
                lines.append(
                    f"    {{{value['id']}U, QST_GENERATOR_VALUE_ATOMIC, "
                    f"{value.get('class', 'QST_GENERATOR_CLASS_NORMAL')}, {value['return_state']}, {weight}, 0, "
                    f"0, NULL, {c_string(value.get('text', ''))}, 0, NULL, {dialect_mask}}},"
                )
            else:
                lines.append(
                    f"    {{{value['id']}U, QST_GENERATOR_VALUE_COMPOSE, "
                    f"{value.get('class', 'QST_GENERATOR_CLASS_NORMAL')}, {value['return_state']}, {weight}, 0, "
                    f"0, NULL, \"\", sizeof({step_name}) / sizeof({step_name}[0]), {step_name}, {dialect_mask}}},"
                )
        lines.extend(["};", ""])
    lines.append("static const SQstGeneratorFragment kCompiledCatalogFragments[] = {")
    for fragment, value_array_name in zip(fragments, value_array_names):
        lines.append(
            f"    {{{fragment['id']}, 1, sizeof({value_array_name}) / sizeof({value_array_name}[0]), {value_array_name}}},"
        )
    lines.extend(
        [
            "};",
            "",
            "EQstCode qst_sql_dimension_catalog(const SQstGeneratorCatalog **out_catalog) {",
            "  static SQstGeneratorCatalog catalog = {",
            "      SQL_DIMENSION_CONFIG_VERSION,",
            "      SQL_DIMENSION_FRAG_QUERY_ROOT,",
            "      sizeof(kCompiledCatalogFragments) / sizeof(kCompiledCatalogFragments[0]),",
            "      kCompiledCatalogFragments,",
            "      {{0}},",
            "  };",
            "  static int32_t initialized = 0;",
            "  EQstCode code = QST_OK;",
            "",
            "  if (out_catalog == NULL) {",
            "    return QST_E_INVALID_ARGUMENT;",
            "  }",
            "  if (initialized == 0) {",
            "    code = qst_generator_catalog_digest(&catalog, &catalog.digest);",
            "    if (code != QST_OK) {",
            "      return code;",
            "    }",
            "    initialized = 1;",
            "  }",
            "  code = qst_generator_catalog_validate(&catalog);",
            "  if (code != QST_OK) {",
            "    return code;",
            "  }",
            "  *out_catalog = &catalog;",
            "  return QST_OK;",
            "}",
            "",
        ]
    )


def write_source(
    path: Path, fragments: list[str], relations: dict[str, str], values: list[str],
    texts: dict[str, str], low_probability_values: set[str],
    low_probability_empty_fragments: set[str]
) -> None:
    lines = [
        "/* Generated from DS chapter 8. Do not edit by hand. */",
        "",
        "#include <stddef.h>",
        "",
        '#include "qst_sql_dimension_config.h"',
        '#include "sql_dimension_ids.h"',
        "",
        "static const SQstSqlDimensionSourceFragment kSourceFragments[] = {",
    ]
    for name in fragments:
        lines.append(
            f"    {{SQL_DIMENSION_{name}, {c_string(name)}, {c_string(relations[name])}}},"
        )
    lines.extend(["};", "", "static const SQstSqlDimensionSourceValue kSourceValues[] = {"])
    for name in values:
        lines.append(
            f"    {{SQL_DIMENSION_{name}, {c_string(name)}, {c_string(texts[name])}}},"
        )
    lines.extend(
        [
            "};",
            "",
            "EQstCode qst_sql_dimension_source(",
            "    const SQstSqlDimensionSourceFragment **out_fragments,",
            "    uint32_t *out_fragment_count,",
            "    const SQstSqlDimensionSourceValue **out_values,",
            "    uint32_t *out_value_count) {",
            "  if (out_fragments == NULL || out_fragment_count == NULL ||",
            "      out_values == NULL || out_value_count == NULL) {",
            "    return QST_E_INVALID_ARGUMENT;",
            "  }",
            "  *out_fragments = kSourceFragments;",
            "  *out_fragment_count = sizeof(kSourceFragments) / sizeof(kSourceFragments[0]);",
            "  *out_values = kSourceValues;",
            "  *out_value_count = sizeof(kSourceValues) / sizeof(kSourceValues[0]);",
            "  return QST_OK;",
            "}",
            "",
        ]
    )
    compiler = CatalogCompiler(fragments, texts, low_probability_values,
                               low_probability_empty_fragments)
    try:
        compiled_fragments = compiler.compile(relations, fragments)
    except ValueError as error:
        raise SystemExit(f"catalog compilation failed: {error}") from error
    write_executable_catalog(lines, compiled_fragments)
    path.write_text("\n".join(lines), encoding="ascii")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", type=Path, required=True)
    parser.add_argument("--header", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    args = parser.parse_args()

    document = args.ds.read_text(encoding="utf-8")
    relations = extract_relations(document)
    fragments = sorted(set(FRAG_RE.findall(document)))
    values = sorted(set(VAL_RE.findall(document)) - INVALID_VALUES)
    value_rows = extract_value_rows(document)

    if set(relations) != set(fragments):
        missing = sorted(set(fragments) - set(relations))
        extra = sorted(set(relations) - set(fragments))
        raise SystemExit(f"FRAG relation mismatch missing={missing} extra={extra}")
    if set(values) != value_rows:
        missing = sorted(set(values) - value_rows)
        extra = sorted(value_rows - set(values))
        raise SystemExit(f"VAL registration mismatch missing={missing} extra={extra}")
    if len(fragments) != 535 or len(values) != 796:
        raise SystemExit(
            f"unexpected DS source counts fragments={len(fragments)} values={len(values)}"
        )
    validate_relations(relations, values)
    args.header.parent.mkdir(parents=True, exist_ok=True)
    write_header(args.header, fragments, values)
    write_source(
        args.source, fragments, relations, values,
        extract_value_texts(document, values),
        extract_low_probability_values(document, values),
        find_required_fragments(relations))


if __name__ == "__main__":
    main()
