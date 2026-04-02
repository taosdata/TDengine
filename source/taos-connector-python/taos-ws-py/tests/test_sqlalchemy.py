import ast
import importlib
from pathlib import Path

import pytest
import taosws
import utils

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import types as sqltypes
from sqlalchemy.engine.url import make_url


def test_read():
    db_name = "test_1774860246"
    setup_conn = taosws.connect("ws://localhost:6041")
    try:
        setup_conn.execute(f"drop database if exists {db_name}")
        setup_conn.execute(f"create database {db_name}")
        setup_conn.execute(f"create table {db_name}.meters (ts timestamp, c1 int, c2 double) tags (t1 int)")
        setup_conn.execute(
            f"insert into {db_name}.d0 using {db_name}.meters tags(0) values (1733189403001, 1, 1.11) (1733189403002, 2, 2.22)"
        )
        setup_conn.execute(
            f"insert into {db_name}.d1 using {db_name}.meters tags(1) values (1733189403003, 3, 3.33) (1733189403004, 4, 4.44)"
        )
        setup_conn.execute(f"create table {db_name}.ntb (ts timestamp, age int)")
        setup_conn.execute(f"insert into {db_name}.ntb values (now, 23)")

        engine = create_engine(
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:6041?timezone=Asia/Shanghai"
        )
        conn = engine.connect()
        try:
            inspection = inspect(engine)

            databases = inspection.get_schema_names()
            assert db_name in databases
            assert inspection.get_table_names(db_name) == ["meters", "ntb"]

            expected_columns = [
                {"name": "ts", "type": inspection.dialect._resolve_type("TIMESTAMP")},
                {"name": "c1", "type": inspection.dialect._resolve_type("INT")},
                {"name": "c2", "type": inspection.dialect._resolve_type("DOUBLE")},
                {"name": "t1", "type": inspection.dialect._resolve_type("INT")},
            ]
            columns = inspection.get_columns("meters", db_name)
            for index, column in enumerate(columns):
                expected = expected_columns[index]
                assert column["name"] == expected["name"]
                assert type(column["type"]) == expected["type"]

            assert inspection.has_table("meters", db_name) is True
            assert inspection.dialect.has_schema(conn, db_name) is True
        finally:
            conn.close()
    finally:
        setup_conn.execute(f"drop database if exists {db_name}")
        setup_conn.close()


def test_read_failover():
    db_name = "test_1774920255"
    conn = taosws.connect(f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:6041")
    conn.execute(f"drop database if exists {db_name}")
    conn.execute(f"create database {db_name}")

    try:
        urls = [
            "taosws://",
            "taosws://localhost",
            "taosws://localhost:6041",
            f"taosws://localhost:6041/{db_name}",
            f"taosws://root@localhost:6041/{db_name}",
            f"taosws://root:@localhost:6041/{db_name}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:6041/{db_name}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:6041/{db_name}?hosts=",
            f"taosws://{utils.test_username()}:{utils.test_password()}@/{db_name}?hosts=localhost:6041",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:6041/{db_name}?hosts=localhost:6041",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:6041/{db_name}?hosts=localhost:6041,127.0.0.1:6041",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:6041/{db_name}?hosts=localhost:6041,127.0.0.1:6041&timezone=Asia/Shanghai",
        ]

        for url in urls:
            engine = create_engine(url)
            econn = engine.connect()
            econn.close()

        invalid_urls = [
            "taosws://:6041",
            f"taosws://:taosdata@=localhost:6041/{db_name}",
        ]

        for url in invalid_urls:
            with pytest.raises(Exception):
                engine = create_engine(url)
                econn = engine.connect()
                econn.close()

    finally:
        conn.execute(f"drop database if exists {db_name}")
        conn.close()


def test_taosws_sqlalchemy_module_is_available():
    module = importlib.import_module("taosws.sqlalchemy")
    assert hasattr(module, "TaosWsDialect")


def test_legacy_taosws_submodule_alias_is_available():
    module = importlib.import_module("taosws.taosws")
    assert hasattr(module, "connect")
    assert module.connect is taosws.connect


def test_resolve_type_covers_all_declared_tdengine_types():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()

    for tdengine_type, sqlalchemy_type in module.TYPES_MAP.items():
        assert dialect._resolve_type(tdengine_type) is sqlalchemy_type

    assert dialect._resolve_type("TYPE_NOT_EXISTS") is sqltypes.UserDefinedType


def test_create_connect_args_prefers_hosts_and_keeps_other_query_params():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()
    url = make_url(
        "taosws://root:taosdata@localhost:6041/test_1774860246?"
        "hosts=localhost:6041,127.0.0.1:6041&timezone=Asia/Shanghai"
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["taosws://root:taosdata@localhost:6041,127.0.0.1:6041/test_1774860246?timezone=Asia/Shanghai"]
    assert kwargs == {}


def test_sqlalchemy_module_does_not_depend_on_taospy_imports():
    module = importlib.import_module("taosws.sqlalchemy")
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                top_level_name = alias.name.split(".")[0]
                assert top_level_name not in {"taos", "taospy"}
