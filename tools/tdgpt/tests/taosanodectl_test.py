from taosanodectl import build_parser, translate_args


def test_model_start_translation():
    args = build_parser().parse_args(["model", "start", "moirai"])
    assert translate_args(args) == ["model-start", "moirai"]


def test_model_start_timeout_translation():
    args = build_parser().parse_args(["model", "start", "moment", "--timeout", "300"])
    assert translate_args(args) == ["model-start", "moment", "--timeout", "300"]


def test_resource_import_translation():
    args = build_parser().parse_args([
        "resource", "import", "--package", "runtime.tar.gz",
        "--expected-sha256", "a" * 64, "--replace",
    ])
    assert translate_args(args) == [
        "resource-import", "--package", "runtime.tar.gz",
        "--expected-sha256", "a" * 64, "--replace",
    ]


def test_service_wait_ready_translation():
    args = build_parser().parse_args(["service", "wait-ready", "--timeout", "45"])
    assert translate_args(args) == ["wait-ready", "--timeout", "45"]


def test_service_wait_ready_uses_configured_default():
    args = build_parser().parse_args(["service", "wait-ready"])
    assert translate_args(args) == ["wait-ready"]


def test_resource_apply_translation():
    args = build_parser().parse_args([
        "resource", "apply", "moirai", "--release", "1.0.0",
        "--resource-base-url", "https://download.taosdata.com",
    ])
    assert translate_args(args) == [
        "resource-apply", "moirai", "--release", "1.0.0",
        "--resource-base-url", "https://download.taosdata.com",
    ]


def test_resource_verify_translation():
    args = build_parser().parse_args(["resource", "verify", "all"])
    assert translate_args(args) == ["resource-verify", "all"]
