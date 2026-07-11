from pathlib import Path


DOCKERFILE = Path(__file__).with_name("DockerfileTDgpt.base")


def _venv4_stage() -> str:
    text = DOCKERFILE.read_text(encoding="utf-8")
    start = text.index("FROM ${PYTHON_BASE} AS venv4-builder")
    end = text.index("\nFROM ${PYTHON_BASE}", start + 1)
    return text[start:end]


def test_moirai_arm64_torch_is_pinned_for_uni2ts_compatibility():
    stage = _venv4_stage()

    assert 'TORCH_PKG="torch==2.4.1";' in stage
    assert 'TORCH_PKG="torch";' not in stage


def test_moirai_cleanup_ignores_find_delete_races():
    stage = _venv4_stage()

    assert "find $VIRTUAL_ENV -depth -type d" in stage
    assert ") -exec rm -rf {} + 2>/dev/null || true" in stage
