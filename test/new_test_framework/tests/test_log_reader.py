import importlib.util
from pathlib import Path


def _load_log_reader():
    module_path = (
        Path(__file__).resolve().parents[1] / "utils" / "log_reader.py"
    )
    spec = importlib.util.spec_from_file_location("log_reader", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.LogReader


class TestLogReader:
    def test_reads_appended_complete_lines(self, tmp_path):
        log_path = tmp_path / "taosdlog.0"
        log_path.write_bytes(b"existing line\n")
        reader = _load_log_reader()([str(log_path)])

        with log_path.open("ab") as log:
            log.write(b"new line\n")

        assert reader.read() == "new line\n"

    def test_detects_truncate_then_rewrite_past_old_offset(
        self, tmp_path
    ):
        log_path = tmp_path / "taosdlog.0"
        old_content = b"old line one\nold line two\n"
        log_path.write_bytes(old_content)
        reader = _load_log_reader()([str(log_path)])

        replacement = b"replacement line one\nreplacement line two\n"
        log_path.write_bytes(replacement)

        assert len(replacement) > len(old_content)
        assert reader.read() == replacement.decode()
