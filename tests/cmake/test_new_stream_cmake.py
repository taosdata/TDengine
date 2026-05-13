from pathlib import Path


def test_new_stream_uses_rocksdb_wrapper_macro():
    cmake_file = (
        Path(__file__).resolve().parents[2]
        / "source"
        / "taos-community"
        / "source"
        / "libs"
        / "new-stream"
        / "CMakeLists.txt"
    )

    text = cmake_file.read_text(encoding="utf-8")

    assert "DEP_td_rocksdb(new-stream)" in text
    assert "DEP_ext_rocksdb(new-stream)" not in text
