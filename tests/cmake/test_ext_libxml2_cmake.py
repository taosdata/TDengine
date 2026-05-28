from pathlib import Path


def test_ext_libxml2_windows_static_release_uses_suffixed_library_name():
    cmake_file = (
        Path(__file__).resolve().parents[2]
        / "source"
        / "taos-community"
        / "cmake"
        / "external.cmake"
    )
    text = cmake_file.read_text(encoding="utf-8")
    start = text.index("elseif(TD_WINDOWS)")
    end = text.index("INIT_EXT(ext_libxml2")
    block = text[start:end]

    assert 'set(ext_libxml2_static libxml2sd.lib)' in block
    assert 'set(ext_libxml2_static libxml2s.lib)' in block
    assert 'set(ext_libxml2_static libxml2.lib)' not in block
