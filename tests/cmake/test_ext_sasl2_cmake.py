from pathlib import Path


def test_ext_sasl2_forces_prototypes_for_legacy_md5_headers():
    cmake_file = (
        Path(__file__).resolve().parents[2]
        / "source"
        / "taos-community"
        / "cmake"
        / "external.cmake"
    )
    text = cmake_file.read_text(encoding="utf-8")
    start = text.index("ExternalProject_Add(ext_sasl2")
    end = text.index("add_dependencies(build_externals ext_sasl2)")
    block = text[start:end]

    patch_command = (
        'COMMAND sed -i "s/#define PROTOTYPES 0/#define PROTOTYPES 1/" '
        "include/makemd5.c saslauthd/md5global.h"
    )

    assert patch_command in block
    assert "COMMAND ./autogen.sh" in block
    assert block.index("COMMAND ./autogen.sh") < block.index(patch_command)
    assert "--with-saslauthd=no" in block
    assert "--with-authdaemond=no" in block
    assert r'CFLAGS=-std=gnu17\ -Wno-missing-braces' not in block
    assert "patch_ext_sasl2.py" not in block
    assert 'COMMAND python3 -c "' not in block


def test_ext_msvcregex_builds_from_archive_subdirectory():
    cmake_file = (
        Path(__file__).resolve().parents[2]
        / "source"
        / "taos-community"
        / "cmake"
        / "external.cmake"
    )
    text = cmake_file.read_text(encoding="utf-8")
    start = text.index("ExternalProject_Add(ext_msvcregex")
    end = text.index("add_dependencies(build_externals ext_msvcregex)")
    block = text[start:end]

    assert 'set(ext_msvcregex_archive_source "${ext_msvcregex_source}/libgnurx-msvc-master")' in text
    assert 'COMMAND "${CMAKE_COMMAND}" -E chdir "${ext_msvcregex_archive_source}" nmake /f NMakefile all test test2 test3' in block
    assert '"${ext_msvcregex_archive_source}/regex.h"' in block
    assert '"${ext_msvcregex_archive_source}/${ext_msvcregex_static}"' in block
