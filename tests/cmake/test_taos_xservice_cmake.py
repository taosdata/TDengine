from pathlib import Path


def test_explorer_ui_pnpm_commands_run_in_ci_mode():
    cmake_file = (
        Path(__file__).resolve().parents[2]
        / "cmake"
        / "taos-xservice.cmake"
    )

    text = cmake_file.read_text(encoding="utf-8")
    start = text.index('add_custom_command(\n    OUTPUT "${_explorer_dist_dir}/index.html"')
    end = text.index("endif()", start)
    block = text[start:end]

    assert 'COMMAND "${CMAKE_COMMAND}" -E env CI=true "${PNPM_EXECUTABLE}" install --frozen-lockfile' in block
    assert 'COMMAND "${CMAKE_COMMAND}" -E env CI=true "${PNPM_EXECUTABLE}" run build' in block
    assert 'COMMAND "${PNPM_EXECUTABLE}" install --frozen-lockfile' not in block
    assert 'COMMAND "${PNPM_EXECUTABLE}" run build' not in block


def test_build_taosx_binary_also_builds_and_deploys_xnoded():
    cmake_file = (
        Path(__file__).resolve().parents[2]
        / "cmake"
        / "taos-xservice.cmake"
    )

    text = cmake_file.read_text(encoding="utf-8")
    start = text.index("if(BUILD_TAOSX_BINARY)")
    end = text.index("endif()", start)
    block = text[start:end]

    assert '${TD_TAOSX_DIR}/xnoded/**/*.rs' in text
    assert 'set(_taosx_xnoded_binary_output "${_taosx_bin_output_dir}/xnoded${CMAKE_EXECUTABLE_SUFFIX}")' in text
    assert '_taosx_deploy_command(xnoded' in block
    assert '"${_taosx_artifact_dir}/xnoded${CMAKE_EXECUTABLE_SUFFIX}"' in block
    assert '"${CARGO_EXECUTABLE}" build -p xnoded ${_taosx_cargo_profile_args}' in block
    assert 'list(APPEND _taosx_binary_outputs "${_taosx_xnoded_binary_output}")' in block
    assert 'COMMAND "${CMAKE_COMMAND}" -E copy_if_different' in block
    assert '"${_taosx_xnoded_binary_output}"' in block
