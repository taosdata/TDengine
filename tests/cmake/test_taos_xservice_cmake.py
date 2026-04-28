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
    assert 'COMMAND "${CMAKE_COMMAND}" -E env' in block
    assert 'CI=true' in block
    assert '"${PNPM_EXECUTABLE}" run build' in block
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


def test_explorer_ui_build_exports_oem_metadata_in_ci_mode():
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
    assert 'COMMAND "${CMAKE_COMMAND}" -E env' in block
    assert '"CUS_PROMPT=${BUILD_CUS_PROMPT}"' in block
    assert '"CUS_NAME=${BUILD_CUS_NAME}"' in block
    assert '"VER_NUMBER=${BUILD_VER_NUMBER}"' in block
    assert '"${PNPM_EXECUTABLE}" run build' in block


def test_explorer_oem_assets_use_custom_title_and_neutral_labels():
    repo_root = Path(__file__).resolve().parents[2]

    index_html = (repo_root / "source" / "taos-xservice" / "explorer" / "index.html").read_text(
        encoding="utf-8"
    )
    sider_vue = (
        repo_root
        / "source"
        / "taos-xservice"
        / "explorer"
        / "src"
        / "layout"
        / "components"
        / "Sider"
        / "index.vue"
    ).read_text(encoding="utf-8")
    router_ts = (
        repo_root
        / "source"
        / "taos-xservice"
        / "explorer"
        / "src"
        / "router"
        / "index.ts"
    ).read_text(encoding="utf-8")
    user_vue = (
        repo_root
        / "source"
        / "taos-xservice"
        / "explorer"
        / "src"
        / "views"
        / "8_administrator"
        / "views"
        / "user.vue"
    ).read_text(encoding="utf-8")
    lang_en = (
        repo_root
        / "source"
        / "taos-xservice"
        / "explorer"
        / "src"
        / "lang"
        / "en"
        / "taosuser.ts"
    ).read_text(encoding="utf-8")
    lang_zh = (
        repo_root
        / "source"
        / "taos-xservice"
        / "explorer"
        / "src"
        / "lang"
        / "zh"
        / "taosuser.ts"
    ).read_text(encoding="utf-8")

    assert "<%= VITE_APP_CUS_NAME %>" in index_html
    assert "<% VITE_APP_CUS_NAME %>" not in index_html
    assert "show: flag ? false : true" in sider_vue
    assert "import { $IS_OEM } from '@/utils/init';" not in router_ts
    assert "path: 'idmp'" in router_ts
    assert "tr('taosuser.dbUserName', 'Database Username')" in user_vue
    assert "TSDB ${tr('userName', 'Username')}" not in user_vue
    assert "dbUserName: 'Database User Name'" in lang_en
    assert "dbUserName: '数据库用户'" in lang_zh
