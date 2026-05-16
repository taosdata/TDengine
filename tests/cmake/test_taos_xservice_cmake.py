import json
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
    assert 'add_custom_target(taosx_binary' in block
    assert 'DEPENDS "${_taosx_binary_output}" "${_taosx_xnoded_binary_output}"' in block
    assert 'COMMAND "${CMAKE_COMMAND}" -E copy_if_different' in block
    assert '"${_taosx_xnoded_binary_output}"' in block


def test_upx_download_is_serialized_through_single_custom_target():
    cmake_file = (
        Path(__file__).resolve().parents[2]
        / "cmake"
        / "taos-xservice.cmake"
    )

    text = cmake_file.read_text(encoding="utf-8")
    upx_setup_block = text[
        text.index("if(_taosx_need_binaries AND _taosx_enable_upx)"):
        text.index("# ── Helper: deploy command for a single binary")
    ]

    assert 'add_custom_target(taosx_upx' in text
    assert 'DEPENDS "${_taosx_upx_binary}"' in text
    assert 'if(_taosx_upx_binary AND NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin")\n    add_custom_target(taosx_upx' in upx_setup_block
    assert 'list(APPEND _taosx_binary_extra_deps\n      "${_taosx_upx_binary}"\n      taosx_upx\n    )' in text

    taosx_block = text[text.index("if(BUILD_TAOSX_BINARY)"):text.index("if(BUILD_TAOSX_AGENT)")]
    agent_block = text[text.index("if(BUILD_TAOSX_AGENT)"):text.index("if(BUILD_EXPLORER)")]
    explorer_block = text[text.index("if(BUILD_EXPLORER)"):text.index("add_custom_target(taosx ALL")]

    assert '${_deploy_cmd_taosx}' in taosx_block
    assert 'DEPENDS ${_taosx_dep_files} ${_taosx_binary_extra_deps}' in taosx_block
    assert '${_deploy_cmd_agent}' in agent_block
    assert 'DEPENDS ${_taosx_dep_files} ${_taosx_binary_extra_deps}' in agent_block
    assert '${_deploy_cmd_explorer}' in explorer_block
    assert 'DEPENDS ${_taosx_dep_files} ${_taosx_binary_extra_deps} "${_explorer_dist_dir}/index.html"' in explorer_block


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


def test_explorer_package_json_allows_native_image_tools_to_build_under_pnpm():
    package_json = (
        Path(__file__).resolve().parents[2]
        / "source"
        / "taos-xservice"
        / "explorer"
        / "package.json"
    )

    data = json.loads(package_json.read_text(encoding="utf-8"))
    built_dependencies = data.get("pnpm", {}).get("onlyBuiltDependencies", [])

    assert "mozjpeg" in built_dependencies
    assert "pngquant-bin" in built_dependencies
    assert "cwebp-bin" in built_dependencies


def test_windows_make_install_script_avoids_pdb_archive_batch_loop():
    make_install = (
        Path(__file__).resolve().parents[2]
        / "source"
        / "taos-community"
        / "packaging"
        / "tools"
        / "make_install.bat"
    )

    text = make_install.read_text(encoding="utf-8")

    assert "set pdb_archive=%binary_dir%\\\\symbols\\\\%verNumber%" in text
    assert 'if exist "%binary_dir%\\\\build\\\\bin\\\\*.pdb" (' in text
    assert 'copy "%binary_dir%\\\\build\\\\bin\\\\*.pdb" "%pdb_archive%\\\\" > nul' in text
    assert "for %%f in (%binary_dir%\\\\build\\\\bin\\\\*.pdb) do (" not in text


def test_linux_install_script_maps_oem_explorer_services_to_explorer_toml():
    install_sh = (
        Path(__file__).resolve().parents[2]
        / "source"
        / "taos-community"
        / "packaging"
        / "tools"
        / "install.sh"
    )

    text = install_sh.read_text(encoding="utf-8")

    assert '*-explorer) echo "explorer.toml" ;;' in text


def test_linux_install_script_uses_computed_service_names_for_config_mapping():
    install_sh = (
        Path(__file__).resolve().parents[2]
        / "source"
        / "taos-community"
        / "packaging"
        / "tools"
        / "install.sh"
    )

    text = install_sh.read_text(encoding="utf-8")
    start = text.index("function get_config_file() {")
    end = text.index("\n}\n", start)
    block = text[start:end]

    assert '"${serverName}") echo "${PREFIX}.cfg" ;;' in block
    assert '"${adapterName}") echo "${adapterName}.toml" ;;' in block
    assert '"${xname}") echo "${xname}.toml" ;;' in block
    assert '"${keeperName}") echo "${keeperName}.toml" ;;' in block
