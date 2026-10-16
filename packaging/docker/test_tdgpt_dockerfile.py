# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from pathlib import Path


DOCKERFILE = Path(__file__).with_name("DockerfileTDgpt.base")
LITE_DOCKERFILE = Path(__file__).with_name("DockerfileTDgpt.lite.base")
IMAGE_DOCKERFILE = Path(__file__).with_name("DockerfileTDgpt")
INSTALL_SCRIPT = Path(__file__).parents[2] / "tools" / "tdgpt" / "script" / "install.sh"
DOCKER_REQUIREMENTS = Path(__file__).parents[2] / "tools" / "tdgpt" / "requirements_docker.txt"


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


def test_lite_image_uses_only_the_base_analytics_requirements():
    text = LITE_DOCKERFILE.read_text(encoding="utf-8")

    assert "ARG PYTHON_BASE=python:3.12.12-slim-trixie" in text
    assert "python3.12 -m venv" in text
    assert "python3.10" not in text
    assert "-r /tmp/requirements_docker.txt" in text
    assert "torch" not in text
    assert "tensorflow" not in text
    assert "accelerate" not in text


def test_lite_requirements_include_http_client():
    text = DOCKER_REQUIREMENTS.read_text(encoding="utf-8")

    assert "requests==2.32.3" in text


def test_lite_target_uses_the_model_free_entrypoint():
    text = IMAGE_DOCKERFILE.read_text(encoding="utf-8")

    assert "AS tdgpt_lite" in text
    assert "entrypoint-lite.sh" in text


def test_lite_entrypoint_starts_api_without_starting_models():
    text = IMAGE_DOCKERFILE.with_name("entrypoint-lite.sh").read_text(encoding="utf-8")

    assert "model-start" not in text
    assert "exec gunicorn" in text


def test_lite_install_creates_dynamic_resource_directories_without_models():
    text = INSTALL_SCRIPT.read_text(encoding="utf-8")
    install_start = text.index("function installProduct()")
    install_product = text[install_start:]

    assert "install_model_dirs\n  if" in install_product
    assert 'if [ "${lite_mode}" != "1" ]; then\n    install_model' in install_product
