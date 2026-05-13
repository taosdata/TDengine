import toml
from pathlib import Path
import taos


def test_versions_are_in_sync():
    """Checks if the pyproject.toml and package.__init__.py __version__ are in sync."""

    path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    pyproject = toml.loads(open(str(path), encoding="utf-8").read())
    pyproject_version = pyproject["tool"]["poetry"]["version"]

    package_init_version = taos.__version__

    assert package_init_version == pyproject_version
