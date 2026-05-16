import importlib
import importlib.util
import pkgutil
import sys


__path__ = pkgutil.extend_path(__path__, __name__)


def _load_native_module():
    full_name = f"{__name__}._taosws"
    if importlib.util.find_spec(full_name) is not None:
        return importlib.import_module(full_name)

    raise ImportError(
        "Failed to import native extension 'taosws._taosws'. " "Ensure taos-ws-py is built and installed correctly."
    )


_native = _load_native_module()

sys.modules.setdefault(f"{__name__}.taosws", _native)

if hasattr(_native, "__all__"):
    for name in _native.__all__:
        globals()[name] = getattr(_native, name)
else:
    for name in dir(_native):
        if not name.startswith("_"):
            globals()[name] = getattr(_native, name)

__doc__ = _native.__doc__
if hasattr(_native, "__all__"):
    __all__ = _native.__all__
