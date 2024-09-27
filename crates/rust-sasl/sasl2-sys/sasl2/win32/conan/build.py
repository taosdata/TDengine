import os

from conan.packager import ConanMultiPackager
from conans import tools

if __name__ == "__main__":
    runtimes = ["MD", "MDd"]
    for subdir in ["sasl2", "sasldb", "gssapiv2"]:
        ref = os.environ.get("CONAN_REFERENCE", "")
        if ref:
            name, ver = ref.split("/", 1)
            os.environ["CONAN_REFERENCE"] = "cyrus-sasl-" + subdir + "/" + ver
        with tools.chdir(os.path.join("win32", "conan", subdir)):
            builder = ConanMultiPackager(visual_runtimes=runtimes)
            builder.add_common_builds(shared_option_name=False, pure_c=True)
            builder.run()
