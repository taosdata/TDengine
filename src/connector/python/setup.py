import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="taos",
    version="2.1.0",
    author="Taosdata Inc.",
    author_email="support@taosdata.com",
    description="TDengine python client package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/taosdata/TDengine/tree/develop/src/connector/python",
    packages=setuptools.find_packages(),
    classifiers=[
        "Environment :: Console",
        "Environment :: MacOS X",
        "Environment :: Win32 (MS Windows)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Operating System :: MacOS",
        "Programming Language :: Python :: 2.7",
        "Operating System :: Linux",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: Microsoft :: Windows :: Windows 10",
    ],
)
