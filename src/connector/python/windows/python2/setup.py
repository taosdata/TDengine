import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="taos",
    version="1.4.15",
    author="Taosdata Inc.",
    author_email="support@taosdata.com",
    description="TDengine python client package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 2",
        "Operating System :: Windows",
    ],
)
