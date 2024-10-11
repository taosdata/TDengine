# TestNG_taosX

## Poetry

[Poetry](https://python-poetry.org/) is used to manage dependencies in this project.

### 1. Install Poetry

```
curl -sSL https://install.python-poetry.org | python3 -
```
Add `export PATH="/root/.local/bin:$PATH"` to your shell configuration file.

### 2. Setup Poetry

Create virtualenv within the root directory of the project.
```
poetry config virtualenvs.in-project true
```

### 3. Install Dependencies

```
poetry install
```

## Allure

[Allure](https://qameta.io/allure-report/) is used to generate test repot in this project.

```
wget https://repo.maven.apache.org/maven2/io/qameta/allure/allure-commandline/2.24.0/allure-commandline-2.24.0.tgz
tar -zxvf allure-commandline-2.24.0.tgz -C /opt/
ln -s /opt/allure-2.24.0/bin/allure /usr/bin/allure
allure --version
```

## Run Test

```
# Please modify the file according to your env
cp setenv.sh.example setenv.sh
source ./setenv.sh
cd test
poetry run pytest --alluredir=../report
```

### More ways to run cases

```shell
# activate venv, then pytest can be run directly
poetry shell

# run a single case
pytest -sv opcua_test.py::test_sanity

# run all opcua cases
pytest -sv opcua_test.py

# run cases by marker
pytest -sv -m sanity

# run case by keyword
pytest -sv opcua_test.py -k observe
```

### List test cases by marker

```shell
pytest -m <marker> --collect-only
```

## Check Report

```
allure serve report
```

## Config

- Env related configs can be found at `config/env.yaml`
- Task related configs can be found at the sub dirs of `config`, which are named after the data source type
- The value of `{}` in `from.fromhost` in task related configs, will be replaced with the value in `config/env.yaml`

## Pre-commit

Use following command to setup pre-commit, which will be trigger during commit.

```shell
poetry install --with=dev
poetry shell
pre-commit install
```
