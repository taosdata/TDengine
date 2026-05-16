# Setup development environment

## Use pipx to install isolated environments

On macOS

Install pipx


```
brew install pipx
pipx ensurepath
```

## Install poetry

Install poetry use pipx

```
pipx install poetry
```

Install poetry with specify python version 

```
pipx install --python python3.11 poetry --force
```

## Install dependencies

```
poetry install 
```

## Run tests

```
poetry run pytest tests
```

Run specific test file

```
poetry run pytest tests/test_example.py
```

Run specific test function

```
poetry run pytest tests/test_example.py::test_function_example
```
```

Rest api test

```
export TDENGINE_URL=localhost:6041 
```

