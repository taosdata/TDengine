# taos-ws-py

This is a Python websocket client for TDengine.

## Init

### mac

install rust

```bash

curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh

source "$HOME/.cargo/env"

```

```bash

pip3 install maturin

```

### linux

install rust

```bash

curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh

source "$HOME/.cargo/env"

```

```bash

pip3 install maturin

```

## Build and Installation

### mac

```bash

maturin build --strip && pip3 install ./target/wheels/taos_ws_py-0.3.2-cp37-abi3-macosx_10_7_x86_64.whl --force-reinstall

```

### linux

```bash

python3 -m maturin build --strip && pip3 install ./target/wheels/taos_ws_py-0.3.2-cp37-abi3-manylinux_2_31_x86_64.whl --force-reinstall

```

## Test

### mac

```bash

pip3 install pytest
# testcase depends on those packages
pip3 install pandas sqlalchemy python-dotenv

export TDENGINE_URL=localhost:6041

pytest tests

```

### linux

```bash

pip3 install pytest
# testcase depends on those packages
pip3 install pandas sqlalchemy python-dotenv

export TDENGINE_URL=localhost:6041

python3 -m pytest tests

```

## Release

```bash

maturin build --release --future-incompat-report --strip
    
```