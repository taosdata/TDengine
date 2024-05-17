# Explorer

## Online Demo

[Preview.](http://192.168.0.201:6060)

## Project setup

```shell
# clone the project
git clone https://github.com/taosdata/explorer.git
cd explorer
yarn

# develop
yarn dev

# develop community
COMMUNITY=community yarn dev
```

### Build

Build frontend distribution.

```shell
yarn build
```

Build explorer binary.

```shell
cargo build --release
```

### Usage

```text
Usage: taos-explorer [OPTIONS]

Options:
  -p, --port <PORT>
          Port

          [env: EXPLORER_PORT=]
          [default: 6060]

  -v, --verbose...
          More output per occurrence

  -q, --quiet...
          Less output per occurrence

  -x, --x-api <X_API>
          API end point for data streaming task management

          [env: EXPLORER_X_API=]
  -C, --cluster <CLUSTER>
  
  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```
