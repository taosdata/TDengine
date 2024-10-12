# TDengine Explorer

## Basics

Open the default UI in browser at <http://explorer:6060>.

You can view the databases and tables with a tree structure.

## Advance

TDengine Explorer helps you manage the data streaming staff in a unified visual tool.

It use taosX for data replication/backup/subscription.

You can either set it in an TOML file like:

```toml
x_api = "http://localhost:6050"
```

Or with environment `EXPLORER_X_API=http://localhost:6050`.

Or with CLI option `--x-api`:

```bash
taos-explorer --x-api http://localhost:6050
```
