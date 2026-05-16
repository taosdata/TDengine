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

### Login CAPTCHA (optional)
Explorer can require a CAPTCHA on every login. It is disabled by default.

In `explorer.toml`:

```toml
[security]
login_captcha = true
```

Or with environment variable:

```bash
EXPLORER_SECURITY_LOGIN_CAPTCHA=true
```

Or with CLI option `--login-captcha`.

Or with CLI option `--x-api`:

```bash
taos-explorer --x-api http://localhost:6050
```
