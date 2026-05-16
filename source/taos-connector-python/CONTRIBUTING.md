# Contributing

## Dev Env

```bash
pip3 install poetry
```

## Release

Before release, you use must the `PYPI_TOKEN` environment to satisfy the `pip3 publish` requires.

For taosws:

```bash
export PYPI_TOKEN=xxx
./ci/release-ws.sh <new-version>
```

For taospy:

```bash
./ci/release.sh <new-version>
```

Release script will do:

1. Generate changelog.
2. Commit with changelog and version bump.
3. Tag with new version
4. Push commit directly to main branch.
5. Push tags

Note that taosws and taospy use different versions.
