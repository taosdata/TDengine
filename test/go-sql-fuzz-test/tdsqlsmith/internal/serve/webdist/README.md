This directory stores generated frontend static assets for embedding into the Go binary.

- Do not commit generated files under this directory.
- Build assets locally with: `(cd web/console && npm run build)`.
- `go:embed` requires at least one embeddable file in `webdist`, so this placeholder is kept in git.
