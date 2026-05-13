# Project Guidelines — plugins/pspace

## Architecture

Java CLI plugin (picocli) wrapping the pSpace time-series database SDK. Modes: `check`, `nodes`, `points`, `query`, `subscribe`, `querySync`. Entry point: `TaosXpSpaceMain`. All config is TOML-based (see `example/`). Communication with taosX happens via `[report].remote` (IPC socket or TCP).

Key packages:

- `com.taosdata.taosx.pspace.config` — TOML configuration models (`Configuration`, `Connection`, `NodesConfig`, `PointsConfig`, `CommandMode`)
- `com.taosdata.taosx.pspace` — domain logic (`Check`, `Nodes`, `Points`, `CheckResult`, `Node`, `Point`)

## Code Style

- Java 8 source level; use Lombok (`@Data`, `@NoArgsConstructor`) for boilerplate.
- Logging via SLF4J → Log4j2 (`src/main/resources/log4j2.xml`).
- CLI parsing via picocli annotations; JSON output via Gson.
- Exclude passwords from `toString()` (see `Connection.java` `@ToString(exclude = "password")`).

## Build and Test

```bash
# Pre-requisite: install pSpace SDK into local Maven repo (one-time)
mvn install:install-file \
  -Dfile=./sdk/pSpace-javaSDK-2.1.10-jar-with-dependencies.jar \
  -DgroupId=com.sunwayland.pspace -DartifactId=pSpace-javaSDK \
  -Dversion=2.1.10 -Dpackaging=jar

# Build fat JAR
mvn clean package              # → target/taosx-pspace.jar

# Or from repo root via cargo-make
cargo make taosx-pspace        # runs: mvn package -f plugins/pspace/pom.xml

# Run unit tests
mvn test
```

## Project Conventions

- Config examples live in `example/*.toml`; keep them in sync when adding new config fields.
- `version.properties` is Maven-filtered at build time — do not hard-code version strings.
- Git commit info is injected via `git-commit-id-plugin` into `git.properties`.
- Fat JAR built with `maven-shade-plugin`; SLF4J classes from pSpace SDK are excluded to avoid duplicates.

## Security

- Never log raw passwords; use `Connection.getMaskedPassword()` for display.
- Example configs contain placeholder credentials — do not commit real ones.

## Documentation

- [Design Documents](docs/dev/design) - Module-level design decisions and business rules
