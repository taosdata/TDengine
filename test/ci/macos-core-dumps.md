# macOS CTest core archives

Core setup and collection are optional diagnostics. Setup failures produce a warning and `core-setup.json`; CTest still runs. Each core is handled independently. After identifying ownership, the collector saves the raw core before generating symbols. Symbol failures are recorded in the index and do not prevent core retention. The original core is removed only after the retained core and its metadata have been written.

Download the job's `macos-ci-artifacts` artifact and inspect `core-collection.log`, `core-state.json`, and `core-archives.json`. The index records absolute archive paths on the macOS runner that executed the job. Copy the needed raw `core.*`, `.images.txt`, `.json`, `.executable`, and `.dSYM` files/directories from that runner using SSH/SCP access. Large core files are kept uncompressed on the runner and are not uploaded as GitLab artifacts. Before the build is deleted, the collector copies the exact crashing executable and generates its dSYM using the still-available build objects. Only executables associated with identified cores are retained. Load the retained executable and dSYM into LLDB for analysis; the JSON sidecar records the commit and executable UUIDs. These files share the core archive's seven-day retention and KEEP policy.

Completed archives live under `/Users/taos/ci-repro/tsdb/unit-cores/job-<job-id>/`. The CI after-script removes completed archives older than 7 days, based on `completed.json`. It skips directories without a completion marker, symlinks, foreign-owned directories, partial archives, and directories containing a `KEEP` marker. Cleanup logs every deletion and continues after individual failures. Unfinished archives and original cores left in `/cores` after collection failures require manual investigation and cleanup; this policy does not delete shared `/cores` files indiscriminately.

The collector verifies executable/dSYM UUID equality and DWARF compile-unit/function information. dsymutil warnings, empty symbols, and UUID mismatches count as symbol failures. If collection or symbol generation fails, the after-script moves the build directory into the job archive's `debug/` directory before removing the repro directory. `core-preserved-build.json` records its original and retained paths; restore the original path or adjust debug-map paths when regenerating symbols from relocated objects. The preserved build shares the same seven-day retention and KEEP policy. If preserving the build fails, the after-script logs the error and skips repro deletion. A successful invocation retains only the core, crashing executable, symbols, and metadata.

To preserve an investigation beyond 7 days, create `KEEP` in its job archive directory before expiry. Remove it when the investigation finishes. To preview or run cleanup manually:

```sh
python3 source/taos-community/test/ci/macos_core_dumps.py --cleanup-archives /Users/taos/ci-repro/tsdb/unit-cores --retention-days 7 --dry-run
python3 source/taos-community/test/ci/macos_core_dumps.py --cleanup-archives /Users/taos/ci-repro/tsdb/unit-cores --retention-days 7
```

The runner must already permit core generation (`kern.coredump=1`, absolute `kern.corefile` ending in `core.%P`, nonzero hard core limit, and write permission on the core directory). CI does not change global sysctl or directory ACLs. Missing prerequisites disable diagnostics for that invocation without blocking tests.
