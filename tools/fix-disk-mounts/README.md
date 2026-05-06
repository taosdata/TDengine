# fix-tdengine-disks

A pure-Bash utility that repairs TDengine metadata files
(`current.json` and `vnodes.json`) after the order of multi-level
storage mount points in `taos.cfg` has been changed.

---

## When to Use This Script

TDengine's Tiered File System (TFS) assigns each configured disk a
numeric ID (`did_id`) **based solely on the order the `dataDir` lines
appear in `taos.cfg`**.  The primary disk always receives `id = 0`;
every subsequent disk at the same level receives `id = 1, 2, 3 …` in
declaration order.

These IDs are persisted inside each vnode's `tsdb/current.json` file
(fields `did.level` / `did.id`).  TDengine uses them at startup to
reconstruct the absolute path to every data file.

**A mismatch between the IDs stored on disk and the IDs implied by the
current `taos.cfg` order causes TDengine to look for files on the wrong
disk**, leading to errors such as:

```text
TSD ERROR tsdbFSDoScanAndFixFile failed since file: … does not exist
VND ERROR failed to open vnode from vnode/vnode2 since No such file or directory
MND ERROR failed to process since Vnode is closed or removed
```

> **Warning – destructive side effect.**  TDengine's internal startup
> check (`tsdbFSDoSanAndFix`) **deletes** any file that is not
> referenced by `current.json`.  If you start taosd with a stale
> `current.json` before running this script, data may be permanently
> lost.  Always run the script first, with `--apply`, and only then
> start taosd.

**Typical scenarios that require this script**:

| Scenario | What went wrong |
|---|---|
| A new disk was inserted in the middle of the `dataDir` list instead of at the end | Existing disks received new IDs; `current.json` still holds old IDs |
| The `dataDir` list was reordered for any reason | Same ID mismatch |
| A disk was removed from the list without migrating its data | Files are still referenced in `current.json` but the disk/path is gone |
| After a cluster re-deployment the storage config was partially rebuilt | IDs may differ from what was written when data was created |

---

## How It Works

### 1 – Rebuild the disk → ID mapping

The script reads every `dataDir` line from `taos.cfg` and assigns
`did_id` values using the same algorithm as `tfsMountDiskToTier` in
`source/libs/tfs/src/tfsTier.c`:

* The disk flagged `primary = 1` at level 0 always gets `id = 0`.
* All other level-0 disks are numbered `1, 2, 3 …` in declaration order.
* Disks at level 1, 2, … are numbered independently starting from 0.

### 2 – Scan and repair `current.json`

For every `current.json` found across all configured disks the script:

1. Extracts every file reference (head / data / sma / tomb / stt
   entries), each described by `fid`, `cid`, and file suffix.
2. Reconstructs the expected filename (`v{vgId}f{fid}ver{cid}.{suffix}`)
   and searches all configured disks for its physical location.
3. Updates `did.level` and `did.id` in the JSON to match the disk where
   the file actually lives.
4. Removes entries for files that cannot be found on any configured disk.
5. Removes entire `fset` blocks whose every file is missing.

### 3 – Repair `vnodes.json`

`vnodes.json` (on the primary disk) stores a `diskPrimary` field for
each vnode — the `did_id` of the level-0 disk that holds the vnode's
main directory (`vnode/vnode{N}/`).

The script determines the correct value by locating each vnode's
`current.json` on disk: whichever disk holds `current.json` is the
primary disk for that vnode.

### Safety

* **Dry-run by default.**  No files are modified unless `--apply` is
  passed.
* **Automatic backup.**  Before overwriting any file the original is
  copied to `<file>.pre-fix.bak` (configurable via `BACKUP_SUFFIX`).

---

## Requirements

| Tool | Notes |
|---|---|
| bash ≥ 4 | Standard on all modern Linux distributions |
| `jq` | JSON processor — `apt install jq` / `yum install jq` |
| `grep`, `sed`, `find`, `sort` | Standard POSIX utilities |

No Python or any other language runtime is required.

---

## Usage

```bash
# 1. Preview what would change (safe, no writes)
bash fix-tdengine-disks.sh

# 2. Apply all repairs
bash fix-tdengine-disks.sh --apply

# 3. Preview repairs for a single vnode
bash fix-tdengine-disks.sh --vnode 3

# 4. Apply repairs for specific vnodes only
bash fix-tdengine-disks.sh --vnode 2,3 --apply

# 5. Use a non-default taos.cfg path
bash fix-tdengine-disks.sh --cfg /data/taos/taos.cfg --apply
```

### Options

| Option | Default | Description |
|---|---|---|
| `--apply` | *(dry-run)* | Write repaired JSON to disk |
| `--vnode <ids>` | all | Comma-separated vgId list to restrict repairs (`2` or `2,3,5`) |
| `--cfg <path>` | `/etc/taos/taos.cfg` | Path to `taos.cfg` |
| `-h` / `--help` | — | Print usage summary |

### Recommended workflow

```text
1. Stop taosd
2. Back up your data directories (optional but strongly recommended)
3. bash fix-tdengine-disks.sh          ← review the dry-run output
4. bash fix-tdengine-disks.sh --apply  ← write the fixes
5. Start taosd and verify
```

---

## Output Example

```text
=================================================================
  TDengine disk fixer  (pure bash + jq)
=================================================================

  Mode         : DRY-RUN
  Backup suffix: .pre-fix.bak
  Vnode filter : all

  Disk map (from /etc/taos/taos.cfg):
    level=0  id=0   /var/lib/taos/data01 [primary]
    level=0  id=1   /var/lib/taos/data02
    level=0  id=2   /var/lib/taos/data03

── current.json ──
  Found 2 current.json file(s).

  /var/lib/taos/data01/vnode/vnode2/tsdb/current.json
      FIX  v2f1736ver3.data: (lv=0,id=2) → (lv=0,id=1)  [/var/lib/taos/data02]
      UNCHANGED  v2f1736ver100.head
    [DRY-RUN] fixed=1 removed=0 fsets_removed=0

── vnodes.json (diskPrimary) ──
  /var/lib/taos/data01/vnode/vnodes.json
    vgId=2    diskPrimary  2 → 1  [/var/lib/taos/data02]
    [DRY-RUN] would fix 1 diskPrimary entries
```

---

## Internals: Key Source Files

| File | Relevance |
|---|---|
| `source/libs/tfs/src/tfsTier.c` | `tfsMountDiskToTier` — assigns `did_id` at startup |
| `source/dnode/vnode/src/tsdb/tsdbFile2.c` | `tfile_to_json` / `tsdbTFileName` — serialises and reconstructs file paths |
| `source/dnode/vnode/src/tsdb/tsdbFS2.c` | `tsdbFSDoSanAndFix` — validates `current.json` at startup and deletes unreferenced files |
| `source/dnode/mgmt/mgmt_vnode/src/vmFile.c` | `diskPrimary` field in `vnodes.json` |
