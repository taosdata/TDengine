# Code Change Log

## 2026-03-27

### fix(catalog): fix heap overflow when fetching virtual child table meta from server

**Files changed:**
- `source/libs/catalog/src/catalog.c`
- `source/libs/catalog/src/ctgAsync.c`

**Root cause:**

When a virtual child table (`TSDB_VIRTUAL_CHILD_TABLE`) has no local meta cache and its meta is fetched from the server (`CTG_IS_META_VBOTH` path), the `ctgCloneMetaOutput()` function allocates `output->tbMeta` with `metaSize + schemaExtSize` bytes only (without space for `colRef`/`tagRef` data, because `tbMeta->colRef` is NULL at clone time).

Subsequently, in `ctgGetTbMeta()` (catalog.c), the code sets `tbMeta->colRef = (char*)tbMeta + metaSize + schemaExtSize` and tries to copy `colRefSize` bytes (e.g., 1564 bytes) into the buffer — but only 492 bytes were allocated, causing a **heap-buffer-overflow**.

Two additional bugs existed in `ctgAsync.c` where the `else` branch (tbMeta == NULL) used incorrect C pointer arithmetic:
- `pOut->tbMeta + sizeof(STableMeta)` advances by `sizeof(STableMeta) * sizeof(STableMeta)` bytes (pointer arithmetic, not byte arithmetic)
- `pOut->vctbMeta + sizeof(SVCTableMeta)` — same wrong pattern; should use `pOut->vctbMeta->colRef`

**Fix:**

1. **catalog.c** (`ctgGetTbMeta`): Add `taosMemoryRealloc` to expand `tbMeta` to `metaSize + schemaExtSize + colRefSize + tagRefSize` before writing colRef/tagRef data. Also add `tagRef` handling and `numOfTagRefs` assignment for completeness.

2. **ctgAsync.c** (`ctgHandleGetTbMetaRsp`, line ~1810): Fix pointer arithmetic — set `colRef` pointer first, then copy data via `pOut->tbMeta->colRef`.

3. **ctgAsync.c** (`ctgHandleGetTbMetasRsp` and `ctgHandleGetTbNamesRsp`, lines ~2031, ~2255): Same pointer arithmetic fix.

**Reproducer:** `test_tmq_vtable.py` with local cache cleared + ASAN build triggers ASAN heap-buffer-overflow in `catalog.c:238`.
