#!/usr/bin/env python3
"""Regenerate the export whitelist of libtaosnative.so (GNU ld version script).

Why: libtaosnative.so is the single provider of the engine for libtaos.so (a thin
driver dispatcher) and for the CLI tools, which resolve engine symbols from it
instead of embedding their own copy of the static stack.  Everything those
consumers import must therefore stay in its dynamic symbol table; hiding the rest
keeps the internal/third-party surface out of the ABI and lets --gc-sections drop
the code behind it.

The whitelist is the union of
  1. every symbol the in-package consumers import from this library,
  2. the public C ABI declared with DLL_EXPORT in include/client/taos.h,
  3. symbols already whitelisted, and taos_* helpers (undocumented helpers that
     out-of-tree connectors call).

(1) alone is not sufficient -- needs are configuration dependent (Release drops
`taosAssertDebug` with NDEBUG; Debug keeps the tmsg.h wrappers that call
`tEncodeTSma`/`tDecodeTSma`; a BUILD_TEST=ON tree adds the TMQ API consumers) --
so the list is MONOTONIC: regenerating from a smaller tree can only add, never
hide.  Regenerate/--check in every configuration that is built; monotonicity
makes them converge on one union instead of fighting each other.  Over-exporting
costs a little size; under-exporting breaks consumers at link or load time.
`--check` reports whether the checked-in file matches what the generator would
emit.

Only symbols defined in the library are considered, so symbols provided by other
libraries on a consumer's link line (libc, arrow, ...) never leak in.

Usage (from a configured build tree):
    python3 source/taos-community/cmake/gen_export_ver.py <build_dir> [--check]
e.g.
    python3 source/taos-community/cmake/gen_export_ver.py /root/tsdb/release
"""
import glob
import os
import re
import subprocess
import sys

SONAME = "libtaosnative.so"
DISPATCHER_SONAME = "libtaos.so"
# The driver's table of public entry points; the dispatcher reads this instead of
# dlsym()ing the public names (see source/client/src/taosApiTable.c).
NATIVE_API_TABLE = "tdNativeApiTable"
VER_REL = "source/taos-community/source/client/taosnative.ver"
PUBLIC_HEADER = "source/taos-community/include/client/taos.h"


def run(cmd):
    """Run cmd and return stdout, failing loudly on error."""
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed (rc={r.returncode}): {r.stderr.strip()}")
    return r.stdout


def elf_files(root):
    out = []
    for dirpath, _dirs, files in os.walk(root):
        for fn in files:
            p = os.path.join(dirpath, fn)
            try:
                with open(p, "rb") as f:
                    if f.read(4) != b"\x7fELF":
                        continue
            except OSError:
                continue
            out.append(p)
    return out


def dt_needed(p):
    return set(re.findall(r"Shared library: \[([^\]]+)\]", run(["readelf", "-d", p])))


def symbols(p):
    """(undefined, defined) symbol names of an object file or library."""
    undef, deff = set(), set()
    for line in run(["nm", "--format=posix", p]).splitlines():
        f = line.split()
        if len(f) < 2:
            continue
        name = f[0].split("@")[0]
        (undef if f[1] in ("U", "w") else deff).add(name)
    return undef, deff


def link_txt_files(build_dir):
    """Every CMakeFiles/<target>.dir/link.txt in the build tree."""
    return glob.glob(os.path.join(build_dir, "**", "CMakeFiles", "*.dir", "link.txt"),
                     recursive=True)


def archive_to_target_dir(links):
    """Map an in-tree static library path -> the <target>.dir holding its objects."""
    out = {}
    for link in links:
        base = os.path.dirname(os.path.dirname(link))       # make's cwd for this target
        try:
            txt = open(link, encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        if not re.search(r"(^|/)ar\b", txt):                # only static-library rules
            continue
        for tok in txt.split():
            if tok.endswith(".a"):
                out.setdefault(os.path.normpath(os.path.join(base, tok)),
                               os.path.dirname(link))
                break
    return out


def object_consumers(build_dir):
    """Pre-link needs of the consumers and their in-tree static helpers.

    Deliberately independent of link success: a target that *fails* to link is
    usually failing because of an incomplete whitelist, so relying on its binary
    would mean the list could never heal itself.

    Two details matter for accuracy:

    * A target's need is `undefined - defined` computed over its own objects
      **plus** the objects of the in-tree static libraries it links (compatAvro,
      toolscJson, os/util/..., whatever it uses).  Leaving the archives out would
      report everything they provide as a dependency of libtaosnative.so and
      over-export it -- which costs size and can resurrect code --gc-sections
      would otherwise drop.
    * Targets that link libtaosnative.so directly are included as well: their
      engine references are real needs of this library too (BUILD_TEST=ON builds
      such consumers).  Only symbols that no other library on the line provides
      survive, because `keep` is intersected with what this library defines.
    """
    links = link_txt_files(build_dir)
    archives = archive_to_target_dir(links)
    need = set()
    scanned = set()
    for link in links:
        try:
            txt = open(link, encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        if SONAME not in txt and DISPATCHER_SONAME not in txt:
            continue
        base = os.path.dirname(os.path.dirname(link))
        dirs = [os.path.dirname(link)]
        for tok in txt.split():
            if tok.endswith(".a"):
                d = archives.get(os.path.normpath(os.path.join(base, tok)))
                if d:
                    dirs.append(d)
        undef, deff, objs_seen = set(), set(), 0
        for d in dirs:
            objs = glob.glob(os.path.join(d, "**", "*.o"), recursive=True)
            if not objs:
                continue
            scanned.add(os.path.relpath(d, build_dir))
            objs_seen += len(objs)
            for o in objs:
                u, df = symbols(o)
                undef |= u
                deff |= df
        if objs_seen:
            need |= (undef - deff)
    return need, sorted(scanned)


def undefined(p):
    """Undefined dynamic symbols (POSIX nm format: 'name U ...')."""
    return {l.split()[0].split("@")[0] for l in
            run(["nm", "-D", "--undefined-only", "--format=posix", p]).splitlines()
            if len(l.split()) >= 2 and l.split()[1] in ("U", "w")}


def defined(p):
    """Defined dynamic symbols."""
    return {l.split()[0].split("@")[0] for l in
            run(["nm", "-D", "--defined-only", "--format=posix", p]).splitlines()
            if len(l.split()) >= 2 and l.split()[1] not in ("U", "w")}


def full_defined(p):
    """Every defined symbol, ignoring the version script.

    Must NOT use `nm -D`: a symbol this library defines but currently hides is
    exactly what a previous whitelist dropped by mistake, and it still has to be
    discoverable so the list can heal itself.
    """
    return {l.split()[0].split("@")[0] for l in
            run(["nm", "--defined-only", "--format=posix", p]).splitlines()
            if len(l.split()) >= 2}


def existing_symbols(path):
    """Symbol names already listed in the version script."""
    if not os.path.exists(path):
        return set()
    with open(path, encoding="utf-8") as f:
        return {m.group(1) for m in
                (re.match(r"\s+([A-Za-z_][A-Za-z0-9_.]*);\s*$", l) for l in f) if m}


def public_api(repo_root):
    """Function names declared with DLL_EXPORT in the public client header."""
    names = set()
    with open(os.path.join(repo_root, PUBLIC_HEADER), encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.lstrip().startswith("#") or "DLL_EXPORT" not in line:
                continue
            m = re.search(r"\b([A-Za-z_]\w*)\s*\(", line.split("DLL_EXPORT", 1)[1])
            if m:
                names.add(m.group(1))
    if not names:
        raise RuntimeError(f"no DLL_EXPORT declarations found in {PUBLIC_HEADER}")
    return names


def render(son, symbols):
    out = []
    out.append(f"# Export whitelist for {son} (GNU ld, Linux only).")
    out.append("#")
    out.append("# DO NOT edit by hand: regenerate with")
    out.append("#   python3 source/taos-community/cmake/gen_export_ver.py <build_dir>")
    out.append("#")
    out.append("# Union of: what in-package consumers import (scanned from both linked")
    out.append("# objects and target object files), the DLL_EXPORT public C API")
    out.append("# (include/client/taos.h), taos_* helpers used by out-of-tree connectors,")
    out.append("# and every symbol already listed here (the list is MONOTONIC: regenerating")
    out.append("# from a tree with fewer consumers can only add, never hide).")
    out.append("#")
    out.append("# Needs are configuration dependent (Release drops taosAssertDebug with")
    out.append("# NDEBUG; Debug keeps the tmsg.h wrappers calling tEncodeTSma/tDecodeTSma;")
    out.append("# BUILD_TEST=ON adds the TMQ API consumers), so regenerate/--check in every")
    out.append("# configuration that is built -- monotonicity makes them converge on one")
    out.append("# union instead of fighting each other.")
    out.append("#")
    out.append("# Keeping this list complete matters: the CLI tools resolve engine symbols")
    out.append("# from this library, so a hidden symbol either fails their link or, worse,")
    out.append("# gets satisfied from a local static archive -- embedding a second copy of")
    out.append("# the engine and splitting its global state.")
    out.append("VER_1.0 {")
    out.append("  global:")
    for s in sorted(symbols):
        out.append(f"    {s};")
    out.append("  local:")
    out.append("    *;")
    out.append("};")
    return "\n".join(out) + "\n"


def main():
    argv = sys.argv[1:]
    check = "--check" in argv
    args = [a for a in argv if not a.startswith("-")]
    unknown = [a for a in argv if a.startswith("-") and a != "--check"]
    if len(args) != 1 or unknown:
        print(__doc__)
        return 1

    build_dir = os.path.abspath(args[0])
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    if not os.path.isdir(build_dir):
        print(f"[gen_export_ver] build dir not found: {build_dir}", file=sys.stderr)
        return 1

    try:
        # Only the canonical output tree: a stale backup (e.g. bck_build/) holds
        # same-named libraries whose consumers are not this build's consumers.
        scan_root = os.path.join(build_dir, "build")
        if not os.path.isdir(scan_root):
            scan_root = build_dir
        all_elf = elf_files(scan_root)

        libs = [p for p in all_elf if p.endswith(SONAME)]
        if not libs:
            print(f"[gen_export_ver] {SONAME} not found under {scan_root}", file=sys.stderr)
            return 1
        lib = sorted(libs, key=lambda p: (p.count(os.sep), p))[0]

        have = full_defined(lib)
        linked = [c for c in all_elf
                  if os.path.abspath(c) != os.path.abspath(lib) and SONAME in dt_needed(c)]
        need = set()
        for c in linked:
            need |= undefined(c)
        obj_need, scanned = object_consumers(build_dir)
        need |= obj_need
        # Linker pseudo-symbols are visible to `nm` in both objects and the
        # library itself, but they must never appear in an export list.
        need = {s for s in need if not s.startswith("_GLOBAL_OFFSET_TABLE_")}

        hard = need & have                                  # imported by consumers
        # The dispatcher owns the public API.  libtaosnative.so must NOT export those
        # names: a consumer that links both libraries would otherwise bind them to
        # whichever one the linker places first, and the driver silently bypassed the
        # dispatcher that way (taosdump's setConnMode failed).  It hands its entry
        # points to the dispatcher through tdNativeApiTable instead, which has to
        # stay exported, and the undocumented taos_* helpers stay exported too --
        # nothing in libtaos.so defines them, so they cannot compete.
        public = public_api(repo_root)
        # Only the names the dispatcher actually exports compete with it: a public
        # header may declare entry points that libtaos.so does not forward yet
        # (taos_register_instance, taos_list_instances, ...).  Those are reachable
        # only here, so hiding them breaks every consumer -- and --gc-sections then
        # deletes them, which is how taosadapter's cgo link started failing.
        dispatchers = [p for p in all_elf if p.endswith(DISPATCHER_SONAME)]
        if dispatchers:
            _disp = sorted(dispatchers, key=lambda p: (p.count(os.sep), p))[0]
            competing = public & defined(_disp)
        else:
            # Nothing to compete with: falling back to "public" would hide every
            # name carried from a previous run and break the monotonic promise of
            # this whitelist (and --gc-sections would then drop them).
            print(f"[gen_export_ver] warning: no {DISPATCHER_SONAME} in the scanned tree,"
                  f" treating the public API as non-competing", file=sys.stderr)
            competing = set()
        api_here = public & have                             # subset this lib implements
        helpers = {s for s in have if s.startswith("taos_")}  # undocumented, used out of tree
        table = {NATIVE_API_TABLE} if NATIVE_API_TABLE in have else set()
        path = os.path.join(repo_root, VER_REL)
        carried = existing_symbols(path)                    # monotonic: never hide what we exported
        # Public names the dispatcher does NOT export are reachable only here, so
        # they must stay exported: the whitelist entry is also what makes the linker
        # treat them as roots again after a previous run let --gc-sections drop them.
        only_here = public - competing
        keep = ((hard | helpers | carried) - competing) | only_here | table
        # Report what consumers wanted but this library cannot provide at all:
        # those need an explicit force-include (see client/CMakeLists.txt).
        print(f"[gen_export_ver] {lib}")
        print(f"  consumers: {len(linked)} linked (DT_NEEDED {SONAME}),"
              f" {len(scanned)} target object dirs")
        print(f"  defined={len(have)}  consumer-need={len(hard)}  api_here={len(api_here)}"
              f"  hidden={len(competing)}"
              f"  taos_*={len(helpers)}  carried={len(carried)}  -> {len(keep)} exported")
        added = sorted(keep - carried)
        if added:
            print(f"  newly whitelisted ({len(added)}): {' '.join(added[:20])}"
                  f"{' ...' if len(added) > 20 else ''}")
        absent = sorted(s for s in need if s not in have)
        if absent:
            print(f"  note: {len(absent)} symbol(s) requested by consumers are not defined here"
                  f" (libc/other libs or a missing force-include): {' '.join(absent[:20])}"
                  f"{' ...' if len(absent) > 20 else ''}")

        content = render(SONAME, keep)
        if check:
            cur = ""
            if os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    cur = f.read()
            if cur != content:
                print(f"  OUT OF DATE -> {path}")
                return 2
            print(f"  up to date ({len(keep)} symbols) -> {path}")
            return 0
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  wrote {path}")
        return 0
    except (OSError, RuntimeError) as e:
        print(f"[gen_export_ver] error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
