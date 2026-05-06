#!/usr/bin/env bash
# fix-tdengine-disks.sh
# ---------------------------------------------------------------
# Fixes TDengine current.json (did.id) and vnodes.json (diskPrimary)
# after mount-point order was changed in taos.cfg.
#
# Requirements: bash >= 4, jq, grep, sed, find, sort
# No Python required.
#
# Usage:
#   bash fix-tdengine-disks.sh                        # dry-run, all vnodes
#   bash fix-tdengine-disks.sh --apply                # write changes
#   bash fix-tdengine-disks.sh --vnode 2              # dry-run, vnode2 only
#   bash fix-tdengine-disks.sh --vnode 2,3 --apply   # apply, vnode2 & vnode3
#   bash fix-tdengine-disks.sh --cfg /path/taos.cfg  # custom config path
# ---------------------------------------------------------------
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
TAOS_CFG=/etc/taos/taos.cfg
VNODE_DIR=vnode
BACKUP_SUFFIX=.pre-fix.bak
DRY_RUN=true
VNODE_FILTER=""

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply)     DRY_RUN=false ;;
        --vnode)     shift; VNODE_FILTER="$1" ;;
        --vnode=*)   VNODE_FILTER="${1#--vnode=}" ;;
        --cfg)       shift; TAOS_CFG="$1" ;;
        --cfg=*)     TAOS_CFG="${1#--cfg=}" ;;
        -h|--help)   grep '^#' "$0" | head -16 | sed 's/^# \?//'; exit 0 ;;
        *)           printf 'ERROR: unknown argument: %s\n' "$1" >&2; exit 1 ;;
    esac
    shift
done

# ── Dependency check ──────────────────────────────────────────────────────────
command -v jq  &>/dev/null || { printf 'ERROR: jq not found. Install: apt/yum install jq\n' >&2; exit 1; }
[[ -f "$TAOS_CFG" ]] || { printf 'ERROR: taos.cfg not found: %s\n' "$TAOS_CFG" >&2; exit 1; }

# ── Parse dataDir → disk arrays ───────────────────────────────────────────────
# Mirrors tfsMountDiskToTier did_id assignment from TDengine source.
DISK_PATHS=()
DISK_LEVELS=()
DISK_IDS=()
DISK_IS_PRIMARY=()
PRIMARY_PATH=""

_parse_datadirs() {
    local l0_n=0 l0_has_primary=false
    declare -A lx=()

    while IFS= read -r raw; do
        raw="${raw%%#*}"          # strip inline comments
        local p lv pr
        read -r _ p lv pr _ <<< "$raw" || true
        [[ -z "$p" ]] && continue
        [[ "$lv" =~ ^[0-9]+$ ]] || lv=0
        [[ "$pr" =~ ^[0-9]+$ ]] || pr=0

        local did
        if (( lv == 0 )); then
            if ! $l0_has_primary; then
                if (( pr == 1 )); then
                    did=0; l0_has_primary=true
                else
                    did=$(( l0_n + 1 ))
                fi
            else
                did=$l0_n
            fi
            l0_n=$(( l0_n + 1 ))
        else
            local c=${lx[$lv]:-0}
            did=$c
            lx[$lv]=$(( c + 1 ))
        fi

        DISK_PATHS+=("$p")
        DISK_LEVELS+=("$lv")
        DISK_IDS+=("$did")
        DISK_IS_PRIMARY+=("$pr")
        (( pr == 1 )) && PRIMARY_PATH="$p" || true
    done < <(grep -iE '^\s*dataDir\s' "$TAOS_CFG")

    # Fallback: if no explicit primary flag, first disk is primary
    if [[ -z "$PRIMARY_PATH" && ${#DISK_PATHS[@]} -gt 0 ]]; then
        PRIMARY_PATH="${DISK_PATHS[0]}"
        DISK_IS_PRIMARY[0]=1
    fi
}

_parse_datadirs
[[ ${#DISK_PATHS[@]} -gt 0 ]] || { printf 'ERROR: no dataDir found in %s\n' "$TAOS_CFG" >&2; exit 1; }

# ── Vnode filter ──────────────────────────────────────────────────────────────
declare -A _VF=()
if [[ -n "$VNODE_FILTER" ]]; then
    IFS=',' read -ra _toks <<< "$VNODE_FILTER"
    for _t in "${_toks[@]}"; do
        _t="${_t// /}"
        [[ "$_t" =~ ^[0-9]+$ ]] && _VF["$_t"]=1 || true
    done
fi
# Returns 0 (true) if vg_id passes the filter
_in_filter() {
    [[ ${#_VF[@]} -eq 0 ]] && return 0
    [[ -n "${_VF[$1]+_}" ]]
}

# ── Extract vgId from a current.json path ─────────────────────────────────────
# e.g. /data01/vnode/vnode3/tsdb/current.json → 3
_vg_from_path() {
    sed -n 's|.*/vnode\([0-9][0-9]*\)/tsdb/current\.json|\1|p' <<< "$1"
}

# ── Print header ──────────────────────────────────────────────────────────────
SEP='================================================================='
[[ "$DRY_RUN" == true ]] && _mode=DRY-RUN || _mode=APPLY
[[ ${#_VF[@]} -gt 0 ]] && _filt="$VNODE_FILTER" || _filt=all

printf '%s\n  TDengine disk fixer  (pure bash + jq)\n%s\n\n' "$SEP" "$SEP"
printf '  Mode         : %s\n' "$_mode"
printf '  Backup suffix: %s\n' "$BACKUP_SUFFIX"
printf '  Vnode filter : %s\n' "$_filt"
printf '\n  Disk map (from %s):\n' "$TAOS_CFG"
for i in "${!DISK_PATHS[@]}"; do
    _tag=""; (( DISK_IS_PRIMARY[i] )) && _tag=" [primary]" || true
    printf '    level=%-2s id=%-3s %s%s\n' \
        "${DISK_LEVELS[$i]}" "${DISK_IDS[$i]}" "${DISK_PATHS[$i]}" "$_tag"
done
printf '\n'

# ── Helper: find a tsdb file across all configured disks ──────────────────────
# On success, sets _FL (level) _FI (did_id) _FP (disk path).
_FL="" _FI="" _FP=""
_find_file() {   # args: vg_id  filename
    local rel="$VNODE_DIR/vnode${1}/tsdb/${2}"
    _FL="" _FI="" _FP=""
    for i in "${!DISK_PATHS[@]}"; do
        if [[ -f "${DISK_PATHS[$i]}/$rel" ]]; then
            _FL="${DISK_LEVELS[$i]}" _FI="${DISK_IDS[$i]}" _FP="${DISK_PATHS[$i]}"
            return 0
        fi
    done
    return 1
}

# ── Helper: backup and write file ─────────────────────────────────────────────
_backup_write() {  # args: path  content
    local bak="${1}${BACKUP_SUFFIX}"
    [[ -f "$bak" ]] || { cp -p "$1" "$bak"; printf '    Backup → %s\n' "$bak"; }
    printf '%s' "$2" > "$1"
    printf '    Written → %s\n' "$1"
}

# ── Totals ────────────────────────────────────────────────────────────────────
T_FIXED=0 T_REMOVED=0 T_UNCHANGED=0 T_FSETS_RM=0
VJ_FIXED=0 VJ_UNCH=0 VJ_MISS=0

# ── Fix one current.json ──────────────────────────────────────────────────────
_fix_current_json() {
    local cj="$1"
    local vg; vg=$(_vg_from_path "$cj")
    if [[ -z "$vg" ]]; then
        printf '    [ERROR] cannot extract vgId from path, skipping\n'; return
    fi

    # Extract all referenced file entries as TSV lines:
    #   fid  cid  suffix  old_level  old_id
    local refs
    refs=$(jq -r '
        .fset[]? | . as $f |
        (["head","data","sma","tomb"][] | . as $s | select($f[$s] != null) |
            [$f[$s].fid, $f[$s].cid, $s,
             $f[$s]["did.level"], $f[$s]["did.id"]] | map(tostring) | join("\t")),
        (.["stt lvl"][]?.files[]? |
            [.fid, .cid, "stt",
             .["did.level"], .["did.id"]] | map(tostring) | join("\t"))
    ' "$cj" | sort -u)

    if [[ -z "$refs" ]]; then
        printf '    (no file entries in fset)\n'; return
    fi

    # Check each file on disk; build a JSON lookup table:
    #   { "v3f1736ver100.head": {"level":0,"id":1}, ... }
    #   { "v3f1736ver3.data"  : null }   ← null means missing → remove
    local fixed=0 removed=0 unchanged=0
    local -a lk_parts=()

    while IFS=$'\t' read -r fid cid sfx olv oid; do
        [[ -z "$fid" ]] && continue
        local fn="v${vg}f${fid}ver${cid}.${sfx}"
        if _find_file "$vg" "$fn"; then
            if [[ "$_FL" == "$olv" && "$_FI" == "$oid" ]]; then
                printf '      UNCHANGED  %s\n' "$fn"
                unchanged=$(( unchanged + 1 ))
            else
                printf '      FIX  %s: (lv=%s,id=%s) → (lv=%s,id=%s)  [%s]\n' \
                    "$fn" "$olv" "$oid" "$_FL" "$_FI" "$_FP"
                fixed=$(( fixed + 1 ))
            fi
            lk_parts+=( "\"$fn\":{\"level\":$_FL,\"id\":$_FI}" )
        else
            printf '      REMOVE  %s  (not found on any disk)\n' "$fn"
            removed=$(( removed + 1 ))
            lk_parts+=( "\"$fn\":null" )
        fi
    done <<< "$refs"

    T_FIXED=$(( T_FIXED + fixed ))
    T_REMOVED=$(( T_REMOVED + removed ))
    T_UNCHANGED=$(( T_UNCHANGED + unchanged ))

    if (( fixed == 0 && removed == 0 )); then
        printf '    Nothing to change.\n'; return
    fi

    # Assemble lookup JSON (no jq needed in the loop → fast)
    local lookup="{$(IFS=','; printf '%s' "${lk_parts[*]}")}"

    # Apply the lookup with one jq call to produce updated current.json
    local new_json
    new_json=$(jq \
        --argjson lk "$lookup" \
        --argjson vg "$vg" \
        '
        # Fix one file-entry object for the given suffix.
        # Returns the (possibly updated) object, or null if the file is missing.
        def fx(s):
            . as $e |
            ("v"+($vg|tostring)+"f"+($e.fid|tostring)+"ver"+($e.cid|tostring)+"."+s) as $k |
            if ($lk | has($k) | not) then $e        # not in lookup → unchanged
            elif $lk[$k] == null       then null     # null → file missing, remove
            else $e | .["did.level"] = $lk[$k].level | .["did.id"] = $lk[$k].id
            end;

        .fset = [
            .fset[]? |
            # Compute corrected values (null = missing)
            (if .head then (.head | fx("head")) else null end) as $h |
            (if .data then (.data | fx("data")) else null end) as $d |
            (if .sma  then (.sma  | fx("sma"))  else null end) as $s |
            (if .tomb then (.tomb | fx("tomb")) else null end) as $t |
            # Rebuild fset without the four file keys, then add back non-null ones
            del(.head, .data, .sma, .tomb) |
            (if $h != null then .head = $h else . end) |
            (if $d != null then .data = $d else . end) |
            (if $s != null then .sma  = $s else . end) |
            (if $t != null then .tomb = $t else . end) |
            # Fix stt lvl files in-place, dropping missing ones
            .["stt lvl"] = [
                .["stt lvl"][]? |
                .files = [.files[]? | fx("stt") | select(. != null)] |
                select(.files | length > 0)
            ] |
            # Drop the entire fset if every file is gone
            select(
                (.head != null) or (.data != null) or
                (.sma  != null) or (.tomb != null) or
                ((.["stt lvl"] | length) > 0)
            )
        ]
        ' "$cj")

    # Count dropped fsets
    local fb fa rm_f
    fb=$(jq '.fset | length' "$cj")
    fa=$(printf '%s' "$new_json" | jq '.fset | length')
    rm_f=$(( fb - fa ))
    T_FSETS_RM=$(( T_FSETS_RM + rm_f ))
    (( rm_f > 0 )) && printf '      [REMOVED %d complete fset(s) — all files missing]\n' "$rm_f" || true

    if [[ "$DRY_RUN" == true ]]; then
        printf '    [DRY-RUN] fixed=%d removed=%d fsets_removed=%d\n' "$fixed" "$removed" "$rm_f"
    else
        _backup_write "$cj" "$new_json"
    fi
}

# ── Fix vnodes.json ────────────────────────────────────────────────────────────
_fix_vnodes_json() {
    printf '── vnodes.json (diskPrimary) ──\n'
    local vj="$PRIMARY_PATH/$VNODE_DIR/vnodes.json"
    if [[ ! -f "$vj" ]]; then
        printf '  [WARN] %s not found\n' "$vj"; return
    fi
    printf '  %s\n' "$vj"

    # Build did_id → disk_path for level-0 disks (for display only)
    declare -A _id2p=()
    for i in "${!DISK_IDS[@]}"; do
        (( DISK_LEVELS[i] == 0 )) && _id2p["${DISK_IDS[$i]}"]="${DISK_PATHS[$i]}" || true
    done

    # Derive each vnode's correct did_id from which level-0 disk holds its current.json.
    # Iterating disks in order; a vnode's entry is overwritten by later disks
    # (consistent with the Python script's sort-based last-wins approach).
    declare -A _v2did=()
    for i in "${!DISK_PATHS[@]}"; do
        (( DISK_LEVELS[i] == 0 )) || continue
        while IFS= read -r cj; do
            local vg; vg=$(_vg_from_path "$cj")
            [[ -n "$vg" ]] && _v2did["$vg"]="${DISK_IDS[$i]}" || true
        done < <(find "${DISK_PATHS[$i]}/$VNODE_DIR" -maxdepth 3 \
                     -name "current.json" -path "*/tsdb/*" 2>/dev/null || true)
    done

    local n
    n=$(jq '.vnodes | length' "$vj")
    local changed_json; changed_json=$(cat "$vj")
    local changed=false

    for (( idx = 0; idx < n; idx++ )); do
        local vg old_dp
        vg=$(jq -r ".vnodes[$idx].vgId"       "$vj")
        old_dp=$(jq -r ".vnodes[$idx].diskPrimary" "$vj")

        _in_filter "$vg" || continue

        local new_dp="${_v2did[$vg]:-}"
        if [[ -z "$new_dp" ]]; then
            printf '    vgId=%-4s diskPrimary=%s  [current.json not found on any disk]\n' "$vg" "$old_dp"
            VJ_MISS=$(( VJ_MISS + 1 ))
        elif [[ "$old_dp" == "$new_dp" ]]; then
            printf '    vgId=%-4s diskPrimary=%s  OK  [%s]\n' \
                "$vg" "$old_dp" "${_id2p[$old_dp]:-?}"
            VJ_UNCH=$(( VJ_UNCH + 1 ))
        else
            printf '    vgId=%-4s diskPrimary  %s → %s  [%s]\n' \
                "$vg" "$old_dp" "$new_dp" "${_id2p[$new_dp]:-?}"
            changed_json=$(printf '%s' "$changed_json" | \
                jq --argjson i "$idx" --argjson dp "$new_dp" \
                   '.vnodes[$i].diskPrimary = $dp')
            changed=true
            VJ_FIXED=$(( VJ_FIXED + 1 ))
        fi
    done

    if $changed; then
        if [[ "$DRY_RUN" == true ]]; then
            printf '    [DRY-RUN] would fix %d diskPrimary entries\n' "$VJ_FIXED"
        else
            _backup_write "$vj" "$changed_json"
        fi
    else
        printf '    No diskPrimary values needed updating.\n'
    fi
}

# ── Main ───────────────────────────────────────────────────────────────────────
printf '── current.json ──\n'

# Collect all current.json across every configured disk
mapfile -t _ALL_CJ < <(
    for i in "${!DISK_PATHS[@]}"; do
        find "${DISK_PATHS[$i]}/$VNODE_DIR" -maxdepth 3 \
            -name "current.json" -path "*/tsdb/*" 2>/dev/null || true
    done | sort -u
)

# Apply vnode filter
CJ_PATHS=()
for _cj in "${_ALL_CJ[@]}"; do
    _vg=$(_vg_from_path "$_cj")
    [[ -n "$_vg" ]] && _in_filter "$_vg" && CJ_PATHS+=("$_cj") || true
done

if [[ ${#CJ_PATHS[@]} -eq 0 ]]; then
    printf 'ERROR: no current.json found on configured disks.\n' >&2; exit 1
fi
printf '  Found %d current.json file(s).\n\n' "${#CJ_PATHS[@]}"

for _cj in "${CJ_PATHS[@]}"; do
    printf '  %s\n' "$_cj"
    _fix_current_json "$_cj"
    printf '\n'
done

printf '\n'
_fix_vnodes_json

# ── Summary ────────────────────────────────────────────────────────────────────
printf '\n%s\n' "$SEP"
printf '  current.json:\n'
printf '    Entries fixed          : %d\n'   "$T_FIXED"
printf '    Entries removed        : %d  (not found on any disk)\n' "$T_REMOVED"
printf '    Fsets removed          : %d  (all files missing)\n'     "$T_FSETS_RM"
printf '    Entries unchanged      : %d\n'   "$T_UNCHANGED"
printf '  vnodes.json:\n'
printf '    diskPrimary fixed      : %d\n'   "$VJ_FIXED"
printf '    diskPrimary unchanged  : %d\n'   "$VJ_UNCH"
printf '    Vnodes not found       : %d\n'   "$VJ_MISS"
if [[ "$DRY_RUN" == true ]]; then
    printf '\n  DRY-RUN – re-run with --apply to write changes.\n'
else
    printf '\n  Done. Start taosd to verify.\n'
fi
