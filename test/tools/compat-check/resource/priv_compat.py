#!/usr/bin/env python3
# filepath: hot_update/resource/priv_compat.py
#
# Version-aware privilege helpers shared by resourceManager.py and userVerifier.py.
#
# Schema differences between TDengine versions:
#
#   3.3.x.y  ins_user_privileges columns: user_name | privilege | db_name | table_name | ...
#            GRANT syntax: GRANT READ/WRITE ON db.stable TO user
#
#   3.4.0.0+ ins_user_privileges columns: user_name | priv_type | priv_scope | db_name | table_name | ...
#            GRANT syntax: GRANT SELECT/INSERT ON db.stable TO user
#            Also auto-assigns SYSINFO_1 role to every new user (must be revoked
#            before adding OBJECT-scope grants to avoid "mixed types" error).
#
# Normalization:
#   Both 'select' (3.4+) and 'read' (3.3) are stored as 'read' in snapshots.
#   Both 'insert' (3.4+) and 'write' (3.3) are stored as 'write' in snapshots.
#   This allows cross-version comparison (3.3 baseline → 3.4 post-upgrade) to pass.

# Canonical mapping: normalize privilege keywords to 3.3-style 'read'/'write'
_PRIV_NORM = {
    "read":   "read",
    "write":  "write",
    "select": "read",
    "insert": "write",
}


def is_v34_plus(cursor) -> bool:
    """
    Detect 3.4.0.0+ by inspecting the actual column schema of
    information_schema.ins_user_privileges.
      3.3.x.y  → column named 'privilege'
      3.4.0.0+ → column named 'priv_type'
    More reliable than server_version() which returns None on some builds.
    """
    try:
        cursor.execute("DESCRIBE information_schema.ins_user_privileges")
        cols = [str(r[0]).lower() for r in cursor.fetchall()]
        return "priv_type" in cols
    except Exception:
        return False  # assume old syntax on any error


def snapshot_privileges(cursor, user_name: str) -> frozenset:
    """
    Return a frozenset of ('read'|'write', db_name, table_name) for *user_name*.

    Automatically detects the server version and uses the correct column name.
    Only table-level grants (non-empty table_name) are included.
    Privilege keywords are normalized so cross-version comparison works.
    """
    v34 = is_v34_plus(cursor)
    priv_col = "priv_type" if v34 else "privilege"
    cursor.execute(
        f"SELECT {priv_col}, db_name, table_name "
        f"FROM information_schema.ins_user_privileges "
        f"WHERE user_name='{user_name}'"
    )
    result = set()
    for r in cursor.fetchall():
        priv = str(r[0]).lower()
        db   = str(r[1])
        tbl  = str(r[2]) if r[2] else ""
        if tbl:  # skip database-level / wildcard rows
            result.add((_PRIV_NORM.get(priv, priv), db, tbl))
    return frozenset(result)


def grant_keywords(cursor) -> tuple:
    """
    Return (read_keyword, write_keyword) appropriate for the connected server.
      3.3.x.y  → ('READ',   'WRITE')
      3.4.0.0+ → ('SELECT', 'INSERT')
    """
    return ("SELECT", "INSERT") if is_v34_plus(cursor) else ("READ", "WRITE")


def revoke_default_role(cursor, user_name: str, logger=None) -> None:
    """
    On 3.4.0.0+ TDengine Enterprise, every new user auto-gets the SYSINFO_1 role
    which contains TABLE-scope SHOW privileges.  Granting OBJECT-scope SELECT/INSERT
    on top of that raises 'Object privileges of different types cannot be mixed'.
    Revoke the role before adding explicit grants.
    No-op on 3.3.x.y (role doesn't exist).
    """
    if not is_v34_plus(cursor):
        return
    try:
        cursor.execute(f"REVOKE ROLE `SYSINFO_1` FROM {user_name}")
        if logger:
            logger.info(f"Revoked default SYSINFO_1 role from {user_name}")
    except Exception as e:
        if logger:
            logger.info(f"REVOKE SYSINFO_1 skipped ({e})")
