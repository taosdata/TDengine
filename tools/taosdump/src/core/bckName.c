/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "bckName.h"

#include <ctype.h>
#include <string.h>

// Filename chars illegal on any platform (union == Windows set); '%' is added to keep decoding unambiguous.
static bool bckNameCharUnsafe(unsigned char c) {
    if (c < 0x20) return true;  // control chars incl NUL
    switch (c) {
        case '<': case '>': case ':': case '"':
        case '/': case '\\': case '|': case '?': case '*':
        case '%':
            return true;
        default:
            return false;
    }
}

// Windows rejects reserved device names with any extension; match the stem (before the first '.').
static bool bckNameStemIsReservedDevice(const char *name) {
    static const char *dev[] = {
        "con", "prn", "aux", "nul",
        "com1", "com2", "com3", "com4", "com5",
        "com6", "com7", "com8", "com9",
        "lpt1", "lpt2", "lpt3", "lpt4", "lpt5",
        "lpt6", "lpt7", "lpt8", "lpt9",
    };
    char stem[16];
    size_t i = 0;
    while (name[i] && name[i] != '.' && i + 1 < sizeof(stem)) {
        stem[i] = (char)tolower((unsigned char)name[i]);
        i++;
    }
    stem[i] = '\0';
    for (size_t k = 0; k < sizeof(dev) / sizeof(dev[0]); k++) {
        if (strcmp(stem, dev[k]) == 0) return true;
    }
    return false;
}

// Internal helper: detect whether a name needs encoding, and report the
// reserved-device flag (which forces the first byte to be encoded) plus the
// name length, so the encoder can reuse both without rescanning the name.
static bool bckNameNeedsEncodeEx(const char *name, bool *forceFirstOut, size_t *lenOut) {
    if (name == NULL) {
        if (forceFirstOut) *forceFirstOut = false;
        if (lenOut) *lenOut = 0;
        return false;
    }
    size_t len = strlen(name);
    if (lenOut) *lenOut = len;
    if (len == 0) {
        if (forceFirstOut) *forceFirstOut = false;
        return false;
    }

    bool reserved = bckNameStemIsReservedDevice(name);
    if (forceFirstOut) *forceFirstOut = reserved;

    // Position rules: leading '.'; trailing '.' or space (Windows strips them).
    if (name[0] == '.') return true;
    if (name[len - 1] == '.' || name[len - 1] == ' ') return true;
    if (reserved) return true;

    for (const unsigned char *p = (const unsigned char *)name; *p; p++) {
        if (bckNameCharUnsafe(*p)) return true;
    }
    return false;
}

bool bckNameNeedsEncode(const char *name) {
    return bckNameNeedsEncodeEx(name, NULL, NULL);
}

static int bckHexVal(unsigned char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

const char *bckNameEncode(const char *name, char *out, int outCap) {
    if (name == NULL || out == NULL || outCap <= 0) return NULL;

    bool forceFirst = false;
    size_t len = 0;
    // Fast path: safe names pass through untouched (zero copy).
    if (!bckNameNeedsEncodeEx(name, &forceFirst, &len)) return name;

    static const char hex[] = "0123456789ABCDEF";
    int o = 0;

    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)name[i];
        bool last = (i == len - 1);
        // Position-sensitive unsafe bytes (encoded only at the boundaries).
        bool positionUnsafe = (i == 0 && (c == '.' || forceFirst)) ||
                              (last && (c == '.' || c == ' '));
        if (bckNameCharUnsafe(c) || positionUnsafe) {
            if (o + 3 >= outCap) return NULL;
            out[o++] = '%';
            out[o++] = hex[c >> 4];
            out[o++] = hex[c & 0x0F];
        } else {
            if (o + 1 >= outCap) return NULL;
            out[o++] = (char)c;
        }
    }
    out[o] = '\0';
    return out;
}

const char *bckNameDecode(const char *name, char *out, int outCap) {
    if (name == NULL || out == NULL || outCap <= 0) return NULL;

    // Fast path: names without '%' (incl all pre-encoding backups) pass through with zero copy.
    if (strchr(name, '%') == NULL) return name;

    int o = 0;
    for (const unsigned char *p = (const unsigned char *)name; *p; p++) {
        if (*p == '%' && p[1] && p[2] &&
            bckHexVal(p[1]) >= 0 && bckHexVal(p[2]) >= 0) {
            if (o + 1 >= outCap) return NULL;
            out[o++] = (char)((bckHexVal(p[1]) << 4) | bckHexVal(p[2]));
            p += 2;
        } else {
            if (o + 1 >= outCap) return NULL;
            out[o++] = (char)*p;
        }
    }
    out[o] = '\0';
    return out;
}

char *bckNameDecodeInPlace(char *name, int cap) {
    if (name == NULL || cap <= 0) return NULL;

    // Fast path: nothing to decode.
    if (strchr(name, '%') == NULL) return name;

    // In-place decode is safe: the output position never overtakes the input.
    int o = 0;
    for (const unsigned char *p = (const unsigned char *)name; *p; p++) {
        if (*p == '%' && p[1] && p[2] &&
            bckHexVal(p[1]) >= 0 && bckHexVal(p[2]) >= 0) {
            if (o + 1 >= cap) return NULL;
            name[o++] = (char)((bckHexVal(p[1]) << 4) | bckHexVal(p[2]));
            p += 2;
        } else {
            if (o + 1 >= cap) return NULL;
            name[o++] = (char)*p;
        }
    }
    name[o] = '\0';
    return name;
}

int bckNameDecodeFileStem(const char *baseName, char *out, int outCap) {
    if (baseName == NULL || out == NULL || outCap <= 0) return -1;

    // The stem is everything before the last '.' (the extension).
    const char *dot = strrchr(baseName, '.');
    size_t stemLen = dot ? (size_t)(dot - baseName) : strlen(baseName);

    // Decode the encoded stem in an internal buffer large enough for the
    // worst-case "%XX" expansion, then copy the (always shorter) decoded
    // name into @out -- so outCap only needs to hold the real name.
    char enc[BCK_NAME_ENC_STEM_MAX];
    if (stemLen >= sizeof(enc)) return -1;
    memcpy(enc, baseName, stemLen);
    enc[stemLen] = '\0';
    if (bckNameDecodeInPlace(enc, sizeof(enc)) == NULL) return -1;

    size_t n = strlen(enc);
    if (n >= (size_t)outCap) n = (size_t)outCap - 1;
    memcpy(out, enc, n);
    out[n] = '\0';
    return 0;
}

const char *bckSqlEscape(const char *name, char *out, int outCap) {
    if (name == NULL || out == NULL || outCap <= 0) return NULL;

    // Fast path: names without '\\' or '\'' pass through untouched (zero copy),
    // so the overwhelming majority of names cost only a single strpbrk scan.
    if (strpbrk(name, "\\'") == NULL) return name;

    int o = 0;
    for (const unsigned char *p = (const unsigned char *)name; *p; p++) {
        if (*p == '\\' || *p == '\'') {
            if (o + 2 >= outCap) return NULL;
            out[o++] = '\\';
            out[o++] = (char)*p;
        } else {
            if (o + 1 >= outCap) return NULL;
            out[o++] = (char)*p;
        }
    }
    out[o] = '\0';
    return out;
}
