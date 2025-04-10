/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "tssInt.h"


// the registry of shared storage types
static const SSharedStorageType* g_registry[8] = {0};


// the default shared storage
static SSharedStorage* g_default = NULL;



// tssRegisterType registers a shared storage type with the given name and
// initialization function.
void tssRegisterType(const SSharedStorageType* t) {
    for (int i = 0; i < countof(g_registry); ++i) {
        SSharedStorageType* type = g_registry[i];
        if (type != NULL && strcmp(type->name, t->name) == 0) {
            tssFatal("shared storage type '%s' already registered\n", t->name);
            return;
        }
    }

    for (int i = 0; i < countof(g_registry); ++i) {
        if (g_registry[i] == NULL) {
            g_registry[i] = (SSharedStorageType*)t;
            return;
        }
    }

    tssFatal("no space left in the registry for shared storage type '%s'\n", t->name);
}



// tssInit initializes the tss module.
int32_t tssInit() {
    s3RegisterType();
    return TSDB_CODE_SUCCESS;
}



// tssUninit uninitializes the tss module.
int32_t tssUninit() {
    return TSDB_CODE_SUCCESS;
}



// tssCreateInstance creates a shared storage instance according to the access string.
int32_t tssCreateInstance(const char* as, SSharedStorage** ppSS) {
    size_t asLen = strlen(as);

    for (int i = 0; i < countof(g_registry); ++i) {
        const SSharedStorageType* t = g_registry[i];
        if (t == NULL) {
            continue;
        }

        const char* name = t->name;
        size_t nameLen = strlen(t->name);
        if (asLen <= nameLen || strncmp(as, name, nameLen) != 0 || as[nameLen] != ':') {
            continue;
        }

        return t->createInstance(as, ppSS);
    }

    tssError("shared storage type not found, access string is: %s\n", as);
    return TSDB_CODE_NOT_FOUND;
}




// tssCloseInstance uninitializes a shared storage instance.
int32_t tssCloseInstance(SSharedStorage* pSS) {
    int32_t code = pSS->type->closeInstance(pSS);
    if (code != TSDB_CODE_SUCCESS) {
        tssError("failed to uninitialize shared storage, code: %d\n", code);
    }
    return code;
}



// tssCreateDefaultInstance creates the default shared storage instance using
// [tsSsAccessString] as the access string.
int32_t tssCreateDefaultInstance() {
    extern char tsSsAccessString[];
    if (g_default != NULL) {
        tssError("default shared storage already initialized\n");
        return 0; // already initialized
    }
    return tssCreateInstance(tsSsAccessString, &g_default);
}



// tssCloseDefaultInstance uninitializes the default shared storage instance.
int32_t tssCloseDefaultInstance() {
    int32_t code = tssCloseInstance(g_default);
    g_default = NULL;
    return code;
}
