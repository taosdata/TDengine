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

#include "indexFstAutomation.h"

StartWithStateValue* startWithStateValueCreate(StartWithStateKind kind, ValueType ty, void* val) {
  StartWithStateValue* sv = taosMemoryCalloc(1, sizeof(StartWithStateValue));
  if (sv == NULL) {
    return NULL;
  }

  sv->kind = kind;
  sv->type = ty;
  if (ty == FST_INT) {
    sv->val = *(int*)val;
  } else if (ty == FST_CHAR) {
    size_t len = strlen((char*)val);
    sv->ptr = (char*)taosMemoryCalloc(1, len + 1);
    memcpy(sv->ptr, val, len);
  } else if (ty == FST_ARRAY) {
    // TODO,
    // nsv->arr = taosArrayFromList()
  }
  return sv;
}
void startWithStateValueDestroy(void* val) {
  StartWithStateValue* sv = (StartWithStateValue*)val;
  if (sv == NULL) {
    return;
  }

  if (sv->type == FST_INT) {
    //
  } else if (sv->type == FST_CHAR) {
    taosMemoryFree(sv->ptr);
  } else if (sv->type == FST_ARRAY) {
    taosArrayDestroy(sv->arr);
  }
  taosMemoryFree(sv);
}
StartWithStateValue* startWithStateValueDump(StartWithStateValue* sv) {
  StartWithStateValue* nsv = taosMemoryCalloc(1, sizeof(StartWithStateValue));
  if (nsv == NULL) {
    return NULL;
  }

  nsv->kind = sv->kind;
  nsv->type = sv->type;
  if (nsv->type == FST_INT) {
    nsv->val = sv->val;
  } else if (nsv->type == FST_CHAR) {
    size_t len = strlen(sv->ptr);
    nsv->ptr = (char*)taosMemoryCalloc(1, len + 1);
    memcpy(nsv->ptr, sv->ptr, len);
  } else if (nsv->type == FST_ARRAY) {
    //
  }
  return nsv;
}

// iterate fst
static void* alwaysMatchStart(FAutoCtx* ctx) { return NULL; }
static bool  alwaysMatchIsMatch(FAutoCtx* ctx, void* state) { return true; }
static bool  alwaysMatchCanMatch(FAutoCtx* ctx, void* state) { return true; }
static bool  alwaysMatchWillAlwaysMatch(FAutoCtx* ctx, void* state) { return true; }
static void* alwaysMatchAccpet(FAutoCtx* ctx, void* state, uint8_t byte) { return NULL; }
static void* alwaysMatchAccpetEof(FAutoCtx* ctx, void* state) { return NULL; }
// prefix query, impl later

static void* prefixStart(FAutoCtx* ctx) {
  StartWithStateValue* data = (StartWithStateValue*)(ctx->stdata);
  return startWithStateValueDump(data);
};
static bool prefixIsMatch(FAutoCtx* ctx, void* sv) {
  StartWithStateValue* ssv = (StartWithStateValue*)sv;
  if (ssv == NULL) {
    return false;
  }
  if (ssv->type == FST_INT) {
    return ssv->val == strlen(ctx->data);
  } else {
    return false;
  }
}
static bool prefixCanMatch(FAutoCtx* ctx, void* sv) {
  StartWithStateValue* ssv = (StartWithStateValue*)sv;
  if (ssv == NULL) {
    return false;
  }
  return ssv->val >= 0;
}
static bool  prefixWillAlwaysMatch(FAutoCtx* ctx, void* state) { return true; }
static void* prefixAccept(FAutoCtx* ctx, void* state, uint8_t byte) {
  StartWithStateValue* ssv = (StartWithStateValue*)state;
  if (ssv == NULL || ctx == NULL) {
    return NULL;
  }

  char* data = ctx->data;
  if (ssv->kind == Done) {
    return startWithStateValueCreate(Done, FST_INT, &ssv->val);
  }
  if ((strlen(data) > ssv->val) && data[ssv->val] == byte) {
    int val = ssv->val + 1;

    StartWithStateValue* nsv = startWithStateValueCreate(Running, FST_INT, &val);
    if (prefixIsMatch(ctx, nsv)) {
      nsv->kind = Done;
    } else {
      nsv->kind = Running;
    }
    return nsv;
  }
  return NULL;
}
static void* prefixAcceptEof(FAutoCtx* ctx, void* state) { return NULL; }

// pattern query, impl later

static void* patternStart(FAutoCtx* ctx) { return NULL; }
static bool  patternIsMatch(FAutoCtx* ctx, void* data) { return true; }
static bool  patternCanMatch(FAutoCtx* ctx, void* data) { return true; }
static bool  patternWillAlwaysMatch(FAutoCtx* ctx, void* state) { return true; }

static void* patternAccept(FAutoCtx* ctx, void* state, uint8_t byte) { return NULL; }

static void* patternAcceptEof(FAutoCtx* ctx, void* state) { return NULL; }

AutomationFunc automFuncs[] = {
    {alwaysMatchStart, alwaysMatchIsMatch, alwaysMatchCanMatch, alwaysMatchWillAlwaysMatch, alwaysMatchAccpet,
     alwaysMatchAccpetEof},
    {prefixStart, prefixIsMatch, prefixCanMatch, prefixWillAlwaysMatch, prefixAccept, prefixAcceptEof},
    {patternStart, patternIsMatch, patternCanMatch, patternWillAlwaysMatch, patternAccept, patternAcceptEof}
    // add more search type
};

FAutoCtx* automCtxCreate(void* data, AutomationType atype) {
  FAutoCtx* ctx = taosMemoryCalloc(1, sizeof(FAutoCtx));
  if (ctx == NULL) {
    return NULL;
  }

  StartWithStateValue* sv = NULL;
  if (atype == AUTOMATION_ALWAYS) {
    int val = 0;
    sv = startWithStateValueCreate(Running, FST_INT, &val);
  } else if (atype == AUTOMATION_PREFIX) {
    int val = 0;
    sv = startWithStateValueCreate(Running, FST_INT, &val);
  } else if (atype == AUTMMATION_MATCH) {
  } else {
    // add more search type
  }

  ctx->data = (data != NULL ? taosStrdup((char*)data) : NULL);
  ctx->type = atype;
  ctx->stdata = (void*)sv;
  return ctx;
}
void automCtxDestroy(FAutoCtx* ctx) {
  startWithStateValueDestroy(ctx->stdata);
  taosMemoryFree(ctx->data);
  taosMemoryFree(ctx);
}
