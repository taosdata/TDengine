/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 */

#include <ctype.h>
#include <regex.h>
#include <string.h>
#include "cJSON.h"
#include "clientInt.h"
#include "clientLog.h"
#include "nodes.h"
#include "querynodes.h"
#include "os.h"

#include "tglobal.h"
#define SQL_SEC_MAX_RULES        128
#define SQL_SEC_RULE_NAME_LEN    128
#define SQL_SEC_RULE_PATTERN_LEN 512
#define SQL_SEC_MAX_PATTERNS     256
#define SQL_SEC_PATTERN_LEN      1024

typedef enum {
  SQL_SEC_ACTION_DENY = 0,
  SQL_SEC_ACTION_ALLOW = 1,
} ESqlSecAction;

typedef struct {
  int32_t       id;
  int32_t       priority;
  bool          enabled;
  ESqlSecAction action;
  char          name[SQL_SEC_RULE_NAME_LEN];
  char          pattern[SQL_SEC_RULE_PATTERN_LEN];
  regex_t       regex;
  bool          regexInited;
} SSqlSecRule;

typedef struct {
  bool          inited;
  TdThreadMutex lock;
  SSqlSecRule   rules[SQL_SEC_MAX_RULES];
  int32_t       numOfRules;
  int64_t       lastLoadTsMs;
  int64_t       lastRuleMtime;
  char          lastRulePath[256];
} SSqlSecCtx;

typedef struct {
  bool               denyMatched;
  bool               allowMatched;
  int32_t            denyPriority;
  int32_t            allowPriority;
  const SSqlSecRule* pDenyRule;
  const SSqlSecRule* pAllowRule;
} SSqlSecMatchResult;

typedef struct {
  bool hasOrTrue;
  bool hasDangerFunc;
  bool hasUnsafeJoin;
} SSqlSecAstResult;

typedef struct {
  char    pattern[SQL_SEC_PATTERN_LEN];
  int32_t count;
  int64_t firstSeenTs;
  int64_t lastSeenTs;
  bool    exported;  // Mark if this pattern has been exported
} SSqlPattern;

typedef struct {
  bool          enabled;
  TdThreadMutex lock;
  SSqlPattern   patterns[SQL_SEC_MAX_PATTERNS];
  int32_t       numOfPatterns;
  int64_t       startTs;
  int32_t       nextRuleId;
  // Thread related
  TdThread      thread;
  int8_t        threadRunning;  // atomic, use atomic_load_8/atomic_store_8
  int8_t        threadStop;     // atomic, use atomic_load_8/atomic_store_8
  TdThreadCond  cond;
  TdThreadMutex threadLock;
} SSqlLearningCtx;

static SSqlSecCtx      gSqlSecCtx = {0};
static SSqlLearningCtx gLearningCtx = {0};

static TdThreadOnce gLearningInitOnce = PTHREAD_ONCE_INIT;
static TdThreadOnce gSqlSecInitOnce   = PTHREAD_ONCE_INIT;

// Generalize SQL by replacing literals with placeholders
static void sqlSecGeneralizePattern(const char* sql, char* pattern, int32_t patternLen) {
  if (sql == NULL || pattern == NULL || patternLen <= 0) return;

  const char* p = sql;
  char*       out = pattern;
  int32_t     outLen = 0;
  bool        inString = false;
  bool        inNumber = false;

  while (*p && outLen < patternLen - 10) {
    if (*p == '\'') {
      if (!inString) {
        // Start of string literal
        inString = true;
        if (outLen + 3 < patternLen) {
          strcpy(out, "?");
          out += 1;
          outLen += 1;
        }
      } else {
        // End of string literal
        inString = false;
      }
      p++;
      continue;
    }

    if (inString) {
      p++;
      continue;
    }

    // Check for number (including floats like 25.5)
    if (isdigit(*p) || (*p == '-' && isdigit(*(p + 1)))) {
      if (!inNumber) {
        // Start of number
        inNumber = true;
        if (outLen + 3 < patternLen) {
          strcpy(out, "?");
          out += 1;
          outLen += 1;
        }
      }
      p++;
      continue;
    } else if (inNumber && *p == '.' && isdigit(*(p + 1))) {
      // Decimal point in a number, continue as part of the number
      p++;
      continue;
    } else {
      inNumber = false;
    }

    // Copy other characters (convert to lowercase)
    *out = tolower(*p);
    out++;
    outLen++;
    p++;
  }

  *out = '\0';
}

static int32_t sqlSecSaveLearnedRules(const char* ruleFile) {
  if (ruleFile == NULL || ruleFile[0] == 0) return -1;

  (void)taosThreadMutexLock(&gLearningCtx.lock);

  // Read existing rules
  cJSON* pRoot = NULL;
  cJSON* pRules = NULL;

  TdFilePtr fp = taosOpenFile(ruleFile, TD_FILE_READ);
  if (fp != NULL) {
    int64_t fsize = 0;
    if (taosFStatFile(fp, &fsize, NULL) == TSDB_CODE_SUCCESS && fsize > 0 && fsize < 4 * 1024 * 1024) {
      char* pBuf = taosMemoryCalloc(1, (size_t)fsize + 1);
      if (pBuf != NULL) {
        int64_t nread = taosReadFile(fp, pBuf, fsize);
        if (nread == fsize) {
          pRoot = cJSON_Parse(pBuf);
        }
        taosMemoryFree(pBuf);
      }
    }
    TAOS_UNUSED(taosCloseFile(&fp));
  }

  if (pRoot == NULL) {
    pRoot = cJSON_CreateObject();
    (void)cJSON_AddStringToObject(pRoot, "version", "1.0");
    pRules = cJSON_CreateArray();
    if(pRules != NULL) {
      (void)cJSON_AddItemToObject(pRoot, "rules", pRules);
    }
  } else {
    pRules = cJSON_GetObjectItemCaseSensitive(pRoot, "rules");
    if (!cJSON_IsArray(pRules)) {
      pRules = cJSON_CreateArray();
      if(pRules != NULL) {
        (void)cJSON_AddItemToObject(pRoot, "rules", pRules);
      }
    }
  }

  // Build a set of existing patterns to avoid duplicates
  SHashObj* existingPatterns = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (existingPatterns != NULL) {
    int32_t rulesCount = cJSON_GetArraySize(pRules);
    for (int32_t i = 0; i < rulesCount; ++i) {
      cJSON* pRule = cJSON_GetArrayItem(pRules, i);
      cJSON* pDesc = cJSON_GetObjectItemCaseSensitive(pRule, "description");
      if (cJSON_IsString(pDesc) && pDesc->valuestring != NULL) {
        // Extract original pattern from description
        const char* desc = pDesc->valuestring;
        const char* prefix = "Learned pattern (count:";
        if (strncmp(desc, prefix, strlen(prefix)) == 0) {
          // This is a learned rule, mark it as existing
          // We use description as a simple way to identify duplicates
          // Better approach: store original pattern in description
          if (taosHashPut(existingPatterns, desc, strlen(desc), &i, sizeof(i)) != 0) {
            tscError("sql security: failed to add existing pattern to hash");
          }
        }
      }
    }
  }

  // Add learned patterns that reached threshold and haven't been exported
  int32_t added = 0;
  for (int32_t i = 0; i < gLearningCtx.numOfPatterns; ++i) {
    SSqlPattern* pPattern = &gLearningCtx.patterns[i];
    if (pPattern->count >= tsWhitelistLearningThreshold && !pPattern->exported) {
      // Check if this pattern already exists in the file
      char desc[256];
      snprintf(desc, sizeof(desc), "Pattern: %s", pPattern->pattern);

      bool exists = false;
      if (existingPatterns != NULL) {
        // Check if pattern already exists
        int32_t rulesCount = cJSON_GetArraySize(pRules);
        for (int32_t j = 0; j < rulesCount; ++j) {
          cJSON* pRule = cJSON_GetArrayItem(pRules, j);
          cJSON* pRuleDesc = cJSON_GetObjectItemCaseSensitive(pRule, "description");
          if (cJSON_IsString(pRuleDesc) && pRuleDesc->valuestring != NULL) {
            if (strstr(pRuleDesc->valuestring, pPattern->pattern) != NULL) {
              exists = true;
              break;
            }
          }
        }
      }

      if (exists) {
        pPattern->exported = true;
        continue;
      }

      cJSON* pRule = cJSON_CreateObject();
      if (pRule == NULL) continue;

      (void)cJSON_AddNumberToObject(pRule, "ruleId", gLearningCtx.nextRuleId++);

      char ruleName[128];
      snprintf(ruleName, sizeof(ruleName), "LEARNED_RULE_%d", gLearningCtx.nextRuleId - 1);
      (void)cJSON_AddStringToObject(pRule, "ruleName", ruleName);
      (void)cJSON_AddStringToObject(pRule, "action", "ALLOW");
      (void)cJSON_AddStringToObject(pRule, "priority", "MEDIUM");

      // Convert pattern to regex
      // Replace ? with regex pattern for any value
      char        regexPattern[SQL_SEC_PATTERN_LEN * 2] = {0};
      const char* src = pPattern->pattern;
      char*       dst = regexPattern;
      while (*src && (dst - regexPattern) < sizeof(regexPattern) - 30) {
        if (*src == '?') {
          // Replace ? with more flexible regex pattern
          // Match: number (int/float), 'string', "string", or identifier
          const char* placeholder = "[^[:space:],;)]+";
          strcpy(dst, placeholder);
          dst += strlen(placeholder);
          src++;
        } else if (*src == '*' || *src == '+' || *src == '.' || *src == '[' || *src == ']' || *src == '(' ||
                   *src == ')' || *src == '{' || *src == '}' || *src == '^' || *src == '$' || *src == '|' ||
                   *src == '\\' || *src == '?') {
          // Escape regex special characters
          *dst++ = '\\';
          *dst++ = *src++;
        } else {
          *dst++ = *src++;
        }
      }
      *dst = '\0';

      (void)cJSON_AddStringToObject(pRule, "pattern", regexPattern);

      snprintf(desc, sizeof(desc), "Learned pattern (count:%d) - Pattern: %s", pPattern->count, pPattern->pattern);
      (void)cJSON_AddStringToObject(pRule, "description", desc);
      (void)cJSON_AddBoolToObject(pRule, "enabled", true);

      if(!cJSON_AddItemToArray(pRules, pRule)) {
        tscError("sql security: failed to add rule to array");
      }
      pPattern->exported = true;
      added++;
    }
  }

  if (existingPatterns != NULL) {
    taosHashCleanup(existingPatterns);
  }

  // Write back to file
  if (added > 0) {
    char* jsonStr = cJSON_Print(pRoot);
    if (jsonStr != NULL) {
      fp = taosOpenFile(ruleFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
      if (fp != NULL) {
        if (taosWriteFile(fp, jsonStr, strlen(jsonStr)) != strlen(jsonStr)) {
          tscError("sql security: failed to write learned rules to %s", ruleFile);
        }
        TAOS_UNUSED(taosCloseFile(&fp));
        tscInfo("sql security: saved %d learned rules to %s", added, ruleFile);

        // Force reload rules by clearing the cache
        if (gSqlSecCtx.inited) {
          (void)taosThreadMutexLock(&gSqlSecCtx.lock);
          gSqlSecCtx.lastRuleMtime = 0;  // Force reload on next check
          (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
        }
      } else {
        tscWarn("sql security: failed to open rule file for writing: %s", ruleFile);
      }
      taosMemoryFree(jsonStr);
    }
  }

  cJSON_Delete(pRoot);
  (void)taosThreadMutexUnlock(&gLearningCtx.lock);

  return added;
}

static void sqlSecInitLearningOnce(void) {
  (void)taosThreadMutexInit(&gLearningCtx.lock, NULL);
  (void)taosThreadMutexInit(&gLearningCtx.threadLock, NULL);
  (void)taosThreadCondInit(&gLearningCtx.cond, NULL);
  gLearningCtx.enabled       = true;
  gLearningCtx.numOfPatterns = 0;
  gLearningCtx.startTs       = taosGetTimestampMs();
  gLearningCtx.nextRuleId    = 1000;  // Start from 1000 for learned rules
  atomic_store_8((int8_t volatile *)&gLearningCtx.threadRunning, 0);
  atomic_store_8((int8_t volatile *)&gLearningCtx.threadStop, 0);
  tscInfo("sql security: learning mode initialized");
}

static void sqlSecInitLearning(void) {
  (void)taosThreadOnce(&gLearningInitOnce, sqlSecInitLearningOnce);
}

static void sqlSecRecordPattern(const char* sql) {
  if (!tsWhitelistLearning || sql == NULL) return;

  if (!gLearningCtx.enabled) {
    sqlSecInitLearning();
  }

  char pattern[SQL_SEC_PATTERN_LEN] = {0};
  sqlSecGeneralizePattern(sql, pattern, sizeof(pattern));

  if (strlen(pattern) == 0) return;

  (void)taosThreadMutexLock(&gLearningCtx.lock);

  int64_t now = taosGetTimestampMs();
  int64_t periodMs = (int64_t)tsWhitelistLearningPeriod * 24 * 3600 * 1000;

  // Check if pattern already exists
  int32_t foundIdx = -1;
  for (int32_t i = 0; i < gLearningCtx.numOfPatterns; ++i) {
    if (strcmp(gLearningCtx.patterns[i].pattern, pattern) == 0) {
      foundIdx = i;
      break;
    }
  }

  if (foundIdx >= 0) {
    // Update existing pattern
    gLearningCtx.patterns[foundIdx].count++;
    gLearningCtx.patterns[foundIdx].lastSeenTs = now;

    // Check if threshold reached - log it
    if (gLearningCtx.patterns[foundIdx].count == tsWhitelistLearningThreshold) {
      tscInfo("sql security: pattern reached threshold, count:%d, pattern:%s", gLearningCtx.patterns[foundIdx].count,
              pattern);
    }
  } else {
    // Add new pattern
    if (gLearningCtx.numOfPatterns < SQL_SEC_MAX_PATTERNS) {
      SSqlPattern* pPattern = &gLearningCtx.patterns[gLearningCtx.numOfPatterns];
      tstrncpy(pPattern->pattern, pattern, SQL_SEC_PATTERN_LEN);
      pPattern->count = 1;
      pPattern->firstSeenTs = now;
      pPattern->lastSeenTs = now;
      pPattern->exported = false;
      gLearningCtx.numOfPatterns++;
    }
  }

  (void)taosThreadMutexUnlock(&gLearningCtx.lock);
}

// Learning thread function
static void* sqlSecLearningThreadFunc(void* arg) {
  tscInfo("sql security: learning export thread started");

  (void)taosThreadMutexLock(&gLearningCtx.threadLock);

  while (!atomic_load_8((int8_t volatile *)&gLearningCtx.threadStop)) {
    // Wait for 10 seconds or until signaled to stop
    struct timespec ts = {0};
    int64_t         nowMs = taosGetTimestampMs();
    int64_t         futMs = nowMs + 10 * 1000;  // check every 10 seconds
    ts.tv_sec = (time_t)(futMs / 1000);
    ts.tv_nsec = (long)((futMs % 1000) * 1000000);

    int ret = taosThreadCondTimedWait(&gLearningCtx.cond, &gLearningCtx.threadLock, &ts);

    if (atomic_load_8((int8_t volatile *)&gLearningCtx.threadStop)) {
      break;
    }

    // Check if there are patterns that reached threshold
    (void)taosThreadMutexLock(&gLearningCtx.lock);
    bool hasThresholdPatterns = false;
    for (int32_t i = 0; i < gLearningCtx.numOfPatterns; ++i) {
      if (gLearningCtx.patterns[i].count >= tsWhitelistLearningThreshold) {
        hasThresholdPatterns = true;
        break;
      }
    }
    (void)taosThreadMutexUnlock(&gLearningCtx.lock);

    // Export rules if there are patterns that reached threshold
    if (hasThresholdPatterns) {
      int32_t saved = sqlSecSaveLearnedRules(tsSqlSecurityRuleFile);
      if (saved > 0) {
        tscInfo("sql security: learning thread exported %d rules to %s", saved, tsSqlSecurityRuleFile);
      }
    }
  }

  (void)taosThreadMutexUnlock(&gLearningCtx.threadLock);

  tscInfo("sql security: learning export thread stopped");
  return NULL;
}

void sqlSecurityStartLearningThread() {
  if (!gLearningCtx.enabled) {
    sqlSecInitLearning();
  }

  (void)taosThreadMutexLock(&gLearningCtx.threadLock);

  if (atomic_load_8((int8_t volatile *)&gLearningCtx.threadRunning)) {
    (void)taosThreadMutexUnlock(&gLearningCtx.threadLock);
    tscInfo("sql security: learning thread already running");
    return;
  }

  atomic_store_8((int8_t volatile *)&gLearningCtx.threadStop, 0);

  TdThreadAttr attr;
  (void)taosThreadAttrInit(&attr);
  (void)taosThreadAttrSetDetachState(&attr, PTHREAD_CREATE_JOINABLE);

  if (taosThreadCreate(&gLearningCtx.thread, &attr, sqlSecLearningThreadFunc, NULL) != 0) {
    tscError("sql security: failed to create learning thread");
    (void)taosThreadMutexUnlock(&gLearningCtx.threadLock);
    return;
  }

  (void)taosThreadAttrDestroy(&attr);
  atomic_store_8((int8_t volatile *)&gLearningCtx.threadRunning, 1);

  (void)taosThreadMutexUnlock(&gLearningCtx.threadLock);

  tscInfo("sql security: learning thread started");
}

void sqlSecurityStopLearningThread() {
  (void)taosThreadMutexLock(&gLearningCtx.threadLock);

  if (!atomic_load_8((int8_t volatile *)&gLearningCtx.threadRunning)) {
    (void)taosThreadMutexUnlock(&gLearningCtx.threadLock);
    tscInfo("sql security: learning thread not running");
    return;
  }

  atomic_store_8((int8_t volatile *)&gLearningCtx.threadStop, 1);

  // Signal the thread to wake up
  (void)taosThreadCondSignal(&gLearningCtx.cond);

  (void)taosThreadMutexUnlock(&gLearningCtx.threadLock);

  // Wait for thread to finish
  (void)taosThreadJoin(gLearningCtx.thread, NULL);

  (void)taosThreadMutexLock(&gLearningCtx.threadLock);
  atomic_store_8((int8_t volatile *)&gLearningCtx.threadRunning, 0);
  (void)taosThreadMutexUnlock(&gLearningCtx.threadLock);

  tscInfo("sql security: learning thread stopped");
}

static void sqlSecClearRules(SSqlSecCtx* pCtx) {
  for (int32_t i = 0; i < pCtx->numOfRules; ++i) {
    if (pCtx->rules[i].regexInited) {
      regfree(&pCtx->rules[i].regex);
      pCtx->rules[i].regexInited = false;
    }
  }
  pCtx->numOfRules = 0;
}

static int32_t sqlSecParsePriority(const cJSON* pRule) {
  const cJSON* p = cJSON_GetObjectItemCaseSensitive((cJSON*)pRule, "priority");
  if (!cJSON_IsString(p) || p->valuestring == NULL) {
    return 2;
  }
  if (strcasecmp(p->valuestring, "HIGH") == 0) return 3;
  if (strcasecmp(p->valuestring, "LOW") == 0) return 1;
  return 2;
}

static ESqlSecAction sqlSecParseAction(const cJSON* pRule) {
  const cJSON* p = cJSON_GetObjectItemCaseSensitive((cJSON*)pRule, "action");
  if (cJSON_IsString(p) && p->valuestring != NULL && strcasecmp(p->valuestring, "ALLOW") == 0) {
    return SQL_SEC_ACTION_ALLOW;
  }
  return SQL_SEC_ACTION_DENY;
}

static bool sqlSecAppendRule(SSqlSecCtx* pCtx, const cJSON* pRule) {
  if (pCtx->numOfRules >= SQL_SEC_MAX_RULES) {
    return false;
  }

  const cJSON* pPattern = cJSON_GetObjectItemCaseSensitive((cJSON*)pRule, "pattern");
  if (!cJSON_IsString(pPattern) || pPattern->valuestring == NULL || strlen(pPattern->valuestring) == 0) {
    return false;
  }

  SSqlSecRule* pDst = &pCtx->rules[pCtx->numOfRules];
  memset(pDst, 0, sizeof(*pDst));

  const cJSON* pId = cJSON_GetObjectItemCaseSensitive((cJSON*)pRule, "ruleId");
  if (cJSON_IsNumber(pId)) {
    pDst->id = pId->valueint;
  }
  const cJSON* pName = cJSON_GetObjectItemCaseSensitive((cJSON*)pRule, "ruleName");
  if (cJSON_IsString(pName) && pName->valuestring != NULL) {
    tstrncpy(pDst->name, pName->valuestring, SQL_SEC_RULE_NAME_LEN);
  } else {
    tstrncpy(pDst->name, "unnamed_rule", SQL_SEC_RULE_NAME_LEN);
  }
  tstrncpy(pDst->pattern, pPattern->valuestring, SQL_SEC_RULE_PATTERN_LEN);
  pDst->priority = sqlSecParsePriority(pRule);
  pDst->action = sqlSecParseAction(pRule);

  const cJSON* pEnabled = cJSON_GetObjectItemCaseSensitive((cJSON*)pRule, "enabled");
  pDst->enabled = !cJSON_IsBool(pEnabled) || cJSON_IsTrue(pEnabled);

  if (!pDst->enabled) {
    pCtx->numOfRules++;
    return true;
  }

  if (regcomp(&pDst->regex, pDst->pattern, REG_EXTENDED | REG_NOSUB | REG_ICASE) != 0) {
    tscWarn("sql security: skip invalid regex rule:%s pattern:%s", pDst->name, pDst->pattern);
    return false;
  }

  pDst->regexInited = true;
  pCtx->numOfRules++;
  return true;
}

static void sqlSecLoadDefaultRules(SSqlSecCtx* pCtx) {
  cJSON* pRules = cJSON_CreateArray();
  if (pRules == NULL) return;

  cJSON* p1 = cJSON_CreateObject();
  if (p1 != NULL) {
    (void)cJSON_AddNumberToObject(p1, "ruleId", 1);
    (void)cJSON_AddStringToObject(p1, "ruleName", "DENY_UNION_SELECT");
    (void)cJSON_AddStringToObject(p1, "action", "DENY");
    (void)cJSON_AddStringToObject(p1, "pattern", "union[[:space:]]+select");
    (void)cJSON_AddBoolToObject(p1, "enabled", true);
    if(!cJSON_AddItemToArray(pRules, p1)) {
      tscError("sql security: failed to add rule to array");
    }
  }

  cJSON* p2 = cJSON_CreateObject();
  if (p2 != NULL) {
    (void)cJSON_AddNumberToObject(p2, "ruleId", 2);
    (void)cJSON_AddStringToObject(p2, "ruleName", "DENY_DROP_TABLE");
    (void)cJSON_AddStringToObject(p2, "action", "DENY");
    (void)cJSON_AddStringToObject(p2, "pattern", "drop[[:space:]]+table");
    (void)cJSON_AddBoolToObject(p2, "enabled", true);
    if(!cJSON_AddItemToArray(pRules, p2)) {
      tscError("sql security: failed to add rule to array");
    }
  }

  int32_t n = cJSON_GetArraySize(pRules);
  for (int32_t i = 0; i < n; ++i) {
    cJSON* pRule = cJSON_GetArrayItem(pRules, i);
    (void)sqlSecAppendRule(pCtx, pRule);
  }
  cJSON_Delete(pRules);
}

static int64_t sqlSecGetFileMtime(const char* path) {
  int64_t fsize = 0;
  int64_t mtime = 0;
  if (path == NULL || path[0] == 0) return -1;
  if (taosStatFile(path, &fsize, &mtime, NULL) < 0) return -1;
  return mtime;
}

static void sqlSecInitRulesOnce(void) {
  (void)taosThreadMutexInit(&gSqlSecCtx.lock, NULL);
  gSqlSecCtx.inited       = true;
  gSqlSecCtx.numOfRules   = 0;
  gSqlSecCtx.lastLoadTsMs = 0;
  gSqlSecCtx.lastRuleMtime = 0;
  gSqlSecCtx.lastRulePath[0] = '\0';
}

static void sqlSecReloadRulesIfNeeded(const char* ruleFile) {
  if (ruleFile == NULL || ruleFile[0] == 0) return;

  (void)taosThreadOnce(&gSqlSecInitOnce, sqlSecInitRulesOnce);

  // Check if reload is needed (with lock to avoid race condition)
  (void)taosThreadMutexLock(&gSqlSecCtx.lock);
  int64_t now = taosGetTimestampMs();
  if (now - gSqlSecCtx.lastLoadTsMs < 1000) {
    (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
    return;
  }
  gSqlSecCtx.lastLoadTsMs = now;

  int64_t mtime = sqlSecGetFileMtime(ruleFile);
  bool    pathChanged = (strcmp(ruleFile, gSqlSecCtx.lastRulePath) != 0);
  if (pathChanged) {
    tstrncpy(gSqlSecCtx.lastRulePath, ruleFile, sizeof(gSqlSecCtx.lastRulePath));
  }
  if (!pathChanged && mtime == gSqlSecCtx.lastRuleMtime && gSqlSecCtx.numOfRules > 0) {
    (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
    return;
  }

  sqlSecClearRules(&gSqlSecCtx);
  gSqlSecCtx.lastRuleMtime = mtime;

  if (mtime < 0) {
    sqlSecLoadDefaultRules(&gSqlSecCtx);
    (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
    return;
  }

  int64_t   fsize = 0;
  TdFilePtr fp = taosOpenFile(ruleFile, TD_FILE_READ);
  if (fp == NULL) {
    tscWarn("sql security: failed to open rule file:%s, using default rules", ruleFile);
    sqlSecLoadDefaultRules(&gSqlSecCtx);
    (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
    return;
  }

  if (taosFStatFile(fp, &fsize, NULL) != TSDB_CODE_SUCCESS) {
    TAOS_UNUSED(taosCloseFile(&fp));
    tscWarn("sql security: failed to stat rule file:%s, using default rules", ruleFile);
    sqlSecLoadDefaultRules(&gSqlSecCtx);
    (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
    return;
  }
  if (fsize <= 0 || fsize > 4 * 1024 * 1024) {
    TAOS_UNUSED(taosCloseFile(&fp));
    tscWarn("sql security: invalid rule file size:%" PRId64 ", using default rules", fsize);
    sqlSecLoadDefaultRules(&gSqlSecCtx);
    (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
    return;
  }

  char* pBuf = taosMemoryCalloc(1, (size_t)fsize + 1);
  if (pBuf == NULL) {
    TAOS_UNUSED(taosCloseFile(&fp));
    tscWarn("sql security: failed to allocate memory for rule file, using default rules");
    sqlSecLoadDefaultRules(&gSqlSecCtx);
    (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
    return;
  }
  int64_t nread = taosReadFile(fp, pBuf, (int64_t)fsize);
  TAOS_UNUSED(taosCloseFile(&fp));
  if (nread != fsize) {
    taosMemoryFree(pBuf);
    tscWarn("sql security: failed to read rule file, using default rules");
    sqlSecLoadDefaultRules(&gSqlSecCtx);
    (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
    return;
  }

  cJSON* pRoot = cJSON_Parse(pBuf);
  taosMemoryFree(pBuf);
  if (pRoot == NULL) {
    tscWarn("sql security: failed to parse rule file JSON, using default rules");
    sqlSecLoadDefaultRules(&gSqlSecCtx);
    (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
    return;
  }

  cJSON* pRules = cJSON_GetObjectItemCaseSensitive(pRoot, "rules");
  if (cJSON_IsArray(pRules)) {
    int32_t n = cJSON_GetArraySize(pRules);
    for (int32_t i = 0; i < n; ++i) {
      cJSON* pRule = cJSON_GetArrayItem(pRules, i);
      (void)sqlSecAppendRule(&gSqlSecCtx, pRule);
    }
  }
  cJSON_Delete(pRoot);

  if (gSqlSecCtx.numOfRules == 0) {
    tscWarn("sql security: no valid rules loaded, using default rules");
    sqlSecLoadDefaultRules(&gSqlSecCtx);
  } else {
    tscInfo("sql security: loaded %d rules from %s", gSqlSecCtx.numOfRules, ruleFile);
  }
  (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);
}

static bool sqlSecIsModeWhitelist(int32_t mode) { return mode == 1 || mode == 3; }
static bool sqlSecIsModeBlacklist(int32_t mode) { return mode == 2 || mode == 3; }

static int32_t sqlSecDecideFromMatches(const SSqlSecMatchResult* pRes, int8_t enabled, int32_t mode) {
  if (!enabled || mode == 0) return TSDB_CODE_SUCCESS;

  bool whitelist = sqlSecIsModeWhitelist(mode);
  bool blacklist = sqlSecIsModeBlacklist(mode);

  // Blacklist mode: deny if matched and no whitelist override
  if (blacklist && pRes->denyMatched && (!whitelist || !pRes->allowMatched)) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }

  // Whitelist mode: deny if not matched
  if (whitelist && !pRes->allowMatched) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }

  // Mixed mode: both matched, compare priority
  // Rule: Higher priority wins. If equal, blacklist wins (deny >= allow means deny)
  if (blacklist && whitelist && pRes->denyMatched && pRes->allowMatched) {
    if (pRes->denyPriority >= pRes->allowPriority) {
      return TSDB_CODE_PAR_PERMISSION_DENIED;
    }
  }

  return TSDB_CODE_SUCCESS;
}

/* Simple substring check as fallback (sql already lowercased) */
static bool sqlSecSimpleDenyMatch(const char* sql, int32_t sqlLen) {
  if (sql == NULL || sqlLen <= 0) return false;
  if (strstr(sql, "union select") != NULL) return true;
  if (strstr(sql, "drop table") != NULL) return true;
  return false;
}

static SAppInstServerCFG* sqlSecGetCfg(SRequestObj* pRequest) {
  if (pRequest == NULL || pRequest->pTscObj == NULL || pRequest->pTscObj->pAppInfo == NULL) return NULL;
  return &pRequest->pTscObj->pAppInfo->serverCfg;
}

int32_t sqlSecurityCheckStringLevel(SRequestObj* pRequest, const char* sql, int32_t sqlLen) {
  SAppInstServerCFG* pCfg = sqlSecGetCfg(pRequest);

  // Manage learning thread based on tsWhitelistLearning
  if (tsWhitelistLearning) {
    // Learning mode enabled, ensure thread is running
    if (!atomic_load_8((int8_t volatile *)&gLearningCtx.threadRunning)) {
      sqlSecurityStartLearningThread();
    }
    sqlSecRecordPattern(sql);
  } else {
    // Learning mode disabled, ensure thread is stopped
    if (atomic_load_8((int8_t volatile *)&gLearningCtx.threadRunning)) {
      sqlSecurityStopLearningThread();
    }
  }

  if (pCfg == NULL || !tsSqlSecurityEnabled || !tsSqlSecurityStringCheck || sql == NULL || sqlLen <= 0) {
    return TSDB_CODE_SUCCESS;
  }
  if (sqlSecIsModeBlacklist(tsSqlSecurityWhitelistMode) && sqlSecSimpleDenyMatch(sql, sqlLen)) {
    tscWarn("req:0x%" PRIx64 ", sql security string check denied (simple match), sql:%s", pRequest->self, sql);
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }

  sqlSecReloadRulesIfNeeded(tsSqlSecurityRuleFile);

  SSqlSecMatchResult m = {0};
  m.denyPriority = -1;
  m.allowPriority = -1;

  (void)taosThreadMutexLock(&gSqlSecCtx.lock);
  for (int32_t i = 0; i < gSqlSecCtx.numOfRules; ++i) {
    SSqlSecRule* pRule = &gSqlSecCtx.rules[i];
    if (!pRule->enabled || !pRule->regexInited) {
      continue;
    }
    if (regexec(&pRule->regex, sql, 0, NULL, 0) != 0) {
      continue;
    }
    if (pRule->action == SQL_SEC_ACTION_DENY) {
      if (!m.denyMatched || pRule->priority > m.denyPriority) {
        m.denyMatched = true;
        m.denyPriority = pRule->priority;
        m.pDenyRule = pRule;
      }
    } else {
      if (!m.allowMatched || pRule->priority > m.allowPriority) {
        m.allowMatched = true;
        m.allowPriority = pRule->priority;
        m.pAllowRule = pRule;
      }
    }
  }
  (void)taosThreadMutexUnlock(&gSqlSecCtx.lock);

  int32_t code = sqlSecDecideFromMatches(&m, tsSqlSecurityEnabled, tsSqlSecurityWhitelistMode);
  if (code != TSDB_CODE_SUCCESS) {
    tscWarn("req:0x%" PRIx64 ", sql security string check denied, mode:%d, denyRule:%s, allowRule:%s, sql:%s",
            pRequest->self, tsSqlSecurityWhitelistMode, m.pDenyRule ? m.pDenyRule->name : "none",
            m.pAllowRule ? m.pAllowRule->name : "none", sql);
  }
  return code;
}

static bool sqlSecIsValueTrue(const SNode* pNode) {
  if (pNode == NULL) return false;
  if (nodeType((SNode*)pNode) != QUERY_NODE_VALUE) return false;

  const SValueNode* pVal = (const SValueNode*)pNode;
  if (pVal->isNull) return false;
  if (pVal->node.resType.type == TSDB_DATA_TYPE_BOOL) return pVal->datum.b;
  if (IS_INTEGER_TYPE(pVal->node.resType.type)) return pVal->datum.i != 0;
  if (pVal->literal != NULL && (strcasecmp(pVal->literal, "true") == 0 || strcmp(pVal->literal, "1") == 0)) {
    return true;
  }
  return false;
}

static bool sqlSecIsValueEqual(const SNode* pLeft, const SNode* pRight) {
  if (pLeft == NULL || pRight == NULL) return false;
  if (nodeType(pLeft) != QUERY_NODE_VALUE || nodeType(pRight) != QUERY_NODE_VALUE) return false;

  const SValueNode* l = (const SValueNode*)pLeft;
  const SValueNode* r = (const SValueNode*)pRight;

  if (l->isNull || r->isNull) return false;

  // Check if both are numbers and equal
  if (IS_NUMERIC_TYPE(l->node.resType.type) && IS_NUMERIC_TYPE(r->node.resType.type)) {
    if (IS_INTEGER_TYPE(l->node.resType.type) && IS_INTEGER_TYPE(r->node.resType.type)) {
      return l->datum.i == r->datum.i;
    }
    if (IS_FLOAT_TYPE(l->node.resType.type) && IS_FLOAT_TYPE(r->node.resType.type)) {
      return l->datum.d == r->datum.d;
    }
  }

  // Check if both are strings and equal
  if (l->literal != NULL && r->literal != NULL) {
    return strcmp(l->literal, r->literal) == 0;
  }

  return false;
}

static bool sqlSecIsConstEqTrue(const SNode* pNode) {
  if (pNode == NULL || nodeType((SNode*)pNode) != QUERY_NODE_OPERATOR) return false;
  const SOperatorNode* pOp = (const SOperatorNode*)pNode;
  if (pOp->opType != OP_TYPE_EQUAL || pOp->pLeft == NULL || pOp->pRight == NULL) return false;

  // Check if both sides are true values (TRUE, 1, etc.)
  if (sqlSecIsValueTrue(pOp->pLeft) && sqlSecIsValueTrue(pOp->pRight)) {
    return true;
  }

  // Check if both sides are equal constant values (1=1, 'a'='a', 0=0, etc.)
  if (sqlSecIsValueEqual(pOp->pLeft, pOp->pRight)) {
    return true;
  }

  return false;
}

static EDealRes sqlSecAstWalker(SNode* pNode, void* pContext) {
  if (pNode == NULL || pContext == NULL) {
    return DEAL_RES_CONTINUE;
  }

  SSqlSecAstResult* pRes = (SSqlSecAstResult*)pContext;

  // Check logic conditions (OR with true values)
  if (nodeType(pNode) == QUERY_NODE_LOGIC_CONDITION) {
    SLogicConditionNode* pCond = (SLogicConditionNode*)pNode;
    if (pCond->condType == LOGIC_COND_TYPE_OR) {
      SNode* pParam = NULL;
      FOREACH(pParam, pCond->pParameterList) {
        if (sqlSecIsValueTrue(pParam) || sqlSecIsConstEqTrue(pParam)) {
          pRes->hasOrTrue = true;
          return DEAL_RES_END;
        }
      }
    }
  }
  // Check operators (constant equal true like 1=1)
  else if (nodeType(pNode) == QUERY_NODE_OPERATOR) {
    if (sqlSecIsConstEqTrue(pNode)) {
      pRes->hasOrTrue = true;
      return DEAL_RES_END;
    }
  }
  // Check dangerous functions
  else if (nodeType(pNode) == QUERY_NODE_FUNCTION) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (strcasecmp(pFunc->functionName, "load_file") == 0 || strcasecmp(pFunc->functionName, "exec") == 0 ||
        strcasecmp(pFunc->functionName, "eval") == 0 || strcasecmp(pFunc->functionName, "sleep") == 0 ||
        strcasecmp(pFunc->functionName, "system") == 0 || strcasecmp(pFunc->functionName, "shell") == 0) {
      pRes->hasDangerFunc = true;
      return DEAL_RES_END;
    }
  }
  // Check JOIN conditions
  else if (nodeType(pNode) == QUERY_NODE_JOIN_TABLE) {
    SJoinTableNode* pJoin = (SJoinTableNode*)pNode;
    if (pJoin->pOnCond != NULL) {
      // Check if JOIN condition is always true (e.g., ON 1=1)
      if (sqlSecIsValueTrue(pJoin->pOnCond) || sqlSecIsConstEqTrue(pJoin->pOnCond)) {
        pRes->hasUnsafeJoin = true;
        return DEAL_RES_END;
      }
    }
  }

  return DEAL_RES_CONTINUE;
}

int32_t sqlSecurityCheckASTLevel(SRequestObj* pRequest, SQuery* pQuery) {
  SAppInstServerCFG* pCfg = sqlSecGetCfg(pRequest);
  if (pCfg == NULL || !tsSqlSecurityEnabled || !tsSqlSecurityASTCheck || pRequest == NULL || pQuery == NULL ||
      pQuery->pRoot == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SSqlSecAstResult res = {0};
  nodesWalkExpr((SNode*)pQuery->pRoot, sqlSecAstWalker, &res);

  if (res.hasOrTrue || res.hasDangerFunc || res.hasUnsafeJoin) {
        tscWarn(
        "req:0x%" PRIx64
        ", sql security AST check denied, hasOrTrue:%d, hasDangerFunc:%d, hasUnsafeJoin:%d, sql:%s",
        pRequest->self, res.hasOrTrue, res.hasDangerFunc, res.hasUnsafeJoin,
        pRequest->sqlstr ? pRequest->sqlstr : "");
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }

  return TSDB_CODE_SUCCESS;
}
