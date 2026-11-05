/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "dmRewrite.h"
#include "cJSON.h"
#include "dmUtil.h"
#include "mnode.h"
#include "osEnv.h"
#include "tconfig.h"
#include "tencrypt.h"
#include "tglobal.h"
#include "tjson.h"
#include "tmisce.h"
#include "ttime.h"
#include "version.h"
#include "wal.h"

#if defined(TD_ENTERPRISE) && defined(LINUX)
extern int32_t cryptLoadProviders(void);
#endif

typedef struct {
  int32_t id;
  int8_t isMnode;
  SDmEndpointMapping map;
} SRewriteNode;

typedef struct {
  const SDmRewriteEndpointsOption *option;
  const char *stage;
  SConfig *cfg;
  SArray *nodes;
  SArray *disks;  // SDiskCfg, level-0 primary first
  cJSON *report;
  cJSON *errors;
  cJSON *changes;
  TdFilePtr lock;
  int32_t dnodeId;
  int64_t clusterId;
  int32_t firstError;
  bool mnodeEncrypted;
  char cfgFile[PATH_MAX];
  char dnodeFile[PATH_MAX];
} SRewriteCheck;

static int32_t rewriteInt(const cJSON *json, const char *key, int64_t *value) {
  const cJSON *item = cJSON_GetObjectItemCaseSensitive(json, key);
  if (cJSON_IsString(item)) {
    const char *p = item->valuestring;
    if (*p == '-') ++p;
    if (*p == '\0') return TSDB_CODE_INVALID_JSON_FORMAT;
    for (; *p != '\0'; ++p) {
      if (*p < '0' || *p > '9') return TSDB_CODE_INVALID_JSON_FORMAT;
    }
    return taosStr2int64(item->valuestring, value);
  }
  if (cJSON_IsNumber(item) && item->valuedouble >= -9007199254740991.0 &&
      item->valuedouble <= 9007199254740991.0) {
    *value = (int64_t)item->valuedouble;
    if ((double)*value == item->valuedouble) return 0;
  }
  return TSDB_CODE_INVALID_JSON_FORMAT;
}

static int32_t rewriteAddInt(cJSON *json, const char *key, int64_t value) {
  char str[32];
  snprintf(str, sizeof(str), "%" PRId64, value);
  return tjsonAddStringToObject(json, key, str);
}

static int32_t rewriteAttach(cJSON *json, const char *key, cJSON *value) {
  if (value == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  if (!cJSON_AddItemToObject(json, key, value)) {
    cJSON_Delete(value);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}

static int32_t rewriteErrorDetail(SRewriteCheck *check, const char *file,
                                  const char *test, int32_t code,
                                  const char *detail) {
  if (code == 0) return 0;
  if (check->firstError == 0) check->firstError = code;
  const char *message = detail != NULL ? detail : tstrerror(code);
  const char *stage = check->stage != NULL ? check->stage : "precheck";
  printf("%s failed: %s, %s: %s\n", stage, file, test, message);
  cJSON *error = cJSON_CreateObject();
  if (error == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  int32_t ret = tjsonAddStringToObject(error, "file", file);
  if (ret == 0) ret = tjsonAddStringToObject(error, "check", test);
  if (ret == 0) ret = tjsonAddStringToObject(error, "message", message);
  if (ret == 0) ret = tjsonAddDoubleToObject(error, "code", code);
  if (ret == 0 && !cJSON_AddItemToArray(check->errors, error)) {
    ret = TSDB_CODE_OUT_OF_MEMORY;
  }
  if (ret != 0) cJSON_Delete(error);
  return ret;
}

static int32_t rewriteError(SRewriteCheck *check, const char *file,
                            const char *test, int32_t code) {
  return rewriteErrorDetail(check, file, test, code, NULL);
}

static int32_t rewritePath(char *path, size_t size, const char *dir,
                           const char *relative) {
  int32_t n = snprintf(path, size, "%s%s%s", dir, TD_DIRSEP, relative);
  return n < 0 || n >= size ? TSDB_CODE_OUT_OF_BUFFER : 0;
}

static int32_t rewriteReadJson(const char *path, cJSON **json) {
  char *content = NULL;
  int32_t len = 0;
  *json = NULL;
  TAOS_CHECK_RETURN(taosReadCfgFile(path, &content, &len));
  const char *end = NULL;
  *json = cJSON_ParseWithOpts(content, &end, true);
  taosMemoryFree(content);
  return *json == NULL ? TSDB_CODE_INVALID_JSON_FORMAT : 0;
}

static bool rewriteSameEp(const SEp *left, const SEp *right) {
  return left->port == right->port &&
         strcmp(left->fqdn, right->fqdn) == 0;
}

static SRewriteNode *rewriteNode(SRewriteCheck *check, int32_t id) {
  for (size_t i = 0; i < taosArrayGetSize(check->nodes); ++i) {
    SRewriteNode *node = taosArrayGet(check->nodes, i);
    if (node->id == id) return node;
  }
  return NULL;
}

static int32_t rewriteReadEp(const cJSON *json, const char *hostKey,
                             const char *portKey, SEp *ep) {
  const char *host =
      cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(json, hostKey));
  int64_t port = 0;
  if (host == NULL || host[0] == '\0' ||
      strlen(host) >= sizeof(ep->fqdn)) return TSDB_CODE_INVALID_CFG;
  TAOS_CHECK_RETURN(rewriteInt(json, portKey, &port));
  if (port < 1 || port > DM_REWRITE_ENDPOINTS_MAX_PORT) {
    return TSDB_CODE_INVALID_CFG;
  }
  tstrncpy(ep->fqdn, host, sizeof(ep->fqdn));
  ep->port = (uint16_t)port;
  return 0;
}

static cJSON *rewriteEpJson(const SEp *ep) {
  cJSON *json = cJSON_CreateObject();
  if (json == NULL) return NULL;
  if (tjsonAddStringToObject(json, "fqdn", ep->fqdn) != 0 ||
      tjsonAddDoubleToObject(json, "port", ep->port) != 0) {
    cJSON_Delete(json);
    return NULL;
  }
  return json;
}

/* ------------------------------------------------------------------------- */
/* Endpoint sites                                                             */
/* ------------------------------------------------------------------------- */

// One endpoint inside a persisted document, together with the way it is stored
// there. The checks and the rewrite walk the same documents through the same
// description, so the shape of a file, the acceptance rule and the error
// message exist once; only the action taken on each site differs.
typedef struct {
  const char *file;
  cJSON      *object;   // object that holds the endpoint
  const char *hostKey;  // address, or the whole "host:port" for a text site
  const char *portKey;  // pair sites only
  const char *field;    // name used in the report and in the console output
  int32_t     dnodeId;  // 0: the address itself identifies the dnode
  bool        text;     // the endpoint is stored as one "host:port" string
  const char *role;     // "voter"/"learner" for a Raft member, else NULL
} SRewriteSite;

// What to do with one validated site: report it, or report and rewrite it.
typedef int32_t (*SRewriteSitePolicy)(SRewriteCheck *check, void *param,
                                      const SRewriteSite *site,
                                      const SRewriteNode *node, const SEp *ep);

// Resolves the endpoint found at a site to the dnode that owns it, and checks
// that it is the endpoint the mapping expects. A rejection is recorded in the
// report (with the value found and the value expected) and reported as false,
// so that a scan can keep checking the remaining sites.
static bool rewriteAcceptEp(SRewriteCheck *check, const SRewriteSite *site,
                             const SEp *ep, const SRewriteNode **pNode) {
  const int32_t       id = site->dnodeId;
  const SRewriteNode *node = id > 0 ? rewriteNode(check, id) : NULL;
  if (id <= 0) {
    for (size_t i = 0; i < taosArrayGetSize(check->nodes); ++i) {
      const SRewriteNode *candidate = taosArrayGet(check->nodes, i);
      if (rewriteSameEp(ep, &candidate->map.source)) {
        node = candidate;
        break;
      }
    }
  }
  if (node == NULL || !rewriteSameEp(ep, &node->map.source)) {
    char detail[256];
    if (node != NULL) {
      (void)snprintf(detail, sizeof(detail),
          "dnode %d uses %s:%u here, but the mapping expects %s:%u", node->id, ep->fqdn,
          (unsigned)ep->port, node->map.source.fqdn, (unsigned)node->map.source.port);
    } else if (id > 0) {
      (void)snprintf(detail, sizeof(detail),
          "dnode %d uses %s:%u here, which is not in the local dnode list", id, ep->fqdn,
          (unsigned)ep->port);
    } else {
      (void)snprintf(detail, sizeof(detail),
          "%s:%u here is not a mapped endpoint, so it cannot be rewritten", ep->fqdn,
          (unsigned)ep->port);
    }
    (void)rewriteErrorDetail(check, site->file, site->field,
                             TSDB_CODE_INVALID_CFG, detail);
    return false;
  }
  *pNode = node;
  return true;
}

// Appends one endpoint change to the precheck report.
static int32_t rewriteRecordChange(SRewriteCheck *check, const SRewriteSite *site,
                                    const SEp *ep, const SEp *target) {
  cJSON *change = cJSON_CreateObject();
  if (change == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  int32_t code = tjsonAddStringToObject(change, "file", site->file);
  if (code == 0) code = tjsonAddStringToObject(change, "field", site->field);
  if (code == 0) code = rewriteAttach(change, "source", rewriteEpJson(ep));
  if (code == 0) code = rewriteAttach(change, "target", rewriteEpJson(target));
  if (code == 0 && !cJSON_AddItemToArray(check->changes, change)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
  if (code != 0) cJSON_Delete(change);
  return code;
}

// Precheck and the first pass of apply: validate the site and report it. A
// rejection was already recorded by rewriteAcceptEp, and the scan carries on so
// that every remaining site is reported in the same run.
static int32_t rewriteCheckPolicy(SRewriteCheck *check, void *param,
                                   const SRewriteSite *site,
                                   const SRewriteNode *node, const SEp *ep) {
  (void)param;
  if (node == NULL) return 0;
  if (rewriteSameEp(&node->map.source, &node->map.target)) return 0;
  return rewriteRecordChange(check, site, ep, &node->map.target);
}

// Validates one endpoint found outside a document the walkers cover, such as a
// dnode row of the mnode snapshot.
static int32_t rewriteCheckEp(SRewriteCheck *check, const char *file,
                              const char *field, int32_t id, const SEp *ep) {
  SRewriteSite        site = {.file = file, .field = field, .dnodeId = id};
  const SRewriteNode *node = NULL;
  (void)rewriteAcceptEp(check, &site, ep, &node);
  return rewriteCheckPolicy(check, NULL, &site, node, ep);
}

/* ------------------------------------------------------------------------- */
/* Document walkers                                                           */
/* ------------------------------------------------------------------------- */

// A Raft member names its cluster and its role next to its endpoint.
static int32_t rewriteSyncMember(SRewriteCheck *check, const cJSON *item,
                                  SRewriteSite *site) {
  int64_t clusterId = 0;
  TAOS_CHECK_RETURN(rewriteInt(item, "clusterId", &clusterId));
  // Bootstrap Raft configurations can retain an unbound cluster ID.
  if (clusterId != 0 && clusterId != check->clusterId) {
    return TSDB_CODE_INVALID_CFG;
  }
  const cJSON *isReplica = cJSON_GetObjectItemCaseSensitive(item, "isReplica");
  site->role = "voter";
  if (isReplica == NULL) return 0;
  if (!cJSON_IsString(isReplica) ||
      (strcmp(isReplica->valuestring, "true") != 0 &&
       strcmp(isReplica->valuestring, "false") != 0)) {
    return TSDB_CODE_INVALID_CFG;
  }
  site->role = strcmp(isReplica->valuestring, "true") == 0 ? "voter" : "learner";
  return 0;
}

// One entry of a member or endpoint list: the key that holds the dnode ID, the
// keys that hold the address, and the text both sides use to name it.
typedef struct {
  const char *idKey;
  const char *hostKey;
  const char *portKey;
  const char *field;
  bool        syncMember;  // Raft member: clusterId and isReplica take part
  bool        hasSelf;     // the local dnode must be a member
} SRewriteArrayShape;

static int32_t rewriteWalkArray(SRewriteCheck *check, const char *path,
                                 const cJSON *array,
                                 const SRewriteArrayShape *shape,
                                 SRewriteSitePolicy policy, void *param) {
  if (!cJSON_IsArray(array) || cJSON_GetArraySize(array) == 0) {
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }
  bool hasSelf = false;
  for (int32_t i = 0; i < cJSON_GetArraySize(array); ++i) {
    const cJSON *item = cJSON_GetArrayItem(array, i);
    int64_t      id = 0;
    TAOS_CHECK_RETURN(rewriteInt(item, shape->idKey, &id));
    if (id <= 0 || id > INT32_MAX) return TSDB_CODE_INVALID_CFG;
    // A list may name each dnode only once.
    for (int32_t j = 0; j < i; ++j) {
      int64_t other = 0;
      TAOS_CHECK_RETURN(
          rewriteInt(cJSON_GetArrayItem(array, j), shape->idKey, &other));
      if (other == id) return TSDB_CODE_INVALID_CFG;
    }
    SRewriteSite site = {.file = path, .object = (cJSON *)item,
                         .hostKey = shape->hostKey, .portKey = shape->portKey,
                         .field = shape->field, .dnodeId = (int32_t)id};
    if (shape->syncMember) {
      TAOS_CHECK_RETURN(rewriteSyncMember(check, item, &site));
    }
    hasSelf |= (int32_t)id == check->dnodeId;
    SEp ep = {0};
    TAOS_CHECK_RETURN(rewriteReadEp(item, shape->hostKey, shape->portKey, &ep));
    const SRewriteNode *node = NULL;
    (void)rewriteAcceptEp(check, &site, &ep, &node);
    TAOS_CHECK_RETURN(policy(check, param, &site, node, &ep));
  }
  return shape->hasSelf && !hasSelf ? TSDB_CODE_INVALID_CFG : 0;
}

static const SRewriteArrayShape rewriteDnodes = {
    .idKey = "id", .hostKey = "fqdn", .portKey = "port",
    .field = "dnodes[].fqdn/port"};
static const SRewriteArrayShape rewriteReplicas = {
    .idKey = "id", .hostKey = "fqdn", .portKey = "port",
    .field = "replicas[].fqdn/port", .hasSelf = true};
static const SRewriteArrayShape rewriteMembersShape = {
    .idKey = "nodeId", .hostKey = "nodeFqdn", .portKey = "nodePort",
    .field = "nodeInfo[].nodeFqdn/nodePort", .syncMember = true,
    .hasSelf = true};

// `configs` holds the local endpoint as fqdn/serverPort and the seed endpoints
// as "host:port" strings.
static int32_t rewriteWalkConfigs(SRewriteCheck *check, const char *path,
                                   cJSON *configs,
                                   SRewriteSitePolicy policy, void *param) {
  const SRewriteNode *self = rewriteNode(check, check->dnodeId);
  if (self == NULL) return TSDB_CODE_INVALID_CFG;
  cJSON *host = cJSON_GetObjectItemCaseSensitive(configs, "fqdn");
  cJSON *port = cJSON_GetObjectItemCaseSensitive(configs, "serverPort");
  if (host != NULL || port != NULL) {
    SEp local = self->map.source;
    if (host != NULL) {
      if (!cJSON_IsString(host) ||
          strlen(host->valuestring) >= sizeof(local.fqdn)) {
        return TSDB_CODE_INVALID_CFG;
      }
      tstrncpy(local.fqdn, host->valuestring, sizeof(local.fqdn));
    }
    if (port != NULL) {
      int64_t value = 0;
      TAOS_CHECK_RETURN(rewriteInt(configs, "serverPort", &value));
      if (value < 1 || value > DM_REWRITE_ENDPOINTS_MAX_PORT) {
        return TSDB_CODE_INVALID_CFG;
      }
      local.port = (uint16_t)value;
    }
    SRewriteSite site = {.file = path, .object = configs, .hostKey = "fqdn",
                         .portKey = "serverPort", .field = "fqdn/serverPort",
                         .dnodeId = self->id};
    const SRewriteNode *node = NULL;
    (void)rewriteAcceptEp(check, &site, &local, &node);
    TAOS_CHECK_RETURN(policy(check, param, &site, node, &local));
  }
  const char *names[] = {"firstEp", "secondEp"};
  for (size_t i = 0; i < tListLen(names); ++i) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(configs, names[i]);
    if (item == NULL) continue;
    if (!cJSON_IsString(item)) return TSDB_CODE_INVALID_CFG;
    if (item->valuestring[0] == '\0') continue;
    SEp ep = {0};
    TAOS_CHECK_RETURN(taosGetFqdnPortFromEp(item->valuestring, &ep));
    SRewriteSite site = {.file = path, .object = configs,
                         .hostKey = names[i], .field = names[i], .text = true};
    const SRewriteNode *node = NULL;
    (void)rewriteAcceptEp(check, &site, &ep, &node);
    TAOS_CHECK_RETURN(policy(check, param, &site, node, &ep));
  }
  return 0;
}

static int32_t rewriteLoadCfg(SRewriteCheck *check, const char *cfgDir) {
  TAOS_CHECK_RETURN(taosExpandDir(cfgDir, check->cfgFile, sizeof(check->cfgFile)));
  if (taosIsDir(check->cfgFile)) {
    char dir[PATH_MAX];
    tstrncpy(dir, check->cfgFile, sizeof(dir));
    TAOS_CHECK_RETURN(rewritePath(check->cfgFile, sizeof(check->cfgFile),
                                  dir, "taos.cfg"));
  }
  if (!taosCheckExistFile(check->cfgFile)) return TSDB_CODE_INVALID_CFG;
  TAOS_CHECK_RETURN(cfgInit(&check->cfg));
  TAOS_CHECK_RETURN(cfgAddDir(check->cfg, "dataDir", tsDataDir,
      CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM));
  const char *keys[] = {"fqdn", "firstEp", "secondEp", "encryptExtDir"};
  for (size_t i = 0; i < tListLen(keys); ++i) {
    TAOS_CHECK_RETURN(cfgAddString(check->cfg, keys[i], "",
        CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM));
  }
  TAOS_CHECK_RETURN(cfgAddInt32(check->cfg, "serverPort", tsServerPort,
      1, DM_REWRITE_ENDPOINTS_MAX_PORT, CFG_SCOPE_SERVER, CFG_DYN_NONE,
      CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM));
  TAOS_CHECK_RETURN(cfgAddBool(check->cfg, "enableIpv6", tsEnableIpv6,
      CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM));
  TAOS_CHECK_RETURN(cfgLoad(check->cfg, CFG_STYPE_CFG_FILE, check->cfgFile));
  tsEnableIpv6 = cfgGetItem(check->cfg, "enableIpv6")->bval;
  SConfigItem *data = cfgGetItem(check->cfg, "dataDir");
  if (data == NULL) return TSDB_CODE_CFG_NOT_FOUND;
  check->disks = taosArrayInit(1, sizeof(SDiskCfg));
  if (check->disks == NULL) return terrno;
  // TFS assigns ID 0 to primary, then level-0 disks in configuration order.
  SDiskCfg primary = {0};
  bool found = false;
  for (size_t i = 0; i < taosArrayGetSize(data->array); ++i) {
    const SDiskCfg *disk = taosArrayGet(data->array, i);
    if (disk->level == 0 && disk->primary) {
      if (found) return TSDB_CODE_INVALID_CFG;
      primary = *disk;
      found = true;
    }
  }
  if (!found) {
    if (taosArrayGetSize(data->array) != 0) return TSDB_CODE_INVALID_CFG;
    tstrncpy(primary.dir, data->str, sizeof(primary.dir));
    primary.primary = 1;
  }
  if (taosArrayPush(check->disks, &primary) == NULL) return terrno;
  for (size_t i = 0; i < taosArrayGetSize(data->array); ++i) {
    const SDiskCfg *disk = taosArrayGet(data->array, i);
    if (disk->level == 0 && !disk->primary) {
      if (taosArrayPush(check->disks, disk) == NULL) return terrno;
    }
  }
  
  for (size_t i = 0; i < taosArrayGetSize(check->disks); ++i) {
    const SDiskCfg *disk = taosArrayGet(check->disks, i);
    if (!taosIsDir(disk->dir) || disk->disable < 0 || disk->disable > 1) {
      return TSDB_CODE_INVALID_CFG;
    }
    for (size_t j = 0; j < i; ++j) {
      const SDiskCfg *other = taosArrayGet(check->disks, j);
      if (strcmp(disk->dir, other->dir) == 0) return TSDB_CODE_INVALID_CFG;
    }
  }
  tstrncpy(tsDataDir, primary.dir, PATH_MAX);
  return 0;
}

static int32_t rewriteLoadKeys(void) {
  char path[PATH_MAX];
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), tsDataDir,
                                "dnode" TD_DIRSEP "config" TD_DIRSEP "master.bin"));
  bool hasMaster = taosCheckExistFile(path);
  TAOS_CHECK_RETURN(dmGetEncryptKey());
  if (hasMaster && !tsUseTaoskEncryption) {
    return TSDB_CODE_DNODE_INVALID_ENCRYPT_CONFIG;
  }
  return 0;
}

static int32_t rewriteIdentity(SRewriteCheck *check) {
  char path[PATH_MAX];
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), tsDataDir,
                                "dnode" TD_DIRSEP "dnode.json"));
  tstrncpy(check->dnodeFile, path, sizeof(check->dnodeFile));
  cJSON *json = NULL;
  TAOS_CHECK_RETURN(rewriteReadJson(path, &json));
  int64_t id = 0, dropped = 0;
  int32_t code = rewriteInt(json, "dnodeId", &id);
  if (code == 0) code = rewriteInt(json, "clusterId", &check->clusterId);
  // A file without `dropped` describes a node that was never dropped.
  if (code == 0 && cJSON_GetObjectItemCaseSensitive(json, "dropped") != NULL) {
    code = rewriteInt(json, "dropped", &dropped);
  }
  if (code == 0 && (id <= 0 || id > INT32_MAX ||
                    check->clusterId <= 0 || dropped != 0)) {
    code = TSDB_CODE_INVALID_CFG;
  }
  check->dnodeId = (int32_t)id;
  const cJSON *nodes = cJSON_GetObjectItem(json, "dnodes");
  if (code == 0 && (!cJSON_IsArray(nodes) ||
                    cJSON_GetArraySize(nodes) == 0)) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
  }
  for (int32_t i = 0; code == 0 && i < cJSON_GetArraySize(nodes); ++i) {
    const cJSON *item = cJSON_GetArrayItem(nodes, i);
    SRewriteNode node = {0};
    int64_t isMnode = 0;
    code = rewriteInt(item, "id", &id);
    if (code == 0) code = rewriteInt(item, "isMnode", &isMnode);
    if (code == 0) code = rewriteReadEp(item, "fqdn", "port", &node.map.source);
    if (code == 0 && (id <= 0 || id > INT32_MAX ||
                      isMnode < 0 || isMnode > 1 ||
                      rewriteNode(check, (int32_t)id) != NULL)) {
      code = TSDB_CODE_INVALID_CFG;
    }
    if (code != 0) break;
    node.id = (int32_t)id;
    node.isMnode = (int8_t)isMnode;
    node.map.target = node.map.source;
    if (taosArrayPush(check->nodes, &node) == NULL) code = terrno;
  }
  if (code == 0 && rewriteNode(check, check->dnodeId) == NULL) {
    code = TSDB_CODE_INVALID_CFG;
  }
  cJSON_Delete(json);
  return code;
}

static int32_t rewriteNodeCompare(const void *a, const void *b) {
  const SRewriteNode *left = a, *right = b;
  return (left->id > right->id) - (left->id < right->id);
}

// Applies the mappings to the local dnode list and records what that changes in
// dnode.json. Both stages call it: the precheck with the mappings from --map,
// the apply with the ones rebuilt from the precheck files, which makes it the
// single place the "mapping" report field and those changes are produced.
static int32_t rewriteMappings(SRewriteCheck *check) {
  for (size_t i = 0; i < taosArrayGetSize(check->option->pMappings); ++i) {
    const SDmEndpointMapping *map =
        taosArrayGet(check->option->pMappings, i);
    bool found = false;
    for (size_t j = 0; j < taosArrayGetSize(check->nodes); ++j) {
      SRewriteNode *node = taosArrayGet(check->nodes, j);
      if (rewriteSameEp(&map->source, &node->map.source)) {
        node->map.target = map->target;
        found = true;
        break;
      }
    }
    TAOS_CHECK_RETURN(rewriteError(check, "--map", "source endpoint exists",
        found ? 0 : TSDB_CODE_INVALID_PARA));
  }
  taosArraySort(check->nodes, rewriteNodeCompare);
  cJSON *array = cJSON_AddArrayToObject(check->report, "mapping");
  if (array == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  for (size_t i = 0; i < taosArrayGetSize(check->nodes); ++i) {
    const SRewriteNode *node = taosArrayGet(check->nodes, i);
    for (size_t j = 0; j < i; ++j) {
      const SRewriteNode *other = taosArrayGet(check->nodes, j);
      int32_t code =
          rewriteSameEp(&node->map.target, &other->map.target) ||
          rewriteSameEp(&node->map.source, &other->map.source)
              ? TSDB_CODE_INVALID_CFG : 0;
      TAOS_CHECK_RETURN(rewriteError(check, "--map",
                                     "unique final endpoints", code));
    }
    cJSON *item = cJSON_CreateObject();
    if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;
    if (!cJSON_AddItemToArray(array, item)) {
      cJSON_Delete(item);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    TAOS_CHECK_RETURN(tjsonAddDoubleToObject(item, "dnodeId", node->id));
    TAOS_CHECK_RETURN(rewriteAttach(item, "source",
                                    rewriteEpJson(&node->map.source)));
    TAOS_CHECK_RETURN(rewriteAttach(item, "target",
                                    rewriteEpJson(&node->map.target)));
    TAOS_CHECK_RETURN(rewriteCheckEp(check, check->dnodeFile,
        rewriteDnodes.field, node->id, &node->map.source));
  }
  return 0;
}

static int32_t rewriteConfigValues(SRewriteCheck *check, const char *file,
                                    cJSON *configs) {
  return rewriteWalkConfigs(check, file, configs, rewriteCheckPolicy, NULL);
}

static int32_t rewriteConfigs(SRewriteCheck *check) {
  char path[PATH_MAX];
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), tsDataDir,
                                "dnode" TD_DIRSEP "config" TD_DIRSEP "local.json"));
  cJSON *local = NULL;
  int32_t localCode = rewriteReadJson(path, &local);
  cJSON *configs = cJSON_GetObjectItem(local, "configs");
  if (localCode == 0 && !cJSON_IsObject(configs)) {
    localCode = TSDB_CODE_INVALID_JSON_FORMAT;
  }
  const cJSON *ipv6 = cJSON_GetObjectItem(configs, "enableIpv6");
  if (localCode == 0 && ipv6 != NULL) {
    if (cJSON_IsBool(ipv6)) tsEnableIpv6 = cJSON_IsTrue(ipv6);
    else localCode = TSDB_CODE_INVALID_CFG;
  }
  cJSON *text = cJSON_CreateObject();
  if (text == NULL) {
    cJSON_Delete(local);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  const char *names[] = {"fqdn", "serverPort", "firstEp", "secondEp"};
  int32_t code = 0;
  for (size_t i = 0; i < tListLen(names) && code == 0; ++i) {
    SConfigItem *item = cfgGetItem(check->cfg, names[i]);
    if (item == NULL) {
      code = TSDB_CODE_CFG_NOT_FOUND;
    } else if (item->stype != CFG_STYPE_DEFAULT) {
      code = item->dtype == CFG_DTYPE_INT32
          ? tjsonAddDoubleToObject(text, names[i], item->i32)
          : tjsonAddStringToObject(text, names[i], item->str);
    }
  }
  tsServerPort = rewriteNode(check, check->dnodeId)->map.source.port;
  if (code == 0) code = rewriteConfigValues(check, check->cfgFile, text);
  cJSON_Delete(text);
  code = rewriteError(check, check->cfgFile, "configuration endpoints", code);
  if (code != 0) {
    cJSON_Delete(local);
    return code;
  }
  code = localCode;
  if (code == 0) {
    code = rewriteConfigValues(check, path, configs);
    const cJSON *provider = cJSON_GetObjectItem(configs, "encryptExtDir");
    SConfigItem *configured = cfgGetItem(check->cfg, "encryptExtDir");
    const char *dir = provider != NULL
        ? cJSON_GetStringValue(provider) : configured->str;
    if (dir == NULL || strlen(dir) >= PATH_MAX) {
      if (code == 0) code = TSDB_CODE_INVALID_CFG;
    } else if (dir[0] != '\0') {
      int32_t providerCode = taosExpandDir(dir, tsEncryptExtDir, PATH_MAX);
#if defined(TD_ENTERPRISE) && defined(LINUX)
      if (providerCode == 0) providerCode = cryptLoadProviders();
#else
      if (providerCode == 0) providerCode = TSDB_CODE_OPS_NOT_SUPPORT;
#endif
      if (code == 0) code = providerCode;
    }
  }
  cJSON_Delete(local);
  TAOS_CHECK_RETURN(rewriteError(check, path,
                                 "persisted configuration endpoints", code));
  for (size_t i = 0; i < taosArrayGetSize(check->option->pMappings); ++i) {
    const SDmEndpointMapping *map =
        taosArrayGet(check->option->pMappings, i);
    // Unchanged endpoints never have to resolve.
    if (rewriteSameEp(&map->source, &map->target)) continue;
    SIpAddr addr = {0};
    code = taosGetIpFromFqdn(tsEnableIpv6, map->target.fqdn, &addr);
    TAOS_CHECK_RETURN(rewriteError(check, map->target.fqdn,
                                   "target name resolves", code));
  }
  return 0;
}

// Precheck policy for a member list: reports the endpoint of every member and
// the member itself, which the apply stage compares across dnodes.
typedef struct {
  cJSON *members;  // report list, NULL when the caller does not need it
} SRewriteMembersParam;

static int32_t rewriteMemberPolicy(SRewriteCheck *check, void *param,
                                    const SRewriteSite *site,
                                    const SRewriteNode *node, const SEp *ep) {
  TAOS_CHECK_RETURN(rewriteCheckPolicy(check, NULL, site, node, ep));
  SRewriteMembersParam *members = param;
  if (members->members == NULL) return 0;
  cJSON *member = cJSON_CreateObject();
  if (member == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  if (!cJSON_AddItemToArray(members->members, member)) {
    cJSON_Delete(member);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  TAOS_CHECK_RETURN(tjsonAddDoubleToObject(member, "dnodeId", site->dnodeId));
  return tjsonAddStringToObject(member, "role", site->role);
}

static int32_t rewriteMembers(SRewriteCheck *check, const char *file,
                               cJSON *array, cJSON *members, bool syncFields) {
  SRewriteMembersParam param = {.members = members};
  return rewriteWalkArray(check, file, array,
      syncFields ? &rewriteMembersShape : &rewriteReplicas,
      rewriteMemberPolicy, &param);
}

static int32_t rewriteCompareSync(const cJSON *config, const cJSON *sync) {
  const cJSON *configured = cJSON_GetObjectItem(config, "syncCfg.nodeInfo");
  const cJSON *persisted = cJSON_GetObjectItem(sync, "nodeInfo");
  if (!cJSON_IsArray(configured) || !cJSON_IsArray(persisted) ||
      cJSON_GetArraySize(configured) != cJSON_GetArraySize(persisted)) {
    return TSDB_CODE_INVALID_CFG;
  }
  for (int32_t i = 0; i < cJSON_GetArraySize(configured); ++i) {
    const cJSON *left = cJSON_GetArrayItem(configured, i);
    int64_t leftId = 0;
    TAOS_CHECK_RETURN(rewriteInt(left, "nodeId", &leftId));
    const char *leftRole = cJSON_GetStringValue(
        cJSON_GetObjectItem(left, "isReplica"));
    bool found = false;
    for (int32_t j = 0; j < cJSON_GetArraySize(persisted); ++j) {
      const cJSON *right = cJSON_GetArrayItem(persisted, j);
      int64_t rightId = 0;
      TAOS_CHECK_RETURN(rewriteInt(right, "nodeId", &rightId));
      if (leftId != rightId) continue;
      const char *rightRole = cJSON_GetStringValue(
          cJSON_GetObjectItem(right, "isReplica"));
      if (strcmp(leftRole != NULL ? leftRole : "true",
                  rightRole != NULL ? rightRole : "true") != 0) {
        return TSDB_CODE_INVALID_CFG;
      }
      found = true;
      break;
    }
    if (!found) return TSDB_CODE_INVALID_CFG;
  }
  return 0;
}

static int32_t rewriteReplayRecord(int32_t vgId, int16_t type,
                                    const void *body, int32_t len, bool *safe) {
  *safe = false;
  if (type == TDMT_SYNC_LEADER_TRANSFER) {
    return syncInspectLeaderTransfer(body, len, vgId, safe);
  }
  if (vgId == 1 && type == TDMT_MND_APPLY_MSG) {
    return mndInspectTxnSeq(body, len, safe);
  }
  return 0;
}

static int32_t rewriteWal(SRewriteCheck *check, const char *base,
                           int32_t vgId, int64_t index, int64_t term,
                           const cJSON *config, cJSON *group) {
  char path[WAL_PATH_LEN];
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), base, "wal"));
  char **dirs = NULL;
  int32_t count = vgId == 1 ? 0 : (int32_t)taosArrayGetSize(check->disks);
  dirs = taosMemoryCalloc(count + 1, sizeof(char *));
  if (dirs == NULL) return terrno;
  int32_t code = 0;
  for (int32_t i = 0; i < count && code == 0; ++i) {
    dirs[i] = taosMemoryMalloc(WAL_PATH_LEN);
    if (dirs[i] == NULL) {
      code = terrno;
      break;
    }
    const SDiskCfg *disk = taosArrayGet(check->disks, i);
    int32_t n = snprintf(dirs[i], WAL_PATH_LEN, "%s%svnode%svnode%d%swal",
                         disk->dir, TD_DIRSEP, TD_DIRSEP, vgId, TD_DIRSEP);
    if (n < 0 || n >= WAL_PATH_LEN) code = TSDB_CODE_OUT_OF_BUFFER;
  }
  SWalInspectOption opt = {.path = path, .diskPaths = (const char **)dirs,
      .numOfDisks = count, .snapshotIndex = index, .snapshotTerm = term,
      .checkReplay = rewriteReplayRecord,
      .cfg.vgId = vgId};
  if (code == 0 && config != NULL) {
    const cJSON *hashChange = cJSON_GetObjectItem(config, "hashChange");
    if (hashChange != NULL) {
      int64_t value = 0;
      code = rewriteInt(config, "hashChange", &value);
      if (code == 0 && value != 0 && value != 1) code = TSDB_CODE_INVALID_CFG;
      opt.allowAlterConfirm = code == 0 && value == 0;
    }
    const char *algorithm = cJSON_GetStringValue(
        cJSON_GetObjectItem(config, "wal.encryptAlgrName"));
    int64_t alg = 0;
    const cJSON *legacy = cJSON_GetObjectItem(config, "wal.encryptAlgorithm");
    if (code == 0 && legacy != NULL) {
      code = rewriteInt(config, "wal.encryptAlgorithm", &alg);
    }
    if (algorithm != NULL) {
      if (strlen(algorithm) >= sizeof(opt.cfg.encryptData.encryptAlgrName)) {
        code = TSDB_CODE_INVALID_CFG;
      } else {
        tstrncpy(opt.cfg.encryptData.encryptAlgrName, algorithm,
                  sizeof(opt.cfg.encryptData.encryptAlgrName));
      }
    }
    if (alg == DND_CA_SM4 && opt.cfg.encryptData.encryptAlgrName[0] == '\0') {
      tstrncpy(opt.cfg.encryptData.encryptAlgrName, TSDB_ENCRYPT_ALGR_SM4_NAME,
                sizeof(opt.cfg.encryptData.encryptAlgrName));
    }
    if (opt.cfg.encryptData.encryptAlgrName[0] != '\0') {
      const char *key = tsDataKey[0] != '\0' ? tsDataKey : tsEncryptKey;
      if (key[0] == '\0') code = TSDB_CODE_DNODE_INVALID_ENCRYPT_CONFIG;
      tstrncpy(opt.cfg.encryptData.encryptKey, key,
                sizeof(opt.cfg.encryptData.encryptKey));
    }
  }
  SWalInspectResult result = {.versions = {.snapshotVer = INT64_MIN,
      .commitVer = INT64_MIN, .lastVer = INT64_MIN}};
  if (code == 0) code = walInspect(&opt, &result);
  const SWalVer versions = result.versions;
  int32_t walCode = code;
  if (versions.snapshotVer != INT64_MIN &&
      versions.commitVer != INT64_MIN && versions.lastVer != INT64_MIN) {
    cJSON *wal = cJSON_AddObjectToObject(group, "wal");
    if (wal == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      code = rewriteAddInt(wal, "snapshotVer", versions.snapshotVer);
      if (code == 0) code = rewriteAddInt(wal, "commitVer", versions.commitVer);
      if (code == 0) code = rewriteAddInt(wal, "lastVer", versions.lastVer);
    }
  }
  // safeLowerBound only means "every record above it was verified" when the
  // inspection actually ran to the end; on a failed inspection the value left in
  // result must not be reported.
  if (code == 0 && walCode == 0) {
    code = rewriteAddInt(group, "safeLowerBound", result.safeLowerBound);
  }
  for (int32_t i = 0; i < count; ++i) taosMemoryFree(dirs[i]);
  taosMemoryFree(dirs);
  if (walCode != 0 && versions.snapshotVer != INT64_MIN &&
      versions.commitVer != INT64_MIN && versions.lastVer != INT64_MIN &&
      (versions.snapshotVer > index || versions.snapshotVer < -1 ||
       index > versions.commitVer || versions.commitVer > versions.lastVer)) {
    char detail[256];
    snprintf(detail, sizeof(detail),
        "snapshot/WAL boundaries differ: S=%" PRId64 ", C=%" PRId64
        ", L=%" PRId64 ", snapshotVer=%" PRId64
        "; require snapshotVer<=S<=C<=L",
        index, versions.commitVer, versions.lastVer, versions.snapshotVer);
    return rewriteErrorDetail(check, path, "snapshot/WAL boundaries",
                                walCode, detail);
  }
  return rewriteError(check, path, "WAL metadata and files",
                       code != 0 ? code : walCode);
}

static int32_t rewriteGroup(SRewriteCheck *check, const char *base,
                             int32_t vgId, int64_t index, int64_t term,
                             const cJSON *config, cJSON *group) {
  char path[PATH_MAX];
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), base,
                                "sync" TD_DIRSEP "raft_config.json"));
  cJSON *json = NULL;
  int32_t code = rewriteReadJson(path, &json);
  if (code != 0) return rewriteError(check, path, "read Raft configuration", code);
  const cJSON *raft = cJSON_GetObjectItem(json, "RaftCfg");
  cJSON *sync = cJSON_GetObjectItem(raft, "SSyncCfg");
  cJSON *array = cJSON_GetObjectItem(sync, "nodeInfo");
  if (code == 0 && config != NULL) code = rewriteCompareSync(config, sync);
  cJSON *members = cJSON_AddArrayToObject(group, "members");
  if (members == NULL) code = TSDB_CODE_OUT_OF_MEMORY;
  if (code == 0) code = rewriteMembers(check, path, array, members, true);
  if (code == 0 && vgId == 1) {
    int32_t expected = 0;
    for (size_t i = 0; i < taosArrayGetSize(check->nodes); ++i) {
      const SRewriteNode *node = taosArrayGet(check->nodes, i);
      expected += node->isMnode;
    }
    if (expected != cJSON_GetArraySize(array)) code = TSDB_CODE_INVALID_CFG;
    for (int32_t i = 0; code == 0 && i < cJSON_GetArraySize(array); ++i) {
      int64_t id = 0;
      code = rewriteInt(cJSON_GetArrayItem(array, i), "nodeId", &id);
      if (code == 0) {
        const SRewriteNode *node = rewriteNode(check, (int32_t)id);
        if (node == NULL || !node->isMnode) {
          code = TSDB_CODE_INVALID_CFG;
        }
      }
    }
  }
  cJSON_Delete(json);
  if (code != 0) {
    return rewriteError(check, path, "Raft member configuration", code);
  }
  TAOS_CHECK_RETURN(rewriteAddInt(group, "snapshotIndex", index));
  TAOS_CHECK_RETURN(rewriteAddInt(group, "snapshotTerm", term));
  return rewriteWal(check, base, vgId, index, term, config, group);
}

static int32_t rewriteMnode(SRewriteCheck *check) {
  const SRewriteNode *self = rewriteNode(check, check->dnodeId);
  char base[PATH_MAX], path[PATH_MAX];
  TAOS_CHECK_RETURN(rewritePath(base, sizeof(base), tsDataDir, "mnode"));
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), base, "mnode.json"));
  if (!self->isMnode && !taosCheckExistFile(path)) {
    return rewriteAttach(check->report, "mnode", cJSON_CreateNull());
  }
  cJSON *json = NULL;
  TAOS_CHECK_RETURN(rewriteReadJson(path, &json));
  int64_t deployed = 0, encrypted = 0;
  int32_t code = rewriteInt(json, "deployed", &deployed);
  if (code == 0 && deployed != self->isMnode) code = TSDB_CODE_INVALID_CFG;
  if (code == 0 && !deployed) {
    cJSON_Delete(json);
    return rewriteAttach(check->report, "mnode", cJSON_CreateNull());
  }
  if (code == 0) code = rewriteInt(json, "encrypted", &encrypted);
  check->mnodeEncrypted = encrypted != 0;
  cJSON *replicas = cJSON_GetObjectItem(json, "replicas");
  if (code == 0 && replicas != NULL) {
    code = rewriteMembers(check, path, replicas, NULL, false);
  }
  cJSON_Delete(json);
  if (code != 0) return code;
  cJSON *group = cJSON_AddObjectToObject(check->report, "mnode");
  if (group == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), base, "data" TD_DIRSEP "sdb.data"));
  SArray *nodes = taosArrayInit(1, sizeof(SDnodeEp));
  if (nodes == NULL) return terrno;
  int64_t index = -1, term = -1;
  int32_t transactions = 0;
  code = mndInspectSdb(path, encrypted != 0, nodes, &index, &term, &transactions);
  if (code == 0) {
    code = rewriteError(check, path, "no unfinished transactions",
                        transactions == 0 ? 0 : TSDB_CODE_INVALID_CFG);
  }
  if (code == 0 && taosArrayGetSize(nodes) != taosArrayGetSize(check->nodes)) {
    code = TSDB_CODE_INVALID_CFG;
  }
  for (size_t i = 0; code == 0 && i < taosArrayGetSize(nodes); ++i) {
    const SDnodeEp *node = taosArrayGet(nodes, i);
    code = rewriteCheckEp(check, path, "SDB_DNODE.fqdn/port",
                           node->id, &node->ep);
    for (size_t j = 0; code == 0 && j < i; ++j) {
      if (((SDnodeEp *)taosArrayGet(nodes, j))->id == node->id) {
        code = TSDB_CODE_INVALID_CFG;
      }
    }
  }
  taosArrayDestroy(nodes);
  if (code != 0) return code;
  return rewriteGroup(check, base, 1, index, term, NULL, group);
}

static int32_t rewriteVnode(SRewriteCheck *check, const cJSON *item,
                             cJSON *groups) {
  int64_t vgId = 0, diskId = 0;
  TAOS_CHECK_RETURN(rewriteInt(item, "vgId", &vgId));
  TAOS_CHECK_RETURN(rewriteInt(item, "diskPrimary", &diskId));
  if (vgId <= 1 || vgId > INT32_MAX || diskId < 0 ||
      diskId >= taosArrayGetSize(check->disks)) return TSDB_CODE_INVALID_CFG;
  for (int32_t i = 0; i < cJSON_GetArraySize(groups); ++i) {
    int64_t existing = 0;
    TAOS_CHECK_RETURN(rewriteInt(cJSON_GetArrayItem(groups, i),
                                  "vgId", &existing));
    if (existing == vgId) return TSDB_CODE_INVALID_CFG;
  }
  const SDiskCfg *disk = taosArrayGet(check->disks, (size_t)diskId);
  char base[PATH_MAX], path[PATH_MAX];
  int32_t n = snprintf(base, sizeof(base), "%s%svnode%svnode%" PRId64,
                       disk->dir, TD_DIRSEP, TD_DIRSEP, vgId);
  if (n < 0 || n >= sizeof(base)) return TSDB_CODE_OUT_OF_BUFFER;
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), base, "vnode.json"));
  cJSON *json = NULL;
  int32_t code = rewriteReadJson(path, &json);
  if (code != 0) return rewriteError(check, path, "read vnode configuration", code);
  cJSON *group = cJSON_CreateObject();
  if (group == NULL || !cJSON_AddItemToArray(groups, group)) {
    cJSON_Delete(group);
    cJSON_Delete(json);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  code = tjsonAddDoubleToObject(group, "vgId", vgId);
  const cJSON *config = cJSON_GetObjectItem(json, "config");
  const cJSON *state = cJSON_GetObjectItem(json, "state");
  int64_t index = -1, term = -1, configuredId = 0;
  if (code == 0) code = rewriteInt(state, "commit version", &index);
  if (code == 0) code = rewriteInt(state, "commit term", &term);
  if (code == 0) code = rewriteInt(config, "vgId", &configuredId);
  if (code == 0 && configuredId != vgId) code = TSDB_CODE_INVALID_CFG;
  if (code == 0) {
    code = rewriteMembers(check, path,
        cJSON_GetObjectItem(config, "syncCfg.nodeInfo"), NULL, true);
  }
  if (code != 0) {
    code = rewriteError(check, path, "vnode state and endpoints", code);
  } else {
    code = rewriteGroup(check, base, (int32_t)vgId, index, term, config, group);
  }
  cJSON_Delete(json);
  return code;
}

static int32_t rewriteVnodeInventory(SRewriteCheck *check,
                                      const cJSON *vnodes) {
  for (size_t i = 0; i < taosArrayGetSize(check->disks); ++i) {
    const SDiskCfg *disk = taosArrayGet(check->disks, i);
    char path[PATH_MAX];
    TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), disk->dir, "vnode"));
    if (!taosCheckExistFile(path)) continue;
    TdDirPtr dir = taosOpenDir(path);
    if (dir == NULL) return terrno;
    int32_t code = 0;
    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
      const char *name = taosGetDirEntryName(entry);
      if (strncmp(name, "vnode", 5) != 0) continue;
      if (name[5] < '0' || name[5] > '9') continue;
      bool numeric = true;
      for (const char *p = name + 5; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') numeric = false;
      }
      if (!numeric) continue;
      int64_t vgId = 0;
      code = taosStr2int64(name + 5, &vgId);
      if (code != 0) break;
      bool found = false;
      for (int32_t j = 0; j < cJSON_GetArraySize(vnodes); ++j) {
        int64_t expected = 0;
        code = rewriteInt(cJSON_GetArrayItem(vnodes, j), "vgId", &expected);
        if (code != 0) break;
        if (expected == vgId) {
          found = true;
          break;
        }
      }
      if (code != 0 || !found) {
        code = TSDB_CODE_INVALID_CFG;
        break;
      }
    }
    int32_t closeCode = taosCloseDir(&dir);
    if (code == 0) code = closeCode;
    if (code != 0) return code;
  }
  return 0;
}

static int32_t rewriteVnodes(SRewriteCheck *check) {
  cJSON *groups = cJSON_AddArrayToObject(check->report, "vnodes");
  if (groups == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  char path[PATH_MAX];
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), tsDataDir,
                                "vnode" TD_DIRSEP "vnodes.json"));
  cJSON *json = NULL;
  if (!taosCheckExistFile(path)) {
    return rewriteVnodeInventory(check, NULL);
  }
  TAOS_CHECK_RETURN(rewriteReadJson(path, &json));
  const cJSON *vnodes = cJSON_GetObjectItem(json, "vnodes");
  int32_t code = cJSON_IsArray(vnodes) ? 0 : TSDB_CODE_INVALID_JSON_FORMAT;
  for (int32_t i = 0; code == 0 && i < cJSON_GetArraySize(vnodes); ++i) {
    const cJSON *item = cJSON_GetArrayItem(vnodes, i);
    int32_t ret = rewriteVnode(check, item, groups);
    code = rewriteError(check, path, "vnode configuration and WAL", ret);
  }
  if (code == 0) code = rewriteVnodeInventory(check, vnodes);
  cJSON_Delete(json);
  return code;
}

// A role that no snode group uses is written without an `eps` field at all,
// with numOfEps 0. The checks and the rewrite must judge that shape the same
// way, so they share this parser instead of writing the rule twice.
static int32_t rewriteSnodeSet(const cJSON *set, int64_t *id, int64_t *count,
                                const cJSON **eps) {
  *eps = cJSON_GetObjectItemCaseSensitive(set, "eps");
  TAOS_CHECK_RETURN(rewriteInt(set, "nodeId", id));
  TAOS_CHECK_RETURN(rewriteInt(set, "numOfEps", count));
  int32_t entries = cJSON_IsArray(*eps) ? cJSON_GetArraySize(*eps) : 0;
  if ((*eps != NULL && !cJSON_IsArray(*eps)) || *count != entries || *id < 0 ||
      *id > INT32_MAX || (*count > 0 && *id == 0)) {
    return TSDB_CODE_INVALID_CFG;
  }
  return 0;
}

// Every role of a snode file holds a set of endpoints, and each of them belongs
// to the dnode the role names.
static int32_t rewriteWalkSnode(SRewriteCheck *check, const char *path,
                                 cJSON *json, SRewriteSitePolicy policy,
                                 void *param) {
  const char *keys[] = {"leader0", "leader1", "replica"};
  for (size_t i = 0; i < tListLen(keys); ++i) {
    cJSON *set = cJSON_GetObjectItemCaseSensitive(json, keys[i]);
    if (set == NULL) continue;
    const cJSON *eps = NULL;
    int64_t      id = 0, count = 0;
    TAOS_CHECK_RETURN(rewriteSnodeSet(set, &id, &count, &eps));
    for (int32_t j = 0; j < count; ++j) {
      cJSON *item = cJSON_GetArrayItem(eps, j);
      SRewriteSite site = {.file = path, .object = item, .hostKey = "fqdn",
                           .portKey = "port", .field = keys[i],
                           .dnodeId = (int32_t)id};
      SEp ep = {0};
      TAOS_CHECK_RETURN(rewriteReadEp(item, "fqdn", "port", &ep));
      const SRewriteNode *node = NULL;
      (void)rewriteAcceptEp(check, &site, &ep, &node);
      TAOS_CHECK_RETURN(policy(check, param, &site, node, &ep));
    }
  }
  return 0;
}

static int32_t rewriteSnode(SRewriteCheck *check) {
  char path[PATH_MAX];
  int32_t n = snprintf(path, sizeof(path), "%s%ssnode%ssnode%d%ssnode.json",
      tsDataDir, TD_DIRSEP, TD_DIRSEP, check->dnodeId, TD_DIRSEP);
  if (n < 0 || n >= sizeof(path)) return TSDB_CODE_OUT_OF_BUFFER;
  char base[PATH_MAX];
  tstrncpy(base, path, sizeof(base));
  char *sep = strrchr(base, TD_DIRSEP[0]);
  if (sep == NULL) return TSDB_CODE_INVALID_CFG;
  *sep = '\0';
  if (!taosCheckExistFile(base)) return 0;
  cJSON *json = NULL;
  TAOS_CHECK_RETURN(rewriteReadJson(path, &json));
  int64_t snodeId = 0;
  int32_t code = rewriteInt(json, "snodeId", &snodeId);
  if (code == 0 && snodeId != check->dnodeId) code = TSDB_CODE_INVALID_CFG;
  if (code == 0) code = rewriteWalkSnode(check, path, json, rewriteCheckPolicy, NULL);
  cJSON_Delete(json);
  return code;
}

static const char *rewriteChangeFile(const cJSON *change) {
  return cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(change, "file"));
}

static void rewritePrintChange(const cJSON *change) {
  const char  *field = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(change, "field"));
  const cJSON *source = cJSON_GetObjectItemCaseSensitive(change, "source");
  const cJSON *target = cJSON_GetObjectItemCaseSensitive(change, "target");
  printf("%s: %s:%d -> %s:%d\n", field,
         cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(source, "fqdn")),
         cJSON_GetObjectItemCaseSensitive(source, "port")->valueint,
         cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(target, "fqdn")),
         cJSON_GetObjectItemCaseSensitive(target, "port")->valueint);
}

static int32_t rewriteFileNameCompare(const void *left, const void *right) {
  return strcmp(*(const char *const *)left, *(const char *const *)right);
}

static bool rewriteListedFile(const SArray *files, const char *file) {
  for (size_t i = 0; i < taosArrayGetSize(files); ++i) {
    if (strcmp(*(const char **)taosArrayGet(files, i), file) == 0) {
      return true;
    }
  }
  return false;
}

// Prints one block per file: the file path, the changes made in it, then a blank
// line. Files are ordered by path so repeated runs print the same layout.
// Returns the number of files listed.
static int32_t rewritePrintChanges(const cJSON *changes, const char *title) {
  int32_t count = cJSON_GetArraySize(changes);
  printf("\n%s\n", title);
  if (count == 0) {
    return 0;
  }
  SArray *files = taosArrayInit((size_t)count, sizeof(char *));
  if (files == NULL) {
    for (int32_t i = 0; i < count; ++i) {
      rewritePrintChange(cJSON_GetArrayItem(changes, i));
    }
    return 0;
  }
  for (int32_t i = 0; i < count; ++i) {
    const char *file = rewriteChangeFile(cJSON_GetArrayItem(changes, i));
    if (file != NULL && !rewriteListedFile(files, file)) {
      if (taosArrayPush(files, &file) == NULL) {
        taosArrayDestroy(files);
        return 0;
      }
    }
  }
  taosArraySort(files, rewriteFileNameCompare);
  for (size_t i = 0; i < taosArrayGetSize(files); ++i) {
    const char *file = *(const char **)taosArrayGet(files, i);
    printf("\n%s\n", file);
    for (int32_t j = 0; j < count; ++j) {
      const cJSON *change = cJSON_GetArrayItem(changes, j);
      const char  *other = rewriteChangeFile(change);
      if (other != NULL && strcmp(other, file) == 0) {
        rewritePrintChange(change);
      }
    }
  }
  int32_t fileCount = (int32_t)taosArrayGetSize(files);
  taosArrayDestroy(files);
  return fileCount;
}

static int32_t rewritePublish(SRewriteCheck *check) {
  TAOS_CHECK_RETURN(tjsonAddStringToObject(check->report, "status",
      check->firstError == 0 ? "passed" : "failed"));
  char time[64];
  TAOS_CHECK_RETURN(taosFormatUtcTime(time, sizeof(time), taosGetTimestampMs(),
                                     TSDB_TIME_PRECISION_MILLI));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(check->report, "generatedAt", time));
  char *content = cJSON_Print(check->report);
  if (content == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  char path[PATH_MAX];
  int32_t n = snprintf(path, sizeof(path), "%s%sprecheck.dnode%d.json",
      check->option->precheckDir, TD_DIRSEP, check->dnodeId);
  int32_t code = n < 0 || n >= sizeof(path) ? TSDB_CODE_OUT_OF_BUFFER : 0;
  char temporary[PATH_MAX] = {0};
  if (code == 0) {
    n = snprintf(temporary, sizeof(temporary), "%s.tmp.%d",
                  path, taosGetPId());
    if (n < 0 || n >= sizeof(temporary)) code = TSDB_CODE_OUT_OF_BUFFER;
  }
  TdFilePtr file = NULL;
  bool created = false;
  if (code == 0) {
    file = taosOpenFile(temporary, TD_FILE_WRITE | TD_FILE_CREATE |
        TD_FILE_EXCL | TD_FILE_CLOEXEC);
    if (file == NULL) code = terrno;
    else created = true;
  }
  if (code == 0 && taosWriteFile(file, content, strlen(content)) != strlen(content)) {
    code = terrno != 0 ? terrno : TAOS_SYSTEM_ERROR(EIO);
  }
  if (code == 0) code = taosFsyncFile(file);
  if (file != NULL) {
    int32_t closeCode = taosCloseFile(&file);
    if (code == 0) code = closeCode;
  }
  if (code == 0) code = taosRenameFile(temporary, path);
  if (code != 0 && created) {
    int32_t removeCode = taosRemoveFile(temporary);
    if (removeCode != 0) {
      printf("failed to remove report temporary file:%s: %s\n",
              temporary, tstrerror(removeCode));
    }
  }
  taosMemoryFree(content);
  if (code == 0 && check->firstError == 0) {
    int32_t files = rewritePrintChanges(check->changes, "ENDPOINTS REWRITE PREVIEW:");
    printf("\nendpoint rewrite preview: %d endpoints in %d files\n",
           cJSON_GetArraySize(check->changes), files);
  }
  if (code == 0) printf("\nPRECHECK RESULT: %s\n", path);
  return code;
}

static int32_t rewriteCheckLeftovers(const char *cfgDir, const SArray *disks, const char *stage);

int32_t dmRewriteEndpointsPrecheck(const char *cfgDir,
                                  const SDmRewriteEndpointsOption *option) {
  SRewriteCheck check = {.option = option, .stage = "precheck"};
  int32_t code = rewriteLoadCfg(&check, cfgDir);
  if (code != 0) goto _exit;
  code = rewriteCheckLeftovers(cfgDir, check.disks, "precheck failed");
  if (code != 0) goto _exit;
  char path[PATH_MAX];
  code = rewritePath(path, sizeof(path), tsDataDir, ".running");
  if (code != 0) goto _exit;
  // Do not create or truncate the daemon's existing lock file.
  check.lock = taosOpenFile(path, TD_FILE_WRITE | TD_FILE_CLOEXEC);
  int32_t lockCode = check.lock == NULL ? terrno : taosLockFile(check.lock);
  code = rewriteLoadKeys();
  if (code != 0) goto _exit;
  check.report = cJSON_CreateObject();
  check.nodes = taosArrayInit(1, sizeof(SRewriteNode));
  if (check.report == NULL || check.nodes == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  check.errors = cJSON_AddArrayToObject(check.report, "errors");
  check.changes = cJSON_CreateArray();
  if (check.errors == NULL || check.changes == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  code = rewriteIdentity(&check);
  if (code != 0) goto _exit;
  code = tjsonAddStringToObject(check.report, "version", td_version);
  if (code == 0) code = rewriteAddInt(check.report, "clusterId", check.clusterId);
  if (code == 0) {
    code = tjsonAddDoubleToObject(check.report, "dnodeId", check.dnodeId);
  }
  if (code != 0) goto _exit;
  code = rewriteError(&check, path, "instance stopped and locked", lockCode);
  if (code != 0) goto _exit;
  if (lockCode == 0) {
    code = rewriteMappings(&check);
    if (code == 0) code = rewriteConfigs(&check);
    if (code == 0) {
      int32_t ret = rewriteMnode(&check);
      code = rewriteError(&check, "mnode", "snapshot and WAL", ret);
    }
    if (code == 0) {
      int32_t ret = rewriteVnodes(&check);
      code = rewriteError(&check, "vnode", "local vnode inventory", ret);
    }
    if (code == 0) {
      int32_t ret = rewriteSnode(&check);
      code = rewriteError(&check, "snode", "configuration endpoints", ret);
    }
  }
  if (code == 0) code = rewritePublish(&check);
  if (code == 0) code = check.firstError;

_exit:
  if (check.lock != NULL) {
    int32_t closeCode = taosCloseFile(&check.lock);
    if (code == 0) code = closeCode;
  }
  if (code != 0) printf("endpoint rewrite precheck failed: %s\n", tstrerror(code));
  cJSON_Delete(check.changes);
  cJSON_Delete(check.report);
  taosArrayDestroy(check.nodes);
  taosArrayDestroy(check.disks);
  cfgCleanup(check.cfg);
  return code;
}

/* ------------------------------------------------------------------------- */
/* Apply                                                                      */
/* ------------------------------------------------------------------------- */

#define REWRITE_BACKUP_SUFFIX  ".before-rewrite-endpoints"
#define REWRITE_GROUP_LOOKBACK 20

typedef struct {
  int32_t dnodeId;
  SEp     source;
  SEp     target;
} SRewriteMapping;

typedef struct {
  int32_t dnodeId;
  cJSON  *json;
} SRewriteReport;

typedef struct {
  SArray *reports;  // SRewriteReport
  SArray *mapping;  // SRewriteMapping, ordered by dnode ID
  char    version[TSDB_VERSION_LEN];
  int64_t clusterId;
} SRewriteReportSet;

typedef struct {
  int32_t vgId;  // 0 for the mnode group
  int32_t dnodeId;
  int64_t index;
  int64_t term;
  int64_t safeLowerBound;
  int64_t snapshotVer;
  int64_t commitVer;
  int64_t lastVer;
  char   *members;  // canonical "id:role" list, ordered by dnode ID
} SRewriteGroupInfo;

typedef struct {
  int32_t id;
  char    role[8];
} SRewriteMember;

typedef enum {
  REWRITE_FILE_CFG = 1,
  REWRITE_FILE_LOCAL,
  REWRITE_FILE_DNODE,
  REWRITE_FILE_MNODE,
  REWRITE_FILE_RAFT,
  REWRITE_FILE_VNODE,
  REWRITE_FILE_SNODE,
  REWRITE_FILE_SDB,
} ERewriteFileKind;

typedef struct {
  char    path[PATH_MAX];
  int32_t kind;
  char   *content;  // rewritten content, absent for the snapshot
  int32_t len;
} SRewriteTarget;

typedef struct {
  char file[PATH_MAX];
  char field[64];
  SEp  source;
  SEp  target;
} SRewriteChange;

typedef struct {
  const char *value;
  int32_t     len;
  bool        quoted;
  bool        changed;
  char        text[TSDB_EP_LEN + 8];
  int32_t     textLen;
} SRewriteCfgValue;

static void rewriteGroupLabel(int32_t vgId, char *label, int32_t size) {
  if (vgId == 0) {
    (void)snprintf(label, size, "mnode");
  } else {
    (void)snprintf(label, size, "vnode%d", vgId);
  }
}

static int32_t rewriteAddChange(SArray *changes, const char *file,
                                 const char *field, const SEp *source,
                                 const SEp *target) {
  SRewriteChange change = {0};
  tstrncpy(change.file, file, sizeof(change.file));
  tstrncpy(change.field, field, sizeof(change.field));
  change.source = *source;
  change.target = *target;
  if (taosArrayPush(changes, &change) == NULL) return terrno;
  return 0;
}

static int32_t rewriteCompareChange(const SRewriteChange *left,
                                     const SRewriteChange *right) {
  int32_t cmp = strcmp(left->file, right->file);
  if (cmp != 0) return cmp;
  cmp = strcmp(left->field, right->field);
  if (cmp != 0) return cmp;
  cmp = strcmp(left->source.fqdn, right->source.fqdn);
  if (cmp != 0) return cmp;
  if (left->source.port != right->source.port) {
    return left->source.port > right->source.port ? 1 : -1;
  }
  cmp = strcmp(left->target.fqdn, right->target.fqdn);
  if (cmp != 0) return cmp;
  if (left->target.port != right->target.port) {
    return left->target.port > right->target.port ? 1 : -1;
  }
  return 0;
}

static void rewriteSortChanges(SArray *changes) {
  size_t count = taosArrayGetSize(changes);
  for (size_t i = 1; i < count; ++i) {
    SRewriteChange key = *(SRewriteChange *)taosArrayGet(changes, i);
    size_t         j = i;
    while (j > 0) {
      SRewriteChange *previous = taosArrayGet(changes, j - 1);
      if (rewriteCompareChange(previous, &key) <= 0) break;
      *(SRewriteChange *)taosArrayGet(changes, j) = *previous;
      --j;
    }
    *(SRewriteChange *)taosArrayGet(changes, j) = key;
  }
}

static int32_t rewriteGroupMembers(const cJSON *group, char **members) {
  const cJSON *array = cJSON_GetObjectItemCaseSensitive(group, "members");
  *members = NULL;
  if (!cJSON_IsArray(array) || cJSON_GetArraySize(array) == 0) {
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }
  int32_t count = cJSON_GetArraySize(array);
  // A Raft group holds at most one member per replica slot, so a longer list
  // came from a forged or damaged precheck file.
  if (count > TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA) {
    return TSDB_CODE_INVALID_CFG;
  }
  SRewriteMember *list = taosMemoryCalloc(count, sizeof(SRewriteMember));
  if (list == NULL) return terrno;
  int32_t code = 0;
  for (int32_t i = 0; i < count && code == 0; ++i) {
    const cJSON *item = cJSON_GetArrayItem(array, i);
    int64_t      id = 0;
    code = rewriteInt(item, "dnodeId", &id);
    const char *role = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(item, "role"));
    if (code == 0 && (id <= 0 || id > INT32_MAX)) code = TSDB_CODE_INVALID_CFG;
    if (code == 0 && (role == NULL || (strcmp(role, "voter") != 0 && strcmp(role, "learner") != 0))) {
      code = TSDB_CODE_INVALID_CFG;
    }
    if (code == 0) {
      list[i].id = (int32_t)id;
      tstrncpy(list[i].role, role, sizeof(list[i].role));
    }
  }
  for (int32_t i = 1; code == 0 && i < count; ++i) {
    SRewriteMember key = list[i];
    int32_t        j = i;
    while (j > 0 && list[j - 1].id > key.id) {
      list[j] = list[j - 1];
      --j;
    }
    list[j] = key;
  }
  for (int32_t i = 1; code == 0 && i < count; ++i) {
    if (list[i].id == list[i - 1].id) code = TSDB_CODE_INVALID_CFG;
  }
  // Each member renders as "," + up to 10 digits + ":" + the longest role name.
  size_t size = (size_t)count * 32 + 1;
  char  *text = NULL;
  if (code == 0) {
    text = taosMemoryMalloc(size);
    if (text == NULL) code = terrno;
  }
  if (code == 0) {
    int32_t used = 0;
    text[0] = '\0';
    for (int32_t i = 0; i < count; ++i) {
      used += snprintf(text + used, size - (size_t)used, "%s%d:%s",
                       i > 0 ? "," : "", list[i].id, list[i].role);
    }
  }
  taosMemoryFree(list);
  if (code != 0) {
    taosMemoryFree(text);
    return code;
  }
  *members = text;
  return 0;
}

static void rewriteFreeGroups(SArray *groups) {
  if (groups == NULL) return;
  for (size_t i = 0; i < taosArrayGetSize(groups); ++i) {
    taosMemoryFree(((SRewriteGroupInfo *)taosArrayGet(groups, i))->members);
  }
  taosArrayDestroy(groups);
}

static int32_t rewriteReadGroup(const cJSON *group, int32_t vgId, int32_t dnodeId, SRewriteGroupInfo *info) {
  if (!cJSON_IsObject(group)) return TSDB_CODE_INVALID_JSON_FORMAT;
  (void)memset(info, 0, sizeof(*info));
  info->vgId = vgId;
  info->dnodeId = dnodeId;
  TAOS_CHECK_RETURN(rewriteInt(group, "snapshotIndex", &info->index));
  TAOS_CHECK_RETURN(rewriteInt(group, "snapshotTerm", &info->term));
  TAOS_CHECK_RETURN(rewriteInt(group, "safeLowerBound", &info->safeLowerBound));
  const cJSON *wal = cJSON_GetObjectItemCaseSensitive(group, "wal");
  if (!cJSON_IsObject(wal)) return TSDB_CODE_INVALID_JSON_FORMAT;
  TAOS_CHECK_RETURN(rewriteInt(wal, "snapshotVer", &info->snapshotVer));
  TAOS_CHECK_RETURN(rewriteInt(wal, "commitVer", &info->commitVer));
  TAOS_CHECK_RETURN(rewriteInt(wal, "lastVer", &info->lastVer));
  return rewriteGroupMembers(group, &info->members);
}

static int32_t rewriteReportGroups(const cJSON *report, int32_t dnodeId, SArray *groups) {
  const cJSON *mnode = cJSON_GetObjectItemCaseSensitive(report, "mnode");
  if (mnode == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;
  int32_t code = 0;
  if (!cJSON_IsNull(mnode)) {
    SRewriteGroupInfo info = {0};
    code = rewriteReadGroup(mnode, 0, dnodeId, &info);
    if (code == 0 && taosArrayPush(groups, &info) == NULL) {
      taosMemoryFree(info.members);
      code = terrno;
    }
  }
  const cJSON *vnodes = cJSON_GetObjectItemCaseSensitive(report, "vnodes");
  if (code == 0 && !cJSON_IsArray(vnodes)) code = TSDB_CODE_INVALID_JSON_FORMAT;
  for (int32_t i = 0; code == 0 && i < cJSON_GetArraySize(vnodes); ++i) {
    const cJSON *item = cJSON_GetArrayItem(vnodes, i);
    int64_t      vgId = 0;
    code = rewriteInt(item, "vgId", &vgId);
    if (code == 0 && (vgId <= 1 || vgId > INT32_MAX)) {
      code = TSDB_CODE_INVALID_CFG;
    }
    if (code != 0) break;
    SRewriteGroupInfo info = {0};
    code = rewriteReadGroup(item, (int32_t)vgId, dnodeId, &info);
    if (code == 0 && taosArrayPush(groups, &info) == NULL) {
      taosMemoryFree(info.members);
      code = terrno;
    }
  }
  return code;
}

static const SRewriteReport *rewriteFindReport(const SRewriteReportSet *set, int32_t dnodeId) {
  for (size_t i = 0; i < taosArrayGetSize(set->reports); ++i) {
    const SRewriteReport *report = taosArrayGet(set->reports, i);
    if (report->dnodeId == dnodeId) return report;
  }
  return NULL;
}

static void rewriteSortMapping(SArray *mapping) {
  size_t count = taosArrayGetSize(mapping);
  for (size_t i = 1; i < count; ++i) {
    SRewriteMapping key = *(SRewriteMapping *)taosArrayGet(mapping, i);
    size_t          j = i;
    while (j > 0) {
      SRewriteMapping *previous = taosArrayGet(mapping, j - 1);
      if (previous->dnodeId <= key.dnodeId) break;
      *(SRewriteMapping *)taosArrayGet(mapping, j) = *previous;
      --j;
    }
    *(SRewriteMapping *)taosArrayGet(mapping, j) = key;
  }
}

static int32_t rewriteReadMapping(const cJSON *report, SArray *mapping) {
  const cJSON *array = cJSON_GetObjectItemCaseSensitive(report, "mapping");
  if (!cJSON_IsArray(array) || cJSON_GetArraySize(array) == 0) {
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }
  for (int32_t i = 0; i < cJSON_GetArraySize(array); ++i) {
    const cJSON *item = cJSON_GetArrayItem(array, i);
    SRewriteMapping map = {0};
    int64_t         id = 0;
    TAOS_CHECK_RETURN(rewriteInt(item, "dnodeId", &id));
    if (id <= 0 || id > INT32_MAX) return TSDB_CODE_INVALID_CFG;
    map.dnodeId = (int32_t)id;
    const cJSON *source = cJSON_GetObjectItemCaseSensitive(item, "source");
    const cJSON *target = cJSON_GetObjectItemCaseSensitive(item, "target");
    TAOS_CHECK_RETURN(rewriteReadEp(source, "fqdn", "port", &map.source));
    TAOS_CHECK_RETURN(rewriteReadEp(target, "fqdn", "port", &map.target));
    if (taosArrayPush(mapping, &map) == NULL) return terrno;
  }
  rewriteSortMapping(mapping);
  for (size_t i = 1; i < taosArrayGetSize(mapping); ++i) {
    const SRewriteMapping *previous = taosArrayGet(mapping, i - 1);
    const SRewriteMapping *current = taosArrayGet(mapping, i);
    if (previous->dnodeId == current->dnodeId) return TSDB_CODE_INVALID_CFG;
  }
  return 0;
}

static bool rewriteSameMapping(const SRewriteMapping *left, const SRewriteMapping *right) {
  return left->dnodeId == right->dnodeId &&
         rewriteSameEp(&left->source, &right->source) &&
         rewriteSameEp(&left->target, &right->target);
}

static int32_t rewriteReportName(const char *name, int32_t *dnodeId) {
  const char *prefix = "precheck.dnode";
  const char *suffix = ".json";
  size_t      len = strlen(name);
  size_t      prefixLen = strlen(prefix);
  size_t      suffixLen = strlen(suffix);
  if (len <= prefixLen + suffixLen ||
      strncmp(name, prefix, prefixLen) != 0 ||
      strcmp(name + len - suffixLen, suffix) != 0) {
    return TSDB_CODE_INVALID_CFG;
  }
  size_t digitLen = len - prefixLen - suffixLen;
  char   digits[16] = {0};
  if (digitLen >= sizeof(digits)) return TSDB_CODE_INVALID_CFG;
  for (size_t i = 0; i < digitLen; ++i) {
    char digit = name[prefixLen + i];
    if (digit < '0' || digit > '9') return TSDB_CODE_INVALID_CFG;
    digits[i] = digit;
  }
  int64_t id = 0;
  TAOS_CHECK_RETURN(taosStr2int64(digits, &id));
  if (id <= 0 || id > INT32_MAX) return TSDB_CODE_INVALID_CFG;
  char expected[64] = {0};
  (void)snprintf(expected, sizeof(expected), "%s%d%s", prefix, (int32_t)id, suffix);
  if (strcmp(expected, name) != 0) return TSDB_CODE_INVALID_CFG;
  *dnodeId = (int32_t)id;
  return 0;
}

static int32_t rewriteAddReport(SRewriteReportSet *set, const char *dir, const char *name) {
  int32_t id = 0;
  if (rewriteReportName(name, &id) != 0) {
    printf("apply failed: unexpected precheck file name '%s'\n", name);
    return TSDB_CODE_INVALID_CFG;
  }
  char path[PATH_MAX];
  TAOS_CHECK_RETURN(rewritePath(path, sizeof(path), dir, name));
  cJSON  *json = NULL;
  int32_t code = rewriteReadJson(path, &json);
  if (code != 0) {
    printf("apply failed: cannot read precheck file '%s': %s\n", path, tstrerror(code));
    return code;
  }
  const char *status = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(json, "status"));
  const char *version = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(json, "version"));
  int64_t clusterId = 0, dnodeId = 0;
  int32_t identity = rewriteInt(json, "clusterId", &clusterId);
  if (identity == 0) identity = rewriteInt(json, "dnodeId", &dnodeId);
  if (status == NULL || strcmp(status, "passed") != 0) {
    printf("apply failed: precheck file '%s' did not pass\n", path);
    code = TSDB_CODE_INVALID_CFG;
  } else if (version == NULL || version[0] == '\0') {
    printf("apply failed: precheck file '%s' has no version\n", path);
    code = TSDB_CODE_INVALID_JSON_FORMAT;
  } else if (identity != 0 || clusterId <= 0 || dnodeId != id) {
    printf("apply failed: precheck file '%s' has an invalid identity\n", path);
    code = identity != 0 ? identity : TSDB_CODE_INVALID_CFG;
  }
  if (code == 0) {
    if (set->version[0] == '\0') {
      tstrncpy(set->version, version, sizeof(set->version));
      set->clusterId = clusterId;
      if (strcmp(set->version, td_version) != 0) {
        printf("apply failed: precheck file '%s' was generated by taosd %s, "
               "current version is %s\n", path, set->version, td_version);
        code = TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(set->version, version) != 0 || set->clusterId != clusterId) {
      printf("apply failed: precheck file '%s' belongs to another cluster or "
             "version\n", path);
      code = TSDB_CODE_INVALID_CFG;
    }
  }
  if (code == 0) {
    SArray *mapping = taosArrayInit(1, sizeof(SRewriteMapping));
    if (mapping == NULL) {
      code = terrno;
    } else {
      code = rewriteReadMapping(json, mapping);
      if (code == 0 && taosArrayGetSize(set->mapping) == 0) {
        taosArrayDestroy(set->mapping);
        set->mapping = mapping;
        mapping = NULL;
      }
      if (code == 0 && mapping != NULL) {
        if (taosArrayGetSize(mapping) != taosArrayGetSize(set->mapping)) {
          code = TSDB_CODE_INVALID_CFG;
        }
        for (size_t i = 0; code == 0 && i < taosArrayGetSize(mapping); ++i) {
          if (!rewriteSameMapping(taosArrayGet(mapping, i), taosArrayGet(set->mapping, i))) {
            code = TSDB_CODE_INVALID_CFG;
          }
        }
      }
      taosArrayDestroy(mapping);
      if (code != 0) {
        printf("apply failed: precheck file '%s' has an inconsistent mapping\n", path);
      }
    }
  }
  if (code == 0 && rewriteFindReport(set, id) != NULL) {
    printf("apply failed: precheck file '%s' duplicates dnode %d\n", path, id);
    code = TSDB_CODE_INVALID_CFG;
  }
  if (code != 0) {
    cJSON_Delete(json);
    return code;
  }
  SRewriteReport report = {.dnodeId = id, .json = json};
  if (taosArrayPush(set->reports, &report) == NULL) {
    cJSON_Delete(json);
    return terrno;
  }
  return 0;
}

static bool rewriteGroupHasMember(const char *members, int32_t id) {
  const char *p = members;
  while (*p != '\0') {
    int32_t value = 0;
    while (*p >= '0' && *p <= '9') {
      value = value * 10 + (*p - '0');
      ++p;
    }
    if (value == id) return true;
    while (*p != '\0' && *p != ',') ++p;
    if (*p == ',') ++p;
  }
  return false;
}

static int32_t rewriteValidateGroups(SRewriteReportSet *set) {
  int32_t  code = 0;
  SArray *groups = taosArrayInit(taosArrayGetSize(set->reports) * 2, sizeof(SRewriteGroupInfo));
  if (groups == NULL) return terrno;
  for (size_t i = 0; code == 0 && i < taosArrayGetSize(set->reports); ++i) {
    const SRewriteReport *report = taosArrayGet(set->reports, i);
    code = rewriteReportGroups(report->json, report->dnodeId, groups);
    if (code != 0) {
      printf("apply failed: precheck file of dnode %d has an invalid group "
             "record\n", report->dnodeId);
    }
  }
  size_t count = taosArrayGetSize(groups);
  for (size_t i = 0; code == 0 && i < count; ++i) {
    const SRewriteGroupInfo *group = taosArrayGet(groups, i);
    int64_t                  lower = group->index - REWRITE_GROUP_LOOKBACK;
    char                     label[32] = {0};
    rewriteGroupLabel(group->vgId, label, sizeof(label));
    if (lower < -1) lower = -1;
    if (group->snapshotVer < -1 || group->snapshotVer > group->index ||
        group->index > group->commitVer || group->commitVer > group->lastVer ||
        group->safeLowerBound < lower || group->safeLowerBound > group->index) {
      printf("apply failed: dnode %d reports inconsistent %s positions\n", group->dnodeId, label);
      code = TSDB_CODE_INVALID_CFG;
    }
  }
  for (size_t i = 0; code == 0 && i < count; ++i) {
    const SRewriteGroupInfo *group = taosArrayGet(groups, i);
    bool                     checked = false;
    char                     label[32] = {0};
    for (size_t k = 0; k < i && !checked; ++k) {
      checked = ((SRewriteGroupInfo *)taosArrayGet(groups, k))->vgId == group->vgId;
    }
    if (checked) continue;
    rewriteGroupLabel(group->vgId, label, sizeof(label));
    int64_t minIndex = INT64_MAX, maxLowerBound = INT64_MIN;
    for (size_t j = i; code == 0 && j < count; ++j) {
      const SRewriteGroupInfo *other = taosArrayGet(groups, j);
      if (other->vgId != group->vgId) continue;
      if (strcmp(other->members, group->members) != 0) {
        printf("apply failed: dnode %d reports different %s members\n", other->dnodeId, label);
        code = TSDB_CODE_INVALID_CFG;
        break;
      }
      if (other->index < minIndex) minIndex = other->index;
      if (other->safeLowerBound > maxLowerBound) {
        maxLowerBound = other->safeLowerBound;
      }
      if (!rewriteGroupHasMember(group->members, other->dnodeId)) {
        printf("apply failed: dnode %d is not a %s member but reports the "
               "group\n", other->dnodeId, label);
        code = TSDB_CODE_INVALID_CFG;
        break;
      }
      for (size_t k = j + 1; code == 0 && k < count; ++k) {
        const SRewriteGroupInfo *peer = taosArrayGet(groups, k);
        if (peer->vgId != other->vgId || peer->index != other->index) continue;
        if (peer->term != other->term) {
          printf("apply failed: dnodes %d and %d report different %s terms at "
                 "position %" PRId64 "\n", other->dnodeId, peer->dnodeId,
                 label, other->index);
          code = TSDB_CODE_INVALID_CFG;
          break;
        }
      }
    }
    if (code == 0 && minIndex < maxLowerBound) {
      printf("apply failed: %s cannot be recovered safely, snapshot position "
             "%" PRId64 " is below safe lower bound %" PRId64 "\n",
             label, minIndex, maxLowerBound);
      code = TSDB_CODE_INVALID_CFG;
    }
    const char *p = group->members;
    while (code == 0 && *p != '\0') {
      int32_t id = 0;
      while (*p >= '0' && *p <= '9') {
        id = id * 10 + (*p - '0');
        ++p;
      }
      bool found = false;
      for (size_t k = 0; k < count && !found; ++k) {
        const SRewriteGroupInfo *member = taosArrayGet(groups, k);
        found = member->vgId == group->vgId && member->dnodeId == id;
      }
      if (!found) {
        printf("apply failed: precheck file of dnode %d is missing for %s\n", id, label);
        code = TSDB_CODE_INVALID_CFG;
      }
      while (*p != '\0' && *p != ',') ++p;
      if (*p == ',') ++p;
    }
  }
  rewriteFreeGroups(groups);
  return code;
}

static int32_t rewriteLoadReports(SRewriteCheck *check, SRewriteReportSet *set) {
  const char *dir = check->option->precheckDir;
  TdDirPtr    handle = taosOpenDir(dir);
  if (handle == NULL) {
    printf("apply failed: cannot open precheck directory %s: %s\n", dir, terrstr());
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_CFG;
  }
  int32_t       code = 0;
  TdDirEntryPtr entry;
  while (code == 0 && (entry = taosReadDir(handle)) != NULL) {
    const char *name = taosGetDirEntryName(entry);
    if (strncmp(name, "precheck.dnode", sizeof("precheck.dnode") - 1) != 0 ||
        taosDirEntryIsDir(entry)) {
      continue;
    }
    code = rewriteAddReport(set, dir, name);
  }
  int32_t closeCode = taosCloseDir(&handle);
  if (code == 0) code = closeCode;
  if (code != 0) return code;
  if (taosArrayGetSize(set->mapping) == 0) {
    printf("apply failed: no usable precheck file found in %s\n", dir);
    return TSDB_CODE_INVALID_CFG;
  }
  if (taosArrayGetSize(set->reports) != taosArrayGetSize(set->mapping)) {
    printf("apply failed: %s has %zu precheck files for %zu dnodes\n", dir,
           taosArrayGetSize(set->reports), taosArrayGetSize(set->mapping));
    return TSDB_CODE_INVALID_CFG;
  }
  for (size_t i = 0; i < taosArrayGetSize(set->mapping); ++i) {
    const SRewriteMapping *map = taosArrayGet(set->mapping, i);
    if (rewriteFindReport(set, map->dnodeId) == NULL) {
      printf("apply failed: precheck file of dnode %d is missing\n", map->dnodeId);
      return TSDB_CODE_INVALID_CFG;
    }
  }
  return rewriteValidateGroups(set);
}

static bool rewriteSameGroup(const SRewriteGroupInfo *left, const SRewriteGroupInfo *right) {
  return left->index == right->index && left->term == right->term &&
         left->safeLowerBound == right->safeLowerBound &&
         left->snapshotVer == right->snapshotVer &&
         left->commitVer == right->commitVer &&
         left->lastVer == right->lastVer &&
         strcmp(left->members, right->members) == 0;
}

static int32_t rewriteCompareLocalState(SRewriteCheck *check, const SRewriteReport *report) {
  if (report == NULL) {
    printf("apply failed: there is no precheck file for dnode %d\n", check->dnodeId);
    return TSDB_CODE_INVALID_CFG;
  }
  int32_t code = 0;
  SArray *expected = taosArrayInit(4, sizeof(SRewriteGroupInfo));
  SArray *actual = taosArrayInit(4, sizeof(SRewriteGroupInfo));
  if (expected == NULL || actual == NULL) {
    rewriteFreeGroups(expected);
    rewriteFreeGroups(actual);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  code = rewriteReportGroups(report->json, check->dnodeId, expected);
  if (code == 0) {
    code = rewriteReportGroups(check->report, check->dnodeId, actual);
  }
  if (code == 0 && taosArrayGetSize(expected) != taosArrayGetSize(actual)) {
    printf("apply failed: local mnode and vnode inventory changed since the "
           "precheck\n");
    code = TSDB_CODE_INVALID_CFG;
  }
  for (size_t i = 0; code == 0 && i < taosArrayGetSize(expected); ++i) {
    const SRewriteGroupInfo *group = taosArrayGet(expected, i);
    bool                     found = false;
    char                     label[32] = {0};
    rewriteGroupLabel(group->vgId, label, sizeof(label));
    for (size_t j = 0; j < taosArrayGetSize(actual) && !found; ++j) {
      const SRewriteGroupInfo *candidate = taosArrayGet(actual, j);
      if (candidate->vgId != group->vgId) continue;
      found = true;
      if (!rewriteSameGroup(group, candidate)) {
        printf("apply failed: local %s state changed since the precheck, "
               "rerun the precheck\n", label);
        code = TSDB_CODE_INVALID_CFG;
      }
    }
    if (code == 0 && !found) {
      printf("apply failed: local %s is not in the precheck result, rerun the "
             "precheck\n", label);
      code = TSDB_CODE_INVALID_CFG;
    }
  }
  rewriteFreeGroups(expected);
  rewriteFreeGroups(actual);
  return code;
}

static int32_t rewriteApplyMapping(SRewriteCheck *check, const SRewriteReportSet *set) {
  if (taosArrayGetSize(set->mapping) != taosArrayGetSize(check->nodes)) {
    printf("apply failed: the mapping covers %zu dnodes but the local dnode "
           "list has %zu\n", taosArrayGetSize(set->mapping),
           taosArrayGetSize(check->nodes));
    return TSDB_CODE_INVALID_CFG;
  }
  for (size_t i = 0; i < taosArrayGetSize(set->mapping); ++i) {
    const SRewriteMapping *map = taosArrayGet(set->mapping, i);
    const SRewriteNode    *node = rewriteNode(check, map->dnodeId);
    if (node == NULL || !rewriteSameEp(&node->map.source, &map->source)) {
      printf("apply failed: local dnode %d does not use the reported endpoint "
             "%s:%u\n", map->dnodeId, map->source.fqdn,
             (unsigned)map->source.port);
      return TSDB_CODE_INVALID_CFG;
    }
    SDmEndpointMapping item = {.source = map->source, .target = map->target};
    if (taosArrayPush(check->option->pMappings, &item) == NULL) return terrno;
  }
  return 0;
}

static int32_t rewriteApplyFileKind(SRewriteCheck *check, const char *path, int32_t *kind) {
  char    expected[PATH_MAX] = {0};
  int32_t n = 0;
  if (strcmp(path, check->cfgFile) == 0) {
    *kind = REWRITE_FILE_CFG;
    return 0;
  }
  if (strcmp(path, check->dnodeFile) == 0) {
    *kind = REWRITE_FILE_DNODE;
    return 0;
  }
  const char *relative[] = {
      "dnode" TD_DIRSEP "config" TD_DIRSEP "local.json",
      "mnode" TD_DIRSEP "mnode.json",
      "mnode" TD_DIRSEP "sync" TD_DIRSEP "raft_config.json",
      "mnode" TD_DIRSEP "data" TD_DIRSEP "sdb.data"};
  const int32_t kinds[] = {REWRITE_FILE_LOCAL, REWRITE_FILE_MNODE,
                           REWRITE_FILE_RAFT, REWRITE_FILE_SDB};
  for (size_t i = 0; i < tListLen(relative); ++i) {
    TAOS_CHECK_RETURN(rewritePath(expected, sizeof(expected), tsDataDir,
                                  relative[i]));
    if (strcmp(path, expected) == 0) {
      *kind = kinds[i];
      return 0;
    }
  }
  n = snprintf(expected, sizeof(expected), "%s%ssnode%ssnode%d%ssnode.json",
               tsDataDir, TD_DIRSEP, TD_DIRSEP, check->dnodeId, TD_DIRSEP);
  if (n < 0 || n >= (int32_t)sizeof(expected)) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }
  if (strcmp(path, expected) == 0) {
    *kind = REWRITE_FILE_SNODE;
    return 0;
  }
  for (size_t i = 0; i < taosArrayGetSize(check->disks); ++i) {
    const SDiskCfg *disk = taosArrayGet(check->disks, i);
    char            base[PATH_MAX] = {0};
    n = snprintf(base, sizeof(base), "%s%svnode%s", disk->dir, TD_DIRSEP, TD_DIRSEP);
    if (n < 0 || n >= (int32_t)sizeof(base)) {
      return TSDB_CODE_OUT_OF_BUFFER;
    }
    size_t prefix = strlen(base);
    if (strncmp(path, base, prefix) != 0) continue;
    const char *name = path + prefix;
    if (strncmp(name, "vnode", 5) != 0 || name[5] < '0' || name[5] > '9') {
      continue;
    }
    const char *p = name + 5;
    while (*p >= '0' && *p <= '9') ++p;
    if (strcmp(p, TD_DIRSEP "vnode.json") == 0) {
      *kind = REWRITE_FILE_VNODE;
      return 0;
    }
    if (strcmp(p, TD_DIRSEP "sync" TD_DIRSEP "raft_config.json") == 0) {
      *kind = REWRITE_FILE_RAFT;
      return 0;
    }
  }
  printf("apply failed: refusing to modify unvalidated file %s\n", path);
  return TSDB_CODE_INVALID_CFG;
}

static int32_t rewriteSetString(cJSON *json, const char *key, const char *value) {
  cJSON *item = cJSON_CreateString(value);
  if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  if (!cJSON_ReplaceItemInObjectCaseSensitive(json, key, item)) {
    cJSON_Delete(item);  // cJSON takes ownership only when the replace succeeds
    return TSDB_CODE_INVALID_CFG;
  }
  return 0;
}

static int32_t rewriteSetNumber(cJSON *json, const char *key, int64_t value) {
  cJSON *item = cJSON_CreateNumber((double)value);
  if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  if (!cJSON_ReplaceItemInObjectCaseSensitive(json, key, item)) {
    cJSON_Delete(item);  // cJSON takes ownership only when the replace succeeds
    return TSDB_CODE_INVALID_CFG;
  }
  return 0;
}

// Writes a validated endpoint back to the site it came from. Only the keys the
// document actually holds are replaced, so a file that stores one of them
// without the other keeps its shape.
static int32_t rewriteSiteSet(const SRewriteSite *site, const SEp *ep) {
  if (site->text) {
    char text[TSDB_EP_LEN] = {0};
    (void)snprintf(text, sizeof(text), "%s:%u", ep->fqdn, (unsigned)ep->port);
    return rewriteSetString(site->object, site->hostKey, text);
  }
  if (cJSON_GetObjectItemCaseSensitive(site->object, site->hostKey) != NULL) {
    TAOS_CHECK_RETURN(rewriteSetString(site->object, site->hostKey, ep->fqdn));
  }
  cJSON *port = cJSON_GetObjectItemCaseSensitive(site->object, site->portKey);
  if (port == NULL) return 0;
  if (cJSON_IsNumber(port)) {
    return rewriteSetNumber(site->object, site->portKey, ep->port);
  }
  char text[16] = {0};
  (void)snprintf(text, sizeof(text), "%u", (unsigned)ep->port);
  return rewriteSetString(site->object, site->portKey, text);
}

typedef struct {
  SArray *changes;  // SRewriteChange, in the order the sites are visited
} SRewriteApplyParam;

// Apply policy: record the change the precheck already validated, then write it.
// A rejected site stops the whole file, because a file is only rewritten once
// every endpoint in it has been accepted.
static int32_t rewriteApplyPolicy(SRewriteCheck *check, void *param,
                                   const SRewriteSite *site,
                                   const SRewriteNode *node, const SEp *ep) {
  (void)check;
  if (node == NULL) return TSDB_CODE_INVALID_CFG;
  if (rewriteSameEp(&node->map.source, &node->map.target)) return 0;
  SRewriteApplyParam *apply = param;
  TAOS_CHECK_RETURN(rewriteAddChange(apply->changes, site->file, site->field,
                                     ep, &node->map.target));
  return rewriteSiteSet(site, &node->map.target);
}

static int32_t rewriteTransformDnodes(SRewriteCheck *check, SArray *changes,
                                       const char *path, cJSON *json) {
  SRewriteApplyParam param = {.changes = changes};
  return rewriteWalkArray(check, path,
      cJSON_GetObjectItemCaseSensitive(json, "dnodes"), &rewriteDnodes,
      rewriteApplyPolicy, &param);
}

static int32_t rewriteTransformRaft(SRewriteCheck *check, SArray *changes,
                                     const char *path, cJSON *json) {
  const cJSON *sync = cJSON_GetObjectItemCaseSensitive(
      cJSON_GetObjectItemCaseSensitive(json, "RaftCfg"), "SSyncCfg");
  if (!cJSON_IsObject(sync)) return TSDB_CODE_INVALID_JSON_FORMAT;
  SRewriteApplyParam param = {.changes = changes};
  return rewriteWalkArray(check, path,
      cJSON_GetObjectItemCaseSensitive(sync, "nodeInfo"),
      &rewriteMembersShape, rewriteApplyPolicy, &param);
}

static int32_t rewriteTransformVnode(SRewriteCheck *check, SArray *changes,
                                      const char *path, cJSON *json) {
  const cJSON *config = cJSON_GetObjectItemCaseSensitive(json, "config");
  if (!cJSON_IsObject(config)) return TSDB_CODE_INVALID_JSON_FORMAT;
  SRewriteApplyParam param = {.changes = changes};
  return rewriteWalkArray(check, path,
      cJSON_GetObjectItemCaseSensitive(config, "syncCfg.nodeInfo"),
      &rewriteMembersShape, rewriteApplyPolicy, &param);
}

static int32_t rewriteTransformMnode(SRewriteCheck *check, SArray *changes,
                                      const char *path, cJSON *json) {
  int64_t deployed = 0;
  TAOS_CHECK_RETURN(rewriteInt(json, "deployed", &deployed));
  if (deployed != 1) return 0;
  cJSON *replicas = cJSON_GetObjectItemCaseSensitive(json, "replicas");
  if (replicas == NULL) return 0;
  SRewriteApplyParam param = {.changes = changes};
  return rewriteWalkArray(check, path, replicas, &rewriteReplicas,
                          rewriteApplyPolicy, &param);
}

static int32_t rewriteTransformSnode(SRewriteCheck *check, SArray *changes,
                                      const char *path, cJSON *json) {
  SRewriteApplyParam param = {.changes = changes};
  return rewriteWalkSnode(check, path, json, rewriteApplyPolicy, &param);
}

static int32_t rewriteTransformConfigs(SRewriteCheck *check, SArray *changes,
                                        const char *path, cJSON *json) {
  cJSON *configs = cJSON_GetObjectItemCaseSensitive(json, "configs");
  if (!cJSON_IsObject(configs)) return TSDB_CODE_INVALID_JSON_FORMAT;
  SRewriteApplyParam param = {.changes = changes};
  return rewriteWalkConfigs(check, path, configs, rewriteApplyPolicy, &param);
}

// A CR line ending is white space, the same way paGetToken treats it.
static bool rewriteCfgBlank(char c) { return c == ' ' || c == '\t' || c == '\r'; }

static int32_t rewriteCfgKey(const char *name, int32_t len) {
  static const char *keys[] = {"fqdn", "serverPort", "firstEp", "secondEp"};
  for (int32_t i = 0; i < (int32_t)tListLen(keys); ++i) {
    if (len != (int32_t)strlen(keys[i])) continue;
    bool same = true;
    for (int32_t j = 0; j < len && same; ++j) {
      char left = name[j];
      char right = keys[i][j];
      if (left >= 'A' && left <= 'Z') left = (char)(left - 'A' + 'a');
      if (right >= 'A' && right <= 'Z') right = (char)(right - 'A' + 'a');
      same = left == right;
    }
    if (same) return i;
  }
  return -1;
}

static int32_t rewriteCfgTokens(const char *line, int32_t len,
                                 const char **name, int32_t *nameLen,
                                 const char **value, int32_t *valueLen,
                                 bool *quoted, bool *found) {
  *name = *value = NULL;
  *nameLen = *valueLen = 0;
  *quoted = *found = false;
  int32_t i = 0;
  while (i < len && rewriteCfgBlank(line[i])) ++i;
  if (i >= len || line[i] == '#') return 0;
  *name = line + i;
  while (i < len && !rewriteCfgBlank(line[i]) && line[i] != '#') ++i;
  *nameLen = i - (int32_t)(*name - line);
  while (i < len && rewriteCfgBlank(line[i])) ++i;
  if (i >= len || line[i] == '#') return 0;
  if (line[i] == '@') {
    *quoted = true;
    ++i;
    *value = line + i;
    while (i < len && line[i] != '@') ++i;
    if (i >= len) return TSDB_CODE_INVALID_CFG;
  } else {
    *value = line + i;
    while (i < len && !rewriteCfgBlank(line[i]) && line[i] != '#') {
      ++i;
    }
  }
  *valueLen = (int32_t)(line + i - *value);
  *found = true;
  return 0;
}

// taos.cfg is rewritten as text rather than as JSON so that comments, quoting
// and line endings survive. The action therefore differs from the JSON
// documents, while the acceptance rule is the same one the walkers use.
static int32_t rewriteCfgResolve(SRewriteCheck *check, SArray *changes,
                                  const char *file, SRewriteCfgValue *slots) {
  const SRewriteNode *self = rewriteNode(check, check->dnodeId);
  static const char  *names[] = {"fqdn", "serverPort", "firstEp", "secondEp"};
  int32_t             code = 0;
  // fqdn and serverPort are the two halves of the local endpoint.
  if (slots[0].value != NULL || slots[1].value != NULL) {
    SEp local = self->map.source;
    if (slots[0].value != NULL) {
      if (slots[0].len >= (int32_t)sizeof(local.fqdn)) {
        printf("apply failed: %s fqdn is too long\n", file);
        return TSDB_CODE_INVALID_CFG;
      }
      (void)memcpy(local.fqdn, slots[0].value, slots[0].len);
      local.fqdn[slots[0].len] = '\0';
    }
    if (slots[1].value != NULL) {
      int64_t port = 0;
      for (int32_t i = 0; i < slots[1].len && code == 0; ++i) {
        char digit = slots[1].value[i];
        if (digit < '0' || digit > '9' ||
            port > DM_REWRITE_ENDPOINTS_MAX_PORT) {
          printf("apply failed: %s serverPort is '%.*s', which is not a port\n",
                 file, slots[1].len, slots[1].value);
          code = TSDB_CODE_INVALID_CFG;
          break;
        }
        port = port * 10 + (digit - '0');
      }
      if (code == 0 && port < 1) {
        printf("apply failed: %s serverPort is '%.*s', which is not a port\n",
               file, slots[1].len, slots[1].value);
        code = TSDB_CODE_INVALID_CFG;
      }
      local.port = (uint16_t)port;
    }
    SRewriteSite        site = {.file = file, .field = "fqdn/serverPort",
                                .dnodeId = self->id};
    const SRewriteNode *node = NULL;
    if (code == 0 && !rewriteAcceptEp(check, &site, &local, &node)) {
      code = TSDB_CODE_INVALID_CFG;
    }
    if (code == 0 && !rewriteSameEp(&node->map.source, &node->map.target)) {
      code = rewriteAddChange(changes, file, site.field, &node->map.source,
                              &node->map.target);
      if (code == 0 && slots[0].value != NULL) {
        (void)snprintf(slots[0].text, sizeof(slots[0].text), "%s",
                       node->map.target.fqdn);
        slots[0].textLen = (int32_t)strlen(slots[0].text);
        slots[0].changed = true;
      }
      if (code == 0 && slots[1].value != NULL) {
        (void)snprintf(slots[1].text, sizeof(slots[1].text), "%u",
                       (unsigned)node->map.target.port);
        slots[1].textLen = (int32_t)strlen(slots[1].text);
        slots[1].changed = true;
      }
    }
  }
  for (int32_t key = 2; code == 0 && key < 4; ++key) {
    if (slots[key].value == NULL || slots[key].len == 0) continue;
    char ep[TSDB_EP_LEN] = {0};
    if (slots[key].len >= (int32_t)sizeof(ep)) {
      printf("apply failed: %s %s is too long\n", file, names[key]);
      code = TSDB_CODE_INVALID_CFG;
      break;
    }
    (void)memcpy(ep, slots[key].value, slots[key].len);
    SEp parsed = {0};
    code = taosGetFqdnPortFromEp(ep, &parsed);
    if (code != 0) {
      printf("apply failed: %s %s is '%.*s', which is not a fqdn:port endpoint: %s\n", file, names[key],
             slots[key].len, slots[key].value, tstrerror(code));
      break;
    }
    SRewriteSite        site = {.file = file, .field = names[key], .text = true};
    const SRewriteNode *node = NULL;
    if (!rewriteAcceptEp(check, &site, &parsed, &node)) {
      code = TSDB_CODE_INVALID_CFG;
      break;
    }
    if (rewriteSameEp(&node->map.source, &node->map.target)) continue;
    code = rewriteAddChange(changes, file, names[key], &node->map.source,
                            &node->map.target);
    if (code == 0) {
      (void)snprintf(slots[key].text, sizeof(slots[key].text), "%s:%u",
                     node->map.target.fqdn, (unsigned)node->map.target.port);
      slots[key].textLen = (int32_t)strlen(slots[key].text);
      slots[key].changed = true;
    }
  }
  return code;
}

static int32_t rewriteReadTextFile(const char *path, char **content, int32_t *len) {
  *content = NULL;
  *len = 0;
  TdFilePtr file = taosOpenFile(path, TD_FILE_READ | TD_FILE_CLOEXEC);
  if (file == NULL) return terrno;
  int64_t size = 0;
  int32_t code = taosFStatFile(file, &size, NULL);
  char   *buffer = NULL;
  if (code == 0 && (size < 0 || size > TSDB_MAX_MSG_SIZE)) {
    code = TSDB_CODE_FILE_CORRUPTED;
  }
  if (code == 0) {
    buffer = taosMemoryMalloc((size_t)size + 1);
    if (buffer == NULL) code = terrno;
  }
  if (code == 0) {
    int64_t readLen = taosReadFile(file, buffer, size);
    if (readLen != size) code = readLen < 0 ? terrno : TSDB_CODE_FILE_CORRUPTED;
  }
  int32_t closeCode = taosCloseFile(&file);
  if (code == 0) code = closeCode;
  if (code != 0) {
    taosMemoryFree(buffer);
    return code;
  }
  buffer[size] = '\0';
  *content = buffer;
  *len = (int32_t)size;
  return 0;
}

static int32_t rewriteTransformCfg(SRewriteCheck *check,
                                    SRewriteTarget *target, SArray *changes) {
  char   *content = NULL;
  int32_t len = 0;
  TAOS_CHECK_RETURN(rewriteReadTextFile(target->path, &content, &len));
  SRewriteCfgValue slots[4] = {{0}};
  int32_t          code = 0;
  for (int32_t pos = 0; pos <= len && code == 0;) {
    int32_t end = pos;
    while (end < len && content[end] != '\n') ++end;
    const char *name = NULL, *value = NULL;
    int32_t     nameLen = 0, valueLen = 0;
    bool        quoted = false, found = false;
    code = rewriteCfgTokens(content + pos, end - pos, &name, &nameLen, &value,
                            &valueLen, &quoted, &found);
    if (code == 0 && found) {
      int32_t key = rewriteCfgKey(name, nameLen);
      if (key >= 0) {
        slots[key].value = value;
        slots[key].len = valueLen;
        slots[key].quoted = quoted;
      }
    }
    if (end >= len) break;
    pos = end + 1;
  }
  if (code == 0) code = rewriteCfgResolve(check, changes, target->path, slots);
  // The file as it is, plus a worst-case replacement for each of the 4 keys: the
  // longest text a slot can hold, the 2 '@' that wrap a quoted value, and 1
  // spare byte. The bytes of the replaced value are not subtracted, so this is
  // an upper bound.
  int32_t size = len + 4 * ((int32_t)sizeof(slots[0].text) + 2) + 1;
  char   *out = NULL;
  if (code == 0) {
    out = taosMemoryMalloc(size);
    if (out == NULL) code = terrno;
  }
  int32_t used = 0;
  for (int32_t pos = 0; code == 0 && pos <= len;) {
    int32_t end = pos;
    while (end < len && content[end] != '\n') ++end;
    const char *line = content + pos;
    int32_t     lineLen = end - pos;
    bool        replaced = false;
    for (int32_t key = 0; key < 4 && !replaced; ++key) {
      if (!slots[key].changed || slots[key].value < line ||
          slots[key].value >= line + lineLen) {
        continue;
      }
      int32_t prefix = (int32_t)(slots[key].value - line);
      (void)memcpy(out + used, line, prefix);
      used += prefix;
      if (slots[key].quoted) out[used++] = '@';
      (void)memcpy(out + used, slots[key].text, slots[key].textLen);
      used += slots[key].textLen;
      if (slots[key].quoted) out[used++] = '@';
      int32_t suffix = lineLen - prefix - slots[key].len;
      (void)memcpy(out + used, slots[key].value + slots[key].len, suffix);
      used += suffix;
      slots[key].changed = false;
      replaced = true;
    }
    if (!replaced) {
      (void)memcpy(out + used, line, lineLen);
      used += lineLen;
    }
    if (end >= len) break;
    out[used++] = '\n';
    pos = end + 1;
  }
  taosMemoryFree(content);
  if (code != 0) {
    taosMemoryFree(out);
    printf("apply failed: cannot rewrite %s: %s\n", target->path, tstrerror(code));
    return code;
  }
  target->content = out;
  target->len = used;
  return 0;
}

static int32_t rewriteTransformTarget(SRewriteCheck *check, SRewriteTarget *target, SArray *changes) {
  if (target->kind == REWRITE_FILE_CFG) {
    return rewriteTransformCfg(check, target, changes);
  }
  if (target->kind == REWRITE_FILE_SDB) return 0;
  cJSON  *json = NULL;
  int32_t code = rewriteReadJson(target->path, &json);
  if (code != 0) {
    printf("apply failed: cannot read %s: %s\n", target->path, tstrerror(code));
    return code;
  }
  switch (target->kind) {
    case REWRITE_FILE_LOCAL:
      code = rewriteTransformConfigs(check, changes, target->path, json);
      break;
    case REWRITE_FILE_DNODE:
      code = rewriteTransformDnodes(check, changes, target->path, json);
      break;
    case REWRITE_FILE_MNODE:
      code = rewriteTransformMnode(check, changes, target->path, json);
      break;
    case REWRITE_FILE_RAFT:
      code = rewriteTransformRaft(check, changes, target->path, json);
      break;
    case REWRITE_FILE_VNODE:
      code = rewriteTransformVnode(check, changes, target->path, json);
      break;
    case REWRITE_FILE_SNODE:
      code = rewriteTransformSnode(check, changes, target->path, json);
      break;
    default:
      code = TSDB_CODE_INVALID_CFG;
      break;
  }
  if (code == 0) {
    char *content = cJSON_Print(json);
    if (content == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      target->content = content;
      target->len = (int32_t)strlen(content);
    }
  }
  cJSON_Delete(json);
  if (code != 0) {
    printf("apply failed: cannot rewrite %s: %s\n", target->path, tstrerror(code));
  }
  return code;
}

static SRewriteTarget *rewriteFindTarget(SArray *targets, const char *path) {
  for (size_t i = 0; i < taosArrayGetSize(targets); ++i) {
    SRewriteTarget *target = taosArrayGet(targets, i);
    if (strcmp(target->path, path) == 0) return target;
  }
  return NULL;
}

static int32_t rewriteCountCheckChanges(const SRewriteCheck *check, const char *file) {
  int32_t count = 0;
  for (int32_t i = 0; i < cJSON_GetArraySize(check->changes); ++i) {
    const cJSON *change = cJSON_GetArrayItem(check->changes, i);
    const char  *name = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(change, "file"));
    if (name != NULL && strcmp(name, file) == 0) ++count;
  }
  return count;
}

static int32_t rewritePlan(SRewriteCheck *check, SArray *targets, SArray *changes) {
  for (int32_t i = 0; i < cJSON_GetArraySize(check->changes); ++i) {
    const cJSON *change = cJSON_GetArrayItem(check->changes, i);
    const char  *file = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(change, "file"));
    if (file == NULL) return TSDB_CODE_INVALID_CFG;
    if (rewriteFindTarget(targets, file) != NULL) continue;
    SRewriteTarget target = {0};
    tstrncpy(target.path, file, sizeof(target.path));
    TAOS_CHECK_RETURN(rewriteApplyFileKind(check, file, &target.kind));
    if (taosArrayPush(targets, &target) == NULL) return terrno;
  }
  for (size_t i = 0; i < taosArrayGetSize(targets); ++i) {
    TAOS_CHECK_RETURN(rewriteTransformTarget(check, taosArrayGet(targets, i), changes));
  }
  return 0;
}

static int32_t rewriteCollectCheckChanges(SRewriteCheck *check, SArray *changes) {
  char sdbData[PATH_MAX] = {0};
  TAOS_CHECK_RETURN(rewritePath(sdbData, sizeof(sdbData), tsDataDir,
                                "mnode" TD_DIRSEP "data" TD_DIRSEP "sdb.data"));
  for (int32_t i = 0; i < cJSON_GetArraySize(check->changes); ++i) {
    const cJSON *change = cJSON_GetArrayItem(check->changes, i);
    const char  *file = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(change, "file"));
    const char  *field = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(change, "field"));
    if (file == NULL || field == NULL) return TSDB_CODE_INVALID_CFG;
    if (strcmp(file, sdbData) == 0) continue;
    const cJSON *sourceEp = cJSON_GetObjectItemCaseSensitive(change, "source");
    const cJSON *targetEp = cJSON_GetObjectItemCaseSensitive(change, "target");
    SEp          source = {0}, target = {0};
    TAOS_CHECK_RETURN(rewriteReadEp(sourceEp, "fqdn", "port", &source));
    TAOS_CHECK_RETURN(rewriteReadEp(targetEp, "fqdn", "port", &target));
    TAOS_CHECK_RETURN(rewriteAddChange(changes, file, field, &source, &target));
  }
  return 0;
}

static int32_t rewriteCompareChanges(SRewriteCheck *check, SArray *changes) {
  SArray *expected = taosArrayInit(1, sizeof(SRewriteChange));
  if (expected == NULL) return terrno;
  int32_t code = rewriteCollectCheckChanges(check, expected);
  rewriteSortChanges(expected);
  rewriteSortChanges(changes);
  if (code == 0 && taosArrayGetSize(expected) != taosArrayGetSize(changes)) {
    printf("apply failed: %zu endpoints were validated but %zu were rewritten\n",
           taosArrayGetSize(expected), taosArrayGetSize(changes));
    code = TSDB_CODE_INVALID_CFG;
  }
  for (size_t i = 0; code == 0 && i < taosArrayGetSize(expected); ++i) {
    const SRewriteChange *left = taosArrayGet(expected, i);
    const SRewriteChange *right = taosArrayGet(changes, i);
    if (rewriteCompareChange(left, right) == 0) continue;
    printf("apply failed: the rewrite of %s %s differs from the validated "
           "change set\n", right->file, right->field);
    code = TSDB_CODE_INVALID_CFG;
  }
  taosArrayDestroy(expected);
  if (code != 0) {
    printf("apply failed: files changed between the checks and the rewrite, "
           "rerun the precheck\n");
  }
  return code;
}

static int32_t rewriteBackupPath(const char *path, char *backup, int32_t size) {
  int32_t n = snprintf(backup, size, "%s" REWRITE_BACKUP_SUFFIX, path);
  return n < 0 || n >= size ? TSDB_CODE_OUT_OF_BUFFER : 0;
}

static int32_t rewriteCopyFile(const char *from, const char *to, bool exclusive) {
  TdFilePtr source = taosOpenFile(from, TD_FILE_READ | TD_FILE_CLOEXEC);
  if (source == NULL) return terrno;
  int32_t flags = TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_CLOEXEC | (exclusive ? TD_FILE_EXCL : 0);
  TdFilePtr copy = taosOpenFile(to, flags);
  int32_t code = copy == NULL ? terrno : 0;
  char    buffer[64 * 1024];
  while (code == 0) {
    int64_t readLen = taosReadFile(source, buffer, sizeof(buffer));
    if (readLen == 0) break;
    if (readLen < 0) {
      code = terrno;
      break;
    }
    if (taosWriteFile(copy, buffer, readLen) != readLen) {
      code = terrno != 0 ? terrno : TAOS_SYSTEM_ERROR(EIO);
    }
  }
  if (code == 0) code = taosFsyncFile(copy);
  int32_t closeCode = taosCloseFile(&source);
  if (code == 0) code = closeCode;
  if (copy != NULL) {
    closeCode = taosCloseFile(&copy);
    if (code == 0) code = closeCode;
  }
  if (code != 0 && exclusive) {
    int32_t removeCode = taosRemoveFile(to);
    if (removeCode != 0 && !errorIsFileNotExist(removeCode)) {
      printf("apply failed: cannot remove backup file %s: %s\n", to, tstrerror(removeCode));
    }
  }
  return code;
}

static void rewriteRemoveBackups(SArray *targets, size_t count) {
  for (size_t i = 0; i < count && i < taosArrayGetSize(targets); ++i) {
    const SRewriteTarget *target = taosArrayGet(targets, i);
    char                  backup[PATH_MAX] = {0};
    if (rewriteBackupPath(target->path, backup, sizeof(backup)) != 0) continue;
    int32_t code = taosRemoveFile(backup);
    if (code != 0 && !errorIsFileNotExist(code)) {
      printf("apply failed: cannot remove backup file %s: %s\n", backup, tstrerror(code));
    }
  }
}

static int32_t rewriteCreateBackups(SArray *targets) {
  for (size_t i = 0; i < taosArrayGetSize(targets); ++i) {
    const SRewriteTarget *target = taosArrayGet(targets, i);
    char    backup[PATH_MAX] = {0};
    int32_t code = rewriteBackupPath(target->path, backup, sizeof(backup));
    if (code == 0 && taosCheckExistFile(backup)) {
      printf("apply failed: backup file %s already exists, remove it and retry\n", backup);
      code = TSDB_CODE_INVALID_CFG;
    }
    if (code == 0) code = rewriteCopyFile(target->path, backup, true);
    if (code != 0) {
      printf("apply failed: cannot back up %s: %s\n", target->path, tstrerror(code));
      rewriteRemoveBackups(targets, i);
      return code;
    }
  }
  return 0;
}

// Restores every rewritten file from its backup and drops the backups that were
// restored. A backup whose restore failed stays in place: it is the only copy of
// the original content left, and keeping it makes the startup guard stop the
// dnode instead of silently leaving a half rewritten instance behind.
static int32_t rewriteRestoreBackups(SArray *targets) {
  int32_t code = 0;
  for (size_t i = 0; i < taosArrayGetSize(targets); ++i) {
    const SRewriteTarget *target = taosArrayGet(targets, i);
    char    backup[PATH_MAX] = {0};
    int32_t ret = rewriteBackupPath(target->path, backup, sizeof(backup));
    if (ret == 0) ret = rewriteCopyFile(backup, target->path, false);
    if (ret != 0) {
      printf("apply failed: cannot restore %s from %s: %s\n", target->path, backup, tstrerror(ret));
      // The manual's recovery steps cover this shape: the backup is still there.
      printf("apply failed: %s is kept for manual recovery\n", backup);
      if (code == 0) code = ret;
      continue;
    }
    int32_t removeCode = taosRemoveFile(backup);
    if (removeCode != 0 && !errorIsFileNotExist(removeCode)) {
      printf("apply failed: cannot remove backup file %s: %s\n", backup, tstrerror(removeCode));
    }
  }
  return code;
}

static int32_t rewriteWriteTextFile(const char *path, const char *content, int32_t len) {
  char    tmp[PATH_MAX] = {0};
  int32_t n = snprintf(tmp, sizeof(tmp), "%s.tmp", path);
  if (n < 0 || n >= (int32_t)sizeof(tmp)) return TSDB_CODE_OUT_OF_BUFFER;
  TdFilePtr file = taosOpenFile(tmp, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_CLOEXEC);
  if (file == NULL) return terrno;
  int32_t code = 0;
  if (taosWriteFile(file, content, len) != len) {
    code = terrno != 0 ? terrno : TAOS_SYSTEM_ERROR(EIO);
  }
  if (code == 0) code = taosFsyncFile(file);
  int32_t closeCode = taosCloseFile(&file);
  if (code == 0) code = closeCode;
  if (code == 0) code = taosRenameFile(tmp, path);
  if (code != 0) {
    int32_t removeCode = taosRemoveFile(tmp);
    if (removeCode != 0 && !errorIsFileNotExist(removeCode)) {
      printf("apply failed: cannot remove %s: %s\n", tmp, tstrerror(removeCode));
    }
  }
  return code;
}

static int32_t rewriteApplySdb(SRewriteCheck *check, const SRewriteTarget *target) {
  int32_t expected = rewriteCountCheckChanges(check, target->path);
  SArray *endpoints = taosArrayInit(taosArrayGetSize(check->nodes), sizeof(SDnodeEp));
  if (endpoints == NULL) return terrno;
  for (size_t i = 0; i < taosArrayGetSize(check->nodes); ++i) {
    const SRewriteNode *node = taosArrayGet(check->nodes, i);
    SDnodeEp            ep = {.id = node->id, .ep = node->map.target};
    if (taosArrayPush(endpoints, &ep) == NULL) {
      taosArrayDestroy(endpoints);
      return terrno;
    }
  }
  int32_t rewritten = 0;
  int32_t code = mndRewriteSdbDnodes(target->path, check->mnodeEncrypted, endpoints, &rewritten);
  taosArrayDestroy(endpoints);
  if (code == 0 && rewritten != expected) {
    printf("apply failed: %s contains %d endpoint changes, %d were expected\n", target->path, rewritten, expected);
    code = TSDB_CODE_INVALID_CFG;
  }
  return code;
}

static int32_t rewriteWriteTarget(SRewriteCheck *check, const SRewriteTarget *target) {
  if (target->kind == REWRITE_FILE_SDB) return rewriteApplySdb(check, target);
  if (target->kind == REWRITE_FILE_CFG) {
    return rewriteWriteTextFile(target->path, target->content, target->len);
  }
  return taosWriteCfgFile(target->path, target->content, target->len);
}

static int32_t rewriteWriteTargets(SRewriteCheck *check, SArray *targets) {
  for (size_t i = 0; i < taosArrayGetSize(targets); ++i) {
    const SRewriteTarget *target = taosArrayGet(targets, i);
    int32_t code = rewriteWriteTarget(check, target);
    if (code != 0) {
      printf("apply failed: cannot write %s: %s\n", target->path, tstrerror(code));
      return code;
    }
  }
  return 0;
}

int32_t dmRewriteEndpointsApply(const char *cfgDir,
                                const SDmRewriteEndpointsOption *option) {
  SRewriteCheck            check = {.stage = "apply"};
  SDmRewriteEndpointsOption local = *option;
  SRewriteReportSet        set = {0};
  SArray *targets = taosArrayInit(1, sizeof(SRewriteTarget));
  SArray *changes = taosArrayInit(1, sizeof(SRewriteChange));
  int32_t code = 0;

  if (targets == NULL || changes == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  local.pMappings = taosArrayInit(1, sizeof(SDmEndpointMapping));
  set.reports = taosArrayInit(1, sizeof(SRewriteReport));
  set.mapping = taosArrayInit(1, sizeof(SRewriteMapping));
  if (local.pMappings == NULL || set.reports == NULL || set.mapping == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  check.option = &local;
  code = rewriteLoadCfg(&check, cfgDir);
  if (code != 0) {
    printf("apply failed: cannot load the configuration: %s\n", tstrerror(code));
    goto _exit;
  }
  code = rewriteCheckLeftovers(cfgDir, check.disks, "apply failed");
  if (code != 0) goto _exit;
  char path[PATH_MAX];
  code = rewritePath(path, sizeof(path), tsDataDir, ".running");
  if (code != 0) goto _exit;
  // Do not create or truncate the daemon's existing lock file.
  check.lock = taosOpenFile(path, TD_FILE_WRITE | TD_FILE_CLOEXEC);
  int32_t lockCode = check.lock == NULL ? terrno : taosLockFile(check.lock);
  code = rewriteLoadKeys();
  if (code != 0) {
    printf("apply failed: cannot load the encryption keys: %s\n", tstrerror(code));
    goto _exit;
  }
  check.report = cJSON_CreateObject();
  check.nodes = taosArrayInit(1, sizeof(SRewriteNode));
  check.changes = cJSON_CreateArray();
  check.errors = check.report == NULL ? NULL : cJSON_AddArrayToObject(check.report, "errors");
  if (check.report == NULL || check.nodes == NULL || check.errors == NULL || check.changes == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  code = rewriteIdentity(&check);
  if (code != 0) {
    printf("apply failed: cannot read the local identity from %s: %s\n", check.dnodeFile, tstrerror(code));
    goto _exit;
  }
  code = rewriteLoadReports(&check, &set);
  if (code != 0) goto _exit;
  if (set.clusterId != check.clusterId) {
    printf("apply failed: precheck files belong to cluster %" PRId64 " but the local cluster is %" PRId64 "\n",
           set.clusterId, check.clusterId);
    code = TSDB_CODE_INVALID_CFG;
    goto _exit;
  }
  code = rewriteError(&check, path, "instance stopped and locked", lockCode);
  if (code == 0) code = check.firstError;
  if (code == 0) {
    code = rewriteApplyMapping(&check, &set);
    if (code == 0) code = rewriteMappings(&check);
    if (code == 0) code = rewriteConfigs(&check);
    if (code == 0) {
      int32_t ret = rewriteMnode(&check);
      code = rewriteError(&check, "mnode", "snapshot and WAL", ret);
    }
    if (code == 0) {
      int32_t ret = rewriteVnodes(&check);
      code = rewriteError(&check, "vnode", "local vnode inventory", ret);
    }
    if (code == 0) {
      int32_t ret = rewriteSnode(&check);
      code = rewriteError(&check, "snode", "configuration endpoints", ret);
    }
    if (code == 0) code = check.firstError;
    if (code == 0) {
      const SRewriteReport *report = rewriteFindReport(&set, check.dnodeId);
      code = rewriteCompareLocalState(&check, report);
      if (code != 0) {
        printf("apply failed: local state differs from the precheck result\n");
      }
    }
    if (code == 0) {
      code = rewritePlan(&check, targets, changes);
      if (code == 0) code = rewriteCompareChanges(&check, changes);
    }
    if (code == 0) code = rewriteCreateBackups(targets);
    if (code == 0) {
      code = rewriteWriteTargets(&check, targets);
      if (code != 0) {
        printf("apply failed: rolling back\n");
        if (rewriteRestoreBackups(targets) != 0) {
          printf("apply failed: the rollback is incomplete, the backups that could not be "
                 "restored were kept; follow the recovery steps of the 'Changing the Address "
                 "of a Node' section of the manual before starting taosd\n");
        }
        goto _exit;
      }
      rewriteRemoveBackups(targets, taosArrayGetSize(targets));
    }
  }
  if (code == 0) {
    rewritePrintChanges(check.changes, "ENDPOINTS REWRITE APPLIED:");
    printf("\nendpoint rewrite applied: %d endpoints in %zu files\n",
           cJSON_GetArraySize(check.changes), taosArrayGetSize(targets));
    printf(
        "\nNOTE: Only the dnode FQDN/port entries in this instance's taosd configuration files"
        " and data directories were rewritten. Clients and services that connect to the old"
        " endpoints, such as taosKeeper, taosAdapter, taosX and taosExplorer, are not updated"
        " and may fail to connect. External source addresses of Federated Query/Stream may also"
        " need an update. Please ensure all relevant configurations are updated accordingly"
        " manually.\n");
  }

_exit:
  if (check.lock != NULL) {
    int32_t closeCode = taosCloseFile(&check.lock);
    if (code == 0) code = closeCode;
  }
  if (code != 0) printf("endpoint rewrite apply failed: %s\n", tstrerror(code));
  for (size_t i = 0; i < taosArrayGetSize(targets); ++i) {
    taosMemoryFree(((SRewriteTarget *)taosArrayGet(targets, i))->content);
  }
  taosArrayDestroy(targets);
  taosArrayDestroy(changes);
  for (size_t i = 0; i < taosArrayGetSize(set.reports); ++i) {
    cJSON_Delete(((SRewriteReport *)taosArrayGet(set.reports, i))->json);
  }
  taosArrayDestroy(set.reports);
  taosArrayDestroy(set.mapping);
  taosArrayDestroy(local.pMappings);
  cJSON_Delete(check.changes);
  cJSON_Delete(check.report);
  taosArrayDestroy(check.nodes);
  taosArrayDestroy(check.disks);
  cfgCleanup(check.cfg);
  return code;
}

/* ------------------------------------------------------------------------- */
/* Startup guard                                                              */
/* ------------------------------------------------------------------------- */

#define REWRITE_LEFTOVER_DEPTH 4

typedef struct {
  char path[PATH_MAX];
} SRewriteLeftover;

static int32_t rewriteAddLeftover(SArray *found, const char *dir, const char *name) {
  SRewriteLeftover item = {0};
  int32_t          n = snprintf(item.path, sizeof(item.path), "%s%s%s", dir, TD_DIRSEP, name);
  if (n < 0 || n >= (int32_t)sizeof(item.path)) return TSDB_CODE_OUT_OF_BUFFER;
  if (taosArrayPush(found, &item) == NULL) return terrno;
  return 0;
}

// Walks a directory tree and collects every rewrite backup. The depth is
// bounded because a backup only ever sits next to one of the files the apply
// rewrites, at most three levels below a data or disk directory.
static int32_t rewriteScanLeftovers(SArray *found, const char *dir, int32_t depth) {
  if (!taosIsDir(dir)) return 0;
  TdDirPtr handle = taosOpenDir(dir);
  // An unreadable directory must not block the startup on its own: whatever
  // needs it fails later with a message of its own.
  if (handle == NULL) return 0;
  size_t        suffixLen = strlen(REWRITE_BACKUP_SUFFIX);
  int32_t       code = 0;
  TdDirEntryPtr entry;
  while (code == 0 && (entry = taosReadDir(handle)) != NULL) {
    const char *name = taosGetDirEntryName(entry);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;
    size_t nameLen = strlen(name);
    if (nameLen > suffixLen && strcmp(name + nameLen - suffixLen, REWRITE_BACKUP_SUFFIX) == 0) {
      code = rewriteAddLeftover(found, dir, name);
      continue;
    }
    if (depth <= 1 || !taosDirEntryIsDir(entry)) continue;
    char child[PATH_MAX];
    int32_t n = snprintf(child, sizeof(child), "%s%s%s", dir, TD_DIRSEP, name);
    if (n < 0 || n >= (int32_t)sizeof(child)) {
      code = TSDB_CODE_OUT_OF_BUFFER;
      break;
    }
    code = rewriteScanLeftovers(found, child, depth - 1);
  }
  int32_t closeCode = taosCloseDir(&handle);
  if (code == 0) code = closeCode;
  return code;
}

// Resolves the directory that holds taos.cfg, which is where its backup lands.
// The configuration path may either be the directory or the file itself.
static int32_t rewriteCfgDirOf(const char *cfgDir, char *dir, int32_t size) {
  char path[PATH_MAX] = {0};
  TAOS_CHECK_RETURN(taosExpandDir(cfgDir, path, sizeof(path)));
  if (strlen(path) >= (size_t)size) return TSDB_CODE_OUT_OF_BUFFER;
  if (taosIsDir(path)) {
    tstrncpy(dir, path, size);
    return 0;
  }
  // Either separator ends the directory name: on Windows a path typed by hand
  // is often written with '/', which the rest of the tool never produces.
  const char *sep = strrchr(path, TD_DIRSEP_CHAR);
#ifdef WINDOWS
  const char *alt = strrchr(path, '/');
  if (alt != NULL && (sep == NULL || alt > sep)) sep = alt;
#endif
  if (sep == NULL) return TSDB_CODE_INVALID_CFG;
  // A separator in the first position names the root directory, which stays as
  // it is: copying nothing would leave the caller with an empty directory.
  size_t len = sep == path ? 1 : (size_t)(sep - path);
  if (len + 1 > (size_t)size) return TSDB_CODE_OUT_OF_BUFFER;
  (void)memcpy(dir, path, len);
  dir[len] = '\0';
  return 0;
}

// Scans the configuration directory, the data directory and every level-0 disk.
// `disks` holds SDiskCfg entries: NULL lets the caller fall back to the daemon
// configuration, while the offline commands pass the list they resolved.
static int32_t rewriteScanInstance(SArray *found, const char *cfgDir, const SArray *disks) {
  char    cfg[PATH_MAX] = {0};
  int32_t code = rewriteCfgDirOf(cfgDir, cfg, sizeof(cfg));
  if (code == 0) code = rewriteScanLeftovers(found, cfg, 1);
  if (code == 0) code = rewriteScanLeftovers(found, tsDataDir, REWRITE_LEFTOVER_DEPTH);
  for (size_t i = 0; code == 0 && disks != NULL && i < taosArrayGetSize(disks); ++i) {
    const SDiskCfg *disk = taosArrayGet(disks, i);
    if (disk->level != 0 || strcmp(disk->dir, tsDataDir) == 0) continue;
    code = rewriteScanLeftovers(found, disk->dir, REWRITE_LEFTOVER_DEPTH);
  }
  return code;
}

// A leftover backup means a rewrite is half done, so the instance is neither
// prechecked, applied nor started until the files are handled.
static int32_t rewriteReportLeftovers(SArray *found, const char *stage) {
  size_t count = taosArrayGetSize(found);
  if (count == 0) return 0;
  for (size_t i = 0; i < count; ++i) {
    const SRewriteLeftover *item = taosArrayGet(found, i);
    printf("%s: endpoint rewrite backup left behind: %s\n", stage, item->path);
    dError("%s: endpoint rewrite backup left behind:%s", stage, item->path);
  }
  printf("%s: %zu endpoint rewrite backup file(s) were left behind, handle them as the 'Changing the "
         "Address of a Node' section of the manual describes and try again\n", stage, count);
  dError("%s: %zu endpoint rewrite backup file(s) were left behind", stage, count);
  return TSDB_CODE_INVALID_CFG;
}

static int32_t rewriteCheckLeftovers(const char *cfgDir, const SArray *disks, const char *stage) {
  SArray *found = taosArrayInit(4, sizeof(SRewriteLeftover));
  if (found == NULL) return terrno;
  int32_t code = rewriteScanInstance(found, cfgDir, disks);
  if (code == 0) code = rewriteReportLeftovers(found, stage);
  taosArrayDestroy(found);
  return code;
}

int32_t dmRewriteEndpointsCheckLeftovers(const char *cfgDir) {
  SArray *disks = NULL;
  if (tsCfg != NULL) {
    SConfigItem *data = cfgGetItem(tsCfg, "dataDir");
    if (data != NULL) disks = data->array;
  }
  return rewriteCheckLeftovers(cfgDir, disks, "failed to start");
}
