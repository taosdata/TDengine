/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */
    
#include "bckArgs.h"
#include "bck.h"
#include "cus_name.h"

//
// ---------------- global args state ----------------
//

#define MAX_DBS 64

static enum ActionType g_action    = ACTION_BACKUP;
static char  g_outPath[MAX_PATH_LEN]   = "./output";
static char  g_host[256]           = "";
static int   g_port                = 6030;
static char  g_user[128]           = "root";
static char  g_password[128]       = "taosdata";
static int   g_dataThread          = 8;
static int   g_tagThread           = 2;
static int   g_retryCount          = 10;
static int   g_retrySleepMs        = 1000;
static char *g_dbs[MAX_DBS + 1]    = { NULL };
static int   g_dbCount             = 0;
static char  g_startTime[64]       = "";
static char  g_endTime[64]         = "";
static char  g_timeFilter[256]     = "";
static int   g_schemaOnly          = 0;
static int   g_debug               = 0;
static int   g_checkpoint          = 0;  // -C: resume from last checkpoint
StorageFormat g_storageFormat = BINARY_TAOS; // default
StmtVersion   g_stmtVersion  = STMT_VERSION_2; // default: STMT2
static BckContent g_content  = BCK_CONTENT_BASIC; // default: basic data only

// rename map: oldName -> newName
#define MAX_RENAME 64
static char *g_renameOld[MAX_RENAME];
static char *g_renameNew[MAX_RENAME];
static int   g_renameCount = 0;
static char  g_renameRaw[1024] = "";

// positional args: dbname [tbname ...]
#define MAX_SPEC_TABLES 1000
static char  g_specDb[TSDB_DB_NAME_LEN] = "";
static char *g_specTables[MAX_SPEC_TABLES + 1] = {NULL};
static int   g_specTableCount = 0;

// DSN / cloud connection
static char  g_dsn[2048]    = "";
static bool  g_dsnMode      = false;

// driver / connect mode (set via -Z / --driver); CONN_MODE_INVALID = auto
static int8_t g_driver = CONN_MODE_INVALID;

// TDengine config directory (set via -c / --config-dir)
static char g_configDir[MAX_PATH_LEN] = "";

// data-batch: max rows per STMT bind/execute call; 0 = use per-version default
static int g_dataBatch = 0;
static bool g_stmtVersionSet = false;

//
// ---------------- usage ----------------
//

void printVersion(bool verbose) {
    if (verbose) {
        logTee("%s\n", TD_PRODUCT_NAME);
    }
    logTee("taosDump version: %s\n", TD_VER_NUMBER);
    if (verbose) {
        logTee("git: %s\n", TAOSDUMP_COMMIT_ID);
        logTee("build: %s\n", BUILD_INFO);
    }
}

static void printUsage(const char *prog) {
    printf("\n");
    printf("Usage: %s [OPTION...] dbname [tbname ...] -o outpath\n", prog);
    printf("  or:  %s [OPTION...] -o outpath\n", prog);
    printf("  or:  %s [OPTION...] -i inpath\n", prog);
    printf("  or:  %s [OPTION...] --databases db1,db2,...\n", prog);
    printf("\nOptions:\n");
    printf("  -h, --host=HOST            Server host. Default is localhost.\n");
    printf("  -p, --password             Prompt for password.\n");
    printf("      -pPASSWORD             Use PASSWORD directly, no space.\n");
    printf("      --password=PASSWORD    Use PASSWORD directly.\n");
    printf("  -P, --port=PORT            Server port. Default is 6030.\n");
    printf("  -u, --user=USER            User name. Default is root.\n");
    printf("  -c, --config-dir=CONFIG_DIR Configure directory.\n");
#ifdef WINDOWS
    printf("                             Default is C:\\TDengine\\cfg.\n");
#else
    printf("                             Default is /etc/taos.\n");
#endif
    printf("  -i, --inpath=INPATH        Input file path for restore.\n");
    printf("  -o, --outpath=OUTPATH      Output file path for backup.\n");
    printf("  -D, --databases=DATABASES  Databases to backup/restore. Use comma\n");
    printf("                             to separate names. Default is all.\n");
    printf("  -F, --format=FORMAT        Data file format: binary (default) or parquet\n");
    printf("  -M, --content=CONTENT      What to backup/restore:\n");
    printf("                                 basic    - super/child/normal tables meta and time-series data(default)\n");
    printf("                                 ext-meta - streams/virtual tables/topics meta\n");
    printf("                                 all      - basic + ext-meta\n");
    printf("  -v, --stmt-version=VER     Restore using STMT API version:\n");
    printf("                                 2 - STMT2 (default, faster)\n");
    printf("                                 1 - STMT1 (legacy).\n");
    printf("  -B, --data-batch=DATA_BATCH\n");
    printf("                             Number of rows per insert stmt. Restore only.\n");
    printf("                                 STMT2 (default): range [1, %d], default %d.\n", STMT2_BATCH_MAX, STMT2_BATCH_DEFAULT);
    printf("                                 STMT1 (-v 1):    range [1, %d], default %d.\n", STMT1_BATCH_MAX, STMT1_BATCH_DEFAULT);
    printf("  -s, --schemaonly           Only backup table schemas, no data.\n");
    printf("  -S, --start-time=START_TIME\n");
    printf("                             Start time to dump. epoch/iso8601 format is acceptable. Example:\n");
    printf("                                 epoch: 1800000000000\n");
    printf("                                 iso8601: 2025-10-01T00:00:00.000+0800\n");
    printf("  -E, --end-time=END_TIME    End   time to dump. epoch/iso8601 format is acceptable. Example:\n");
    printf("                                 epoch: 1802000000000\n");
    printf("                                 iso8601: 2025-11-01T00:00:00.000+0800\n");
    printf("  -T, --thread-num=THREAD_NUM\n");
    printf("                             Number of threads for data backup/restore.\n");
    printf("                             Default is 8.\n");
    printf("  -m, --tag-thread-num=THREAD_NUM\n");
    printf("                             Number of threads for tag backup.Default is 2.\n");
    printf("  -k, --retry-count=VALUE    Number of retry attempts. Default is 10.\n");
    printf("  -z, --retry-sleep-ms=VALUE Sleep between retries in ms. Default is 1000.\n");
    printf("  -W, --rename=RENAME-LIST   Rename database during restore.\n");
    printf("                             RENAME-LIST example:\n");
    printf("                             \"db1->newdb1|db2->newdb2|...\"\n");
    printf("  -X, --dsn=DSN              DSN to connect the cloud service.\n");
    printf("                             e.g. https://host?token=<TOKEN>\n");
    printf("                             Env var TDENGINE_CLOUD_DSN is also\n");
    printf("                             supported (option overrides env var).\n");
    printf("  -Z, --driver=DRIVER        Connect with native or websocket protocol, values:\n");
    printf("                                  Native (or 0)    - port 6030, faster. (Default)\n");
    printf("                                  WebSocket (or 1) - port 6041, no client driver install needed.\n");
    printf("                             When DSN is set, defaults to WebSocket.\n");
    printf("  -C, --checkpoint           Resume backup/restore from the last checkpoint\n");
    printf("                             (checkpoint files are always written; use -C\n");
    printf("                             to skip already-completed items on next run).\n");
    printf("  -g, --debug                Enable debug mode.\n");
    printf("      --help                 Give this help list.\n");
    printf("  -V, --version              Print program version.\n");
}

//
// ---------------- long option helpers ----------------
//

// Check if argv[i] matches a long option and extract value.
// Supports: --option=value  or  --option value (next argv)
// Returns the value string, or NULL if no match.
// Advances *pi past the consumed args.
static const char* matchLong(int argc, char *argv[], int *pi, const char *name, int needsValue) {
    int i = *pi;
    size_t nlen = strlen(name);

    if (strncmp(argv[i], name, nlen) != 0) return NULL;

    if (argv[i][nlen] == '=') {
        // --option=value
        return &argv[i][nlen + 1];
    }

    if (argv[i][nlen] == '\0') {
        if (!needsValue) return "";  // flag-only
        if (i + 1 < argc) {
            *pi = i + 1;
            return argv[i + 1];
        }
        return NULL;  // missing value
    }

    return NULL;  // no match
}

// Legal TDengine identifier: letters/digits/underscore, bounded length.
// Used to validate --rename target database names before they get spliced
// into restore SQL statements.
static bool isValidDbIdentifier(const char *name) {
    size_t len = strlen(name);
    if (len == 0 || len >= TSDB_DB_NAME_LEN) return false;
    for (size_t i = 0; i < len; i++) {
        char c = name[i];
        if (!isalnum((unsigned char)c) && c != '_') return false;
    }
    return true;
}

static bool isSeparatedPasswordArg(int argc, char *argv[], int index) {
    return (index + 1 < argc)
        && (strlen(argv[index + 1]) > 0)
        && (argv[index + 1][0] != '-');
}

static void printSeparatedPasswordArgError(const char *option, const char *password) {
    fprintf(stderr, "option %s does not accept a separated password argument: %s\n"
            "Use \"-p\"/\"--password\" to enter password interactively, "
            "or use \"-pPASSWORD\"/\"--password=PASSWORD\".\n",
            option, password);
}

static int readPasswordFromTerminal(void) {
    memset(g_password, 0, sizeof(g_password));
    (void)printf("Enter password: ");
    (void)taosSetConsoleEcho(false);
    if (scanf("%127s", g_password) != 1) {
        (void)taosSetConsoleEcho(true);
        (void)fprintf(stderr, "password reading error\n");
        return -1;
    }
    (void)taosSetConsoleEcho(true);
    (void)printf("\n");
    return 0;
}

//
// ---------------- DSN parsing ----------------
//
// Parse a TDengine cloud/websocket DSN:
//   https://host[:port]?key=value   (default port 443)
//   http://host[:port]?key=value    (default port 6041)
// Sets g_host, g_port, g_user, g_password from DSN components.
//
static void applyDsn(const char *dsn) {
    if (!dsn || dsn[0] == '\0') return;
    snprintf(g_dsn, sizeof(g_dsn), "%s", dsn);
    g_dsnMode = true;

    // default port by scheme
    int defaultPort = 443;
    if (strncasecmp(dsn, "http://", 7) == 0) defaultPort = 6041;

    // find "://"
    const char *after = strstr(dsn, "://");
    if (!after) {
        fprintf(stderr, "error: DSN missing '://', expected format: https://host[:port]?token=<TOKEN>\n");
        exit(-1);
    }
    after += 3;

    // work on mutable copy
    char tmp[2048];
    snprintf(tmp, sizeof(tmp), "%s", after);

    // split at '?' to get query string
    char *query = strchr(tmp, '?');
    char *userKey = NULL;
    char *userPwd = NULL;
    if (query) {
        *query = '\0';
        userKey = query + 1;
        // split "key=value"
        char *eq = strchr(userKey, '=');
        if (eq) {
            *eq    = '\0';
            userPwd = eq + 1;
        }
    }

    // split host at ':' for explicit port
    char *colon = strchr(tmp, ':');
    if (colon) {
        *colon = '\0';
        int portVal = atoi(colon + 1);
        if (portVal > 0) g_port = portVal;
    } else {
        g_port = defaultPort;
    }

    snprintf(g_host, sizeof(g_host), "%s", tmp);
    if (userKey && userKey[0]) snprintf(g_user,     sizeof(g_user),     "%s", userKey);
    if (userPwd && userPwd[0]) snprintf(g_password, sizeof(g_password), "%s", userPwd);
}

//
// ---------------- parse databases ----------------
//

static void parseDatabases(const char *dbStr) {
    // free previous
    for (int i = 0; i < g_dbCount; i++) {
        taosMemoryFree(g_dbs[i]);
        g_dbs[i] = NULL;
    }
    g_dbCount = 0;

    // parse comma-separated list
    char *copy = taosStrdup(dbStr);
    char *token = strtok(copy, ",");
    while (token != NULL && g_dbCount < MAX_DBS) {
        // trim spaces
        while (*token == ' ') token++;
        char *end = token + strlen(token) - 1;
        while (end > token && *end == ' ') *end-- = '\0';

        if (strlen(token) > 0) {
            g_dbs[g_dbCount] = taosStrdup(token);
            g_dbCount++;
        }
        token = strtok(NULL, ",");
    }
    g_dbs[g_dbCount] = NULL;
    taosMemoryFree(copy);
}

//
// ---------------- outpath normalization ----------------
//

//
// Normalize a user-supplied output/input path so that different spellings of
// the same directory map to the same string.  This is required because the
// restore checkpoint (restoreCkpt.c) matches data-file paths with exact
// strcmp() against the strings recorded in restore_checkpoint.txt: if the
// user passes the same directory as "dir/" on one run and "dir" on the next,
// the recorded keys differ (e.g. "dir//db/..." vs "dir/db/...") and -C resume
// silently re-restores every file instead of skipping them.
//
// Rules:
//   1. collapse consecutive '/' (and '\' on Windows) into a single '/';
//   2. a leading "//" (or "\\") is preserved as-is so UNC paths keep working;
//   3. trailing separators are stripped, except a bare root "/" is kept.
//
static void normalizeOutPath(char *out) {
    char *src = out;
    char *dst = out;

    // preserve a leading "//" or "\\" (UNC); count consecutive leading seps
    int leadingSeps = 0;
    while (*src == '/'
#ifdef WINDOWS
           || *src == '\\'
#endif
    ) {
        leadingSeps++;
        src++;
    }
    int emitLeading = (leadingSeps >= 2) ? 2 : (leadingSeps == 1 ? 1 : 0);
    while (emitLeading-- > 0) *dst++ = '/';

    bool prevSep = false;
    while (*src) {
        bool isSep = (*src == '/');
#ifdef WINDOWS
        isSep = isSep || (*src == '\\');
#endif
        if (isSep) {
            prevSep = true;
            src++;
            continue;
        }
        if (prevSep) {
            *dst++ = '/';
            prevSep = false;
        }
        *dst++ = *src++;
    }
    *dst = '\0';
}

static void setOutPath(const char *val) {
    snprintf(g_outPath, sizeof(g_outPath), "%s", val);
    // Different spellings of the same directory ("dir/" vs "dir") must
    // produce byte-identical checkpoint keys, otherwise restore -C resume
    // (exact strcmp matching in restoreCkpt.c) silently re-restores every
    // file when the -i path spelling changes between runs.
    normalizeOutPath(g_outPath);
}

//
// ---------------- checkpoint format check ----------------
//
// -C resume skips files by existence using the CURRENT extension (.dat/.par).
// Changing format mid-resume would mix both extensions, and restore accepts
// both → duplicate data.  Every run logs its format to {outPath}/backup.log
// ("  Format       : binary|parquet"); -C runs append, so the LAST such line is
// the most recent run's format.  Returns 1 if it differs from this run's
// format (resume must refuse), 0 if it matches or no line is found.  On a
// found line, `prevName` (prevNameSz bytes) is filled with "binary"/"parquet".
#define BCK_LOG_SCAN_CHUNK 65536   // bytes per read
#define BCK_LOG_CARRY      64      // tail kept across chunk boundary
static int backupFormatChangedSinceLastRun(char *prevName, int prevNameSz) {
    char logPath[MAX_PATH_LEN];
    snprintf(logPath, sizeof(logPath), "%s/backup.log", g_outPath);

    if (!taosCheckExistFile(logPath)) {
        return 0;
    }
    TdFilePtr fp = taosOpenFile(logPath, TD_FILE_READ);
    if (fp == NULL) {
        return 0;
    }

    // Scan in chunks, keep the LAST match (most recent run).
    char   lastFmt[16] = {0};
    bool   found = false;
    char   buf[BCK_LOG_SCAN_CHUNK + BCK_LOG_CARRY + 1] = {0};
    int64_t carry = 0;

    for (;;) {
        int64_t n = taosReadFile(fp, buf + carry, BCK_LOG_SCAN_CHUNK);
        if (n <= 0) break;
        int64_t total = carry + n;
        buf[total] = '\0';

        const char *p = buf;
        while ((p = strstr(p, "Format")) != NULL) {
            const char *colon = strchr(p, ':');
            if (colon && colon - p < 16) {
                const char *q = colon + 1;
                while (*q == ' ' || *q == '\t') q++;
                if (strncmp(q, "binary", 6) == 0 &&
                    (q[6] == '\0' || q[6] == '\n' || q[6] == '\r' || q[6] == ' ')) {
                    snprintf(lastFmt, sizeof(lastFmt), "binary");
                    found = true;
                } else if (strncmp(q, "parquet", 7) == 0 &&
                           (q[7] == '\0' || q[7] == '\n' || q[7] == '\r' || q[7] == ' ')) {
                    snprintf(lastFmt, sizeof(lastFmt), "parquet");
                    found = true;
                }
            }
            p++;
        }

        // keep the tail so a marker straddling the boundary is still seen
        carry = (total > BCK_LOG_CARRY) ? BCK_LOG_CARRY : total;
        memmove(buf, buf + total - carry, carry);
    }
    taosCloseFile(&fp);

    if (!found) {
        return 0;
    }

    if (prevName && prevNameSz > 0) {
        snprintf(prevName, prevNameSz, "%s", lastFmt);
    }

    const char *cur = (g_storageFormat == BINARY_PARQUET) ? "parquet" : "binary";
    return strcmp(lastFmt, cur) != 0;
}

//
// ---------------- interface ----------------
//

int argsInit(int argc, char *argv[]) {
    int hasOutput = 0;
    int hasInput  = 0;
    const char *val = NULL;

    // apply TDENGINE_CLOUD_DSN env var first; command-line -X overrides it
    {
        const char *envDsn = getenv("TDENGINE_CLOUD_DSN");
        if (envDsn && envDsn[0]) {
            applyDsn(envDsn);
        }
    }

    for (int i = 1; i < argc; i++) {
        // ---- outpath ----
        if ((strcmp(argv[i], "-o") == 0 && i + 1 < argc && (val = argv[++i])) ||
            (val = matchLong(argc, argv, &i, "--outpath", 1))) {
            setOutPath(val);
            g_action = ACTION_BACKUP;
            hasOutput = 1;
        }
        // ---- inpath ----
        else if ((strcmp(argv[i], "-i") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--inpath", 1))) {
            setOutPath(val);
            g_action = ACTION_RESTORE;
            hasInput = 1;
        }
        // ---- databases ----
        else if ((strcmp(argv[i], "-D") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--databases", 1))) {
            parseDatabases(val);
        }
        // ---- format ----
        else if ((strcmp(argv[i], "-F") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--format", 1))) {
            if (strcasecmp(val, "binary") == 0) {
                g_storageFormat = BINARY_TAOS;
            } else if (strcasecmp(val, "parquet") == 0) {
                g_storageFormat = BINARY_PARQUET;
            } else {
                printf("error: unknown format: %s\n", val);
                printUsage(argv[0]);
                return -1;
            }
        }
        // ---- content ----
        else if ((strcmp(argv[i], "-M") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--content", 1))) {
            if (strcasecmp(val, "basic") == 0) {
                g_content = BCK_CONTENT_BASIC;
            } else if (strcasecmp(val, "ext-meta") == 0) {
                g_content = BCK_CONTENT_EXTMETA;
            } else if (strcasecmp(val, "all") == 0) {
                g_content = BCK_CONTENT_ALL;
            } else {
                printf("error: unknown content: %s (use basic, ext-meta or all)\n", val);
                printUsage(argv[0]);
                return -1;
            }
        }
        // ---- data-batch ----
        else if ((strcmp(argv[i], "-B") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--data-batch", 1))) {
            int bval = atoi(val);
            // Accept the full valid range here; STMT-version-specific upper bound
            // is checked in the post-parse validation step below.
            if (bval < 1 || bval > STMT1_BATCH_MAX) {
                printf("error: --data-batch must be in range [1, %d], got: %s\n",
                       STMT1_BATCH_MAX, val);
                printUsage(argv[0]);
                return -1;
            }
            g_dataBatch = bval;
        }
        // ---- stmt-version ----
        else if ((strcmp(argv[i], "-v") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--stmt-version", 1))) {
            if (strcmp(val, "1") == 0 || strcasecmp(val, "stmt1") == 0) {
                g_stmtVersion = STMT_VERSION_1;
                g_stmtVersionSet = true;
            } else if (strcmp(val, "2") == 0 || strcasecmp(val, "stmt2") == 0) {
                g_stmtVersion = STMT_VERSION_2;
                g_stmtVersionSet = true;
            } else {
                printf("error: unknown stmt-version: %s (use 1 or 2)\n", val);
                printUsage(argv[0]);
                return -1;
            }
        }
        // ---- thread-num ----
        else if ((strcmp(argv[i], "-T") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--thread-num", 1))) {
            g_dataThread = atoi(val);
            if (g_dataThread < 1) {
                printf("error: --thread-num must be >= 1, got: %s\n", val);
                printUsage(argv[0]);
                return -1;
            }
        }
        // ---- tag-thread-num ----
        else if ((strcmp(argv[i], "-m") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--tag-thread-num", 1))) {
            g_tagThread = atoi(val);
            if (g_tagThread < 1) {
                printf("error: --tag-thread-num must be >= 1, got: %s\n", val);
                printUsage(argv[0]);
                return -1;
            }
        }
        // ---- host ----
        else if ((strcmp(argv[i], "-h") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--host", 1))) {
            snprintf(g_host, sizeof(g_host), "%s", val);
        }
        // ---- port ----
        else if ((strcmp(argv[i], "-P") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--port", 1))) {
            int pval = atoi(val);
            if (pval < 1 || pval > 65535) {
                printf("error: --port must be in range [1, 65535], got: %s\n", val);
                printUsage(argv[0]);
                return -1;
            }
            g_port = pval;
        }
        // ---- user ----
        else if ((strcmp(argv[i], "-u") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--user", 1))) {
            snprintf(g_user, sizeof(g_user), "%s", val);
        }
        else if (strlen(argv[i]) > strlen("-u")
                 && strncmp(argv[i], "-u", strlen("-u")) == 0) {
            snprintf(g_user, sizeof(g_user), "%s", argv[i] + strlen("-u"));
        }
        // ---- password ----
        else if (strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--password") == 0) {
            if (isSeparatedPasswordArg(argc, argv, i)) {
                printSeparatedPasswordArgError(argv[i], argv[i + 1]);
                return -1;
            }
            if (readPasswordFromTerminal() != 0) {
                return -1;
            }
        }
        else if (strncmp(argv[i], "-p", strlen("-p")) == 0) {
            snprintf(g_password, sizeof(g_password), "%s", argv[i] + strlen("-p"));
        }
        else if (strncmp(argv[i], "--password=", strlen("--password=")) == 0) {
            snprintf(g_password, sizeof(g_password), "%s", argv[i] + strlen("--password="));
        }
        // ---- start-time ----
        else if ((strcmp(argv[i], "-S") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--start-time", 1))) {
            snprintf(g_startTime, sizeof(g_startTime), "%s", val);
        }
        // ---- end-time ----
        else if ((strcmp(argv[i], "-E") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--end-time", 1))) {
            snprintf(g_endTime, sizeof(g_endTime), "%s", val);
        }
        // ---- schemaonly ----
        else if (strcmp(argv[i], "-s") == 0 || matchLong(argc, argv, &i, "--schemaonly", 0)) {
            g_schemaOnly = 1;
        }
        // ---- config-dir ----
        else if ((strcmp(argv[i], "-c") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--config-dir", 1))) {
            snprintf(g_configDir, sizeof(g_configDir), "%s", val);
        }
        // ---- debug ----
        else if (strcmp(argv[i], "-g") == 0 || matchLong(argc, argv, &i, "--debug", 0)) {
            g_debug = 1;
        }
        // ---- checkpoint ----
        else if (strcmp(argv[i], "-C") == 0 || matchLong(argc, argv, &i, "--checkpoint", 0)) {
            g_checkpoint = 1;
        }
        // ---- retry-count ----
        else if ((strcmp(argv[i], "-k") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--retry-count", 1))) {
            g_retryCount = atoi(val);
            if (g_retryCount < 0) {
                printf("error: --retry-count must be >= 0, got: %s\n", val);
                printUsage(argv[0]);
                return -1;
            }
        }
        // ---- retry-sleep-ms ----
        else if ((strcmp(argv[i], "-z") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--retry-sleep-ms", 1))) {
            g_retrySleepMs = atoi(val);
            if (g_retrySleepMs < 0) {
                printf("error: --retry-sleep-ms must be >= 0, got: %s\n", val);
                printUsage(argv[0]);
                return -1;
            }
        }
        // ---- rename ----
        else if ((strcmp(argv[i], "-W") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--rename", 1))) {
            // build display string: replace '=' with '->' (e.g. "db1->db2")
            { int j = 0;
              for (int k = 0; val[k] && j < (int)sizeof(g_renameRaw) - 3; k++) {
                  if (val[k] == '=') { g_renameRaw[j++] = '-'; g_renameRaw[j++] = '>'; }
                  else               { g_renameRaw[j++] = val[k]; }
              }
              g_renameRaw[j] = '\0'; }
            // parse "db1=newDB1|db2=newDB2" or "db1->newDB1|db2->newDB2"
            char *copy = taosStrdup(val);
            char *pair = strtok(copy, "|");
            while (pair != NULL && g_renameCount < MAX_RENAME) {
                // prefer "->" separator, fall back to "="
                char *sep = strstr(pair, "->");
                char *oldN, *newN;
                if (sep) {
                    *sep = '\0';
                    oldN = pair;
                    newN = sep + 2;
                } else {
                    char *eq = strchr(pair, '=');
                    if (!eq) { pair = strtok(NULL, "|"); continue; }
                    *eq = '\0';
                    oldN = pair;
                    newN = eq + 1;
                }
                // trim spaces
                while (*oldN == ' ') oldN++;
                while (*newN == ' ') newN++;
                char *e1 = oldN + strlen(oldN) - 1;
                while (e1 > oldN && *e1 == ' ') *e1-- = '\0';
                char *e2 = newN + strlen(newN) - 1;
                while (e2 > newN && *e2 == ' ') *e2-- = '\0';
                if (strlen(oldN) > 0 && strlen(newN) > 0) {
                    if (!isValidDbIdentifier(newN)) {
                        printf("error: --rename target database name is invalid: %s\n", newN);
                        printUsage(argv[0]);
                        taosMemoryFree(copy);
                        return -1;
                    }
                    g_renameOld[g_renameCount] = taosStrdup(oldN);
                    g_renameNew[g_renameCount] = taosStrdup(newN);
                    g_renameCount++;
                }
                pair = strtok(NULL, "|");
            }
            taosMemoryFree(copy);
        }
        // ---- dsn ----
        else if ((strcmp(argv[i], "-X") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--dsn", 1))) {
            applyDsn(val);
        }
        // ---- driver ----
        else if ((strcmp(argv[i], "-Z") == 0 && i + 1 < argc && (val = argv[++i])) ||
                 (val = matchLong(argc, argv, &i, "--driver", 1))) {
            // reuse exact same logic as getConnMode() in pub.c / taosdump
            if (strcasecmp(val, "native") == 0 || strcmp(val, "0") == 0) {
                g_driver = CONN_MODE_NATIVE;
            } else if (strcasecmp(val, "websocket") == 0 || strcmp(val, "1") == 0) {
                g_driver = CONN_MODE_WEBSOCKET;
            } else {
                fprintf(stderr, "invalid input %s for option -Z, only support: Native (0) or WebSocket (1)\n", val);
                exit(-1);
            }
        }
        // ---- version ----
        else if (strcmp(argv[i], "-V") == 0 || matchLong(argc, argv, &i, "--version", 0)) {
            printVersion(true);
            exit(0);
        }
        // ---- help ----
        else if (matchLong(argc, argv, &i, "--help", 0)) {
            printUsage(argv[0]);
            exit(0);
        }
        // ---- positional args: dbname [tbname ...] ----
        else if (argv[i][0] != '-') {
            if (g_specDb[0] == '\0') {
                // first positional arg = database name
                snprintf(g_specDb, sizeof(g_specDb), "%s", argv[i]);
            } else if (g_specTableCount < MAX_SPEC_TABLES) {
                // subsequent positional args = table names
                g_specTables[g_specTableCount] = taosStrdup(argv[i]);
                if (g_specTables[g_specTableCount] == NULL) {
                    printf("error: out of memory\n");
                    return -1;
                }
                g_specTableCount++;
                g_specTables[g_specTableCount] = NULL;
            }
        }
        else {
            printf("unknown option: %s\n", argv[i]);
            printUsage(argv[0]);
            return -1;
        }
    }

    // validate
    if (!hasOutput && !hasInput) {
        printf("error: must specify -o (backup) or -i (restore)\n");
        printUsage(argv[0]);
        return -1;
    }

    // cross-validate restore-only options against backup mode
    if (g_action == ACTION_BACKUP) {
        if (g_dataBatch > 0) {
            printf("error: --data-batch is for restore only, not allowed with -o\n");
            printUsage(argv[0]);
            return -1;
        }
        if (g_stmtVersionSet) {
            printf("error: --stmt-version is for restore only, not allowed with -o\n");
            printUsage(argv[0]);
            return -1;
        }
        if (g_renameCount > 0) {
            printf("error: --rename is for restore only, not allowed with -o\n");
            printUsage(argv[0]);
            return -1;
        }

        // -C resume must reuse the previous backup's format: a different
        // format would leave a mixed .dat/.par output → restore duplicates.
        if (g_checkpoint) {
            char prevFmt[32] = {0};
            if (backupFormatChangedSinceLastRun(prevFmt, sizeof(prevFmt))) {
                printf("error: -C/--checkpoint cannot resume: data format changed.\n"
                       "       Previous backup format: %s, current format: %s.\n"
                       "       Resume requires the same -F/--format, or delete the\n"
                       "       output directory and run a fresh backup.\n",
                       prevFmt[0] ? prevFmt : "(unknown)",
                       (g_storageFormat == BINARY_PARQUET) ? "parquet" : "binary");
                printUsage(argv[0]);
                return -1;
            }
        }
    }

    // cross-validate backup-only options against restore mode
    if (g_action == ACTION_RESTORE) {
        if (g_startTime[0] || g_endTime[0]) {
            printf("error: --start-time/--end-time is for backup only, not allowed with -i\n");
            printUsage(argv[0]);
            return -1;
        }
    }

    // cross-validate --data-batch against the selected --stmt-version (restore only)
    if (g_dataBatch > 0) {
        int maxBatch = (g_stmtVersion == STMT_VERSION_2) ? STMT2_BATCH_MAX : STMT1_BATCH_MAX;
        if (g_dataBatch > maxBatch) {
            printf("error: --data-batch=%d exceeds the maximum %d for STMT%d.\n"
                   "       Use -v 1 / --stmt-version=1 to raise the limit to %d.\n",
                   g_dataBatch, maxBatch, (int)g_stmtVersion, STMT1_BATCH_MAX);
            printUsage(argv[0]);
            return -1;
        }
    }

    // positional dbname and -D/--databases are mutually exclusive.
    // Using both at the same time is ambiguous — which databases to operate on?
    if (g_specDb[0] != '\0') {
        if (g_dbCount > 0) {
            printf("error: cannot specify both -D/--databases and a positional database name.\n"
                   "       Use either -D db1,db2,...  OR  dbname [tbname ...], not both.\n");
            printUsage(argv[0]);
            return -1;
        }
        g_dbCount = 1;
        g_dbs[0] = taosStrdup(g_specDb);
        g_dbs[1] = NULL;
    }

    // build time filter
    if (g_startTime[0] || g_endTime[0]) {
        // helper: if all digits, use as-is (epoch); otherwise wrap in quotes (ISO8601)
        char sBuf[80] = "", eBuf[80] = "";
        if (g_startTime[0]) {
            int isEpoch = 1;
            for (const char *p = g_startTime; *p; p++) { if (*p < '0' || *p > '9') { isEpoch = 0; break; } }
            if (isEpoch) snprintf(sBuf, sizeof(sBuf), "%s", g_startTime);
            else         snprintf(sBuf, sizeof(sBuf), "'%s'", g_startTime);
        }
        if (g_endTime[0]) {
            int isEpoch = 1;
            for (const char *p = g_endTime; *p; p++) { if (*p < '0' || *p > '9') { isEpoch = 0; break; } }
            if (isEpoch) snprintf(eBuf, sizeof(eBuf), "%s", g_endTime);
            else         snprintf(eBuf, sizeof(eBuf), "'%s'", g_endTime);
        }

        // _c0 is the primary-key timestamp pseudo-column - always the first
        // column regardless of its actual name, so this filter works even
        // when a stable's time column isn't literally called "ts".
        if (g_startTime[0] && g_endTime[0]) {
            snprintf(g_timeFilter, sizeof(g_timeFilter), "WHERE _c0 >= %s AND _c0 <= %s", sBuf, eBuf);
        } else if (g_startTime[0]) {
            snprintf(g_timeFilter, sizeof(g_timeFilter), "WHERE _c0 >= %s", sBuf);
        } else {
            snprintf(g_timeFilter, sizeof(g_timeFilter), "WHERE _c0 <= %s", eBuf);
        }
    }

    return 0;
}

void argsDestroy() {
    for (int i = 0; i < g_dbCount; i++) {
        taosMemoryFree(g_dbs[i]);
        g_dbs[i] = NULL;
    }
    g_dbCount = 0;
    for (int i = 0; i < g_renameCount; i++) {
        taosMemoryFree(g_renameOld[i]);
        taosMemoryFree(g_renameNew[i]);
        g_renameOld[i] = NULL;
        g_renameNew[i] = NULL;
    }
    g_renameCount = 0;
    for (int i = 0; i < g_specTableCount; i++) {
        taosMemoryFree(g_specTables[i]);
        g_specTables[i] = NULL;
    }
    g_specTableCount = 0;
    g_specDb[0] = '\0';
}


//
// -------------------- get args ----------------
//

enum ActionType argAction() {
    return g_action;
}

int argRetryCount() {
    return g_retryCount;
}

// ms
int argRetrySleepMs() {
    return g_retrySleepMs;
}

char** argBackDB() {
    return g_dbs;
}

char* argOutPath() {
    return g_outPath;
}

StorageFormat argStorageFormat() {
    return g_storageFormat;
}

BckContent argContent() {
    return g_content;
}

bool argContentBasic() {
    return (g_content & BCK_CONTENT_BASIC) != 0;
}

bool argContentExtMeta() {
    return (g_content & BCK_CONTENT_EXTMETA) != 0;
}

const char* argContentName() {
    switch (g_content) {
        case BCK_CONTENT_BASIC:   return "basic";
        case BCK_CONTENT_EXTMETA: return "ext-meta";
        case BCK_CONTENT_ALL:     return "all";
        default:                  return "unknown";
    }
}

int argTagThread() {
    return g_tagThread;
}

int argDataThread() {
    return g_dataThread;
}

char* argTimeFilter() {
    if (g_timeFilter[0] == '\0') return NULL;
    return g_timeFilter;
}

char* argStartTime() {
    return g_startTime[0] ? g_startTime : NULL;
}

char* argEndTime() {
    return g_endTime[0] ? g_endTime : NULL;
}

int argSchemaOnly() {
    return g_schemaOnly;
}

int argCheckpoint() {
    return g_checkpoint;
}

int argDebug() {
    return g_debug;
}

char* argHost() {
    return g_host[0] ? g_host : NULL;
}

int argPort() {
    return g_port;
}

char* argUser() {
    return g_user;
}

char* argPassword() {
    return g_password;
}

const char* argRenameDb(const char *oldName) {
    for (int i = 0; i < g_renameCount; i++) {
        if (strcmp(g_renameOld[i], oldName) == 0) {
            return g_renameNew[i];
        }
    }
    return oldName;
}

int argRenameCount(void) {
    return g_renameCount;
}

const char* argRenameOldAt(int i) {
    return (i >= 0 && i < g_renameCount) ? g_renameOld[i] : NULL;
}

const char* argRenameNewAt(int i) {
    return (i >= 0 && i < g_renameCount) ? g_renameNew[i] : NULL;
}

const char* argRenameList() {
    if (g_renameRaw[0] == '\0') return NULL;
    return g_renameRaw;
}

StmtVersion argStmtVersion() {
    return g_stmtVersion;
}

// DSN / cloud connection
const char* argDsn() {
    return g_dsn;
}

bool argIsDsn() {
    return g_dsnMode;
}

int8_t argDriver() {
    return g_driver;
}

const char* argConfigDir() {
    return g_configDir;
}

// positional spec: dbname [tbname ...]
const char* argSpecDb() {
    return g_specDb[0] ? g_specDb : NULL;
}

char** argSpecTables() {
    return g_specTableCount > 0 ? g_specTables : NULL;
}

int argSpecTablesCount() {
    return g_specTableCount;
}

// Returns true if stbName itself is in the spec-tables list.
// Used to detect "user requested whole STB" vs "user requested specific CTBs".
bool argStbNameInSpecTables(const char *stbName) {
    if (!stbName) return false;
    char **specTbs = argSpecTables();
    if (!specTbs) return false;
    for (int i = 0; specTbs[i] != NULL; i++) {
        if (strcmp(stbName, specTbs[i]) == 0) return true;
    }
    return false;
}

bool argTableInSpecTables(const char *tbName) {
    // reuses the same list/comparison as argStbNameInSpecTables(); kept as a
    // separate name because the caller's intent differs (plain table-name
    // membership check vs. "is this STB itself requested wholesale").
    return argStbNameInSpecTables(tbName);
}

//
// Build SQL IN clause fragment for spec tables.
// e.g. argBuildInClause("table_name", buf, len) -> "table_name IN ('d1','d2')"
// Returns number of chars written, or 0 if no spec tables.
//
int argBuildInClause(const char *colName, char *buf, int bufLen) {
    char **specTables = argSpecTables();
    if (!specTables || !specTables[0]) return 0;

    int off = snprintf(buf, bufLen, "%s IN (", colName);
    for (int i = 0; specTables[i] != NULL; i++) {
        if (i > 0) off += snprintf(buf + off, bufLen - off, ",");
        off += snprintf(buf + off, bufLen - off, "'%s'", specTables[i]);
    }
    off += snprintf(buf + off, bufLen - off, ")");
    return off;
}

int argDataBatch() {
    return g_dataBatch;
}
