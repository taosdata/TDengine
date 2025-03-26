/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <ctype.h>
#include <bench.h>
#include "benchLog.h"
#include "pub.h"

char resEncodingChunk[] = "Encoding: chunked";
char succMessage[] = "succ";
char resHttp[] = "HTTP/1.1 ";
char resHttpOk[] = "HTTP/1.1 200 OK";
char influxHttpOk[] = "HTTP/1.1 204";
char opentsdbHttpOk[] = "HTTP/1.1 400";

FORCE_INLINE void* benchCalloc(size_t nmemb, size_t size, bool record) {
    void* ret = calloc(nmemb, size);
    if (NULL == ret) {
        errorPrint("%s", "failed to allocate memory\n");
        exit(EXIT_FAILURE);
    }
    if (record) {
        g_memoryUsage += nmemb * size;
    }
    return ret;
}

FORCE_INLINE void tmfclose(FILE *fp) {
    if (NULL != fp) {
        fclose(fp);
        fp = NULL;
    }
}

FORCE_INLINE void tmfree(void *buf) {
    if (NULL != buf) {
        free(buf);
    }
}

void engineError(char * module, char * fun, int32_t code) {
    errorPrint("%s API:%s error code:0x%08X %s\n", TIP_ENGINE_ERR, fun, code, module);
}

void ERROR_EXIT(const char *msg) {
    errorPrint("%s", msg);
    exit(EXIT_FAILURE);
}

#ifdef WINDOWS
HANDLE g_stdoutHandle;
DWORD  g_consoleMode;

void setupForAnsiEscape(void) {
    DWORD mode = 0;
    g_stdoutHandle = GetStdHandle(STD_OUTPUT_HANDLE);

    if (g_stdoutHandle == INVALID_HANDLE_VALUE) {
        exit(GetLastError());
    }

    if (!GetConsoleMode(g_stdoutHandle, &mode)) {
        exit(GetLastError());
    }

    g_consoleMode = mode;

    // Enable ANSI escape codes
    mode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;

    if (!SetConsoleMode(g_stdoutHandle, mode)) {
        exit(GetLastError());
    }
}

void resetAfterAnsiEscape(void) {
    // Reset colors
    printf("\x1b[0m");

    // Reset console mode
    if (!SetConsoleMode(g_stdoutHandle, g_consoleMode)) {
        exit(GetLastError());
    }
}

unsigned int taosRandom() {
    unsigned int number;
    rand_s(&number);

    return number;
}
#else  // Not windows
void setupForAnsiEscape(void) {}

void resetAfterAnsiEscape(void) {
    // Reset colors
    printf("\x1b[0m");
}

FORCE_INLINE unsigned int taosRandom() { return (unsigned int)rand(); }
#endif

void swapItem(char** names, int32_t i, int32_t j ) {
    debugPrint("swap item i=%d (%s) j=%d (%s)\n", i, names[i], j, names[j]);
    char * p = names[i];
    names[i] = names[j];
    names[j] = p;
}

int getAllChildNameOfSuperTable(TAOS *taos, char *dbName, char *stbName,
        char ** childTblNameOfSuperTbl,
        int64_t childTblCountOfSuperTbl) {
    char cmd[SHORT_1K_SQL_BUFF_LEN] = "\0";
    snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
             "select distinct tbname from %s.`%s` limit %" PRId64,
            dbName, stbName, childTblCountOfSuperTbl);
    TAOS_RES *res = taos_query(taos, cmd);
    int32_t   code = taos_errno(res);
    int64_t   count = 0;
    if (code) {
        printErrCmdCodeStr(cmd, code, res);
        return -1;
    }
    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (0 == strlen((char *)(row[0]))) {
            errorPrint("No.%" PRId64 " table return empty name\n",
                    count);
            return -1;
        }
        int32_t * lengths = taos_fetch_lengths(res);
        childTblNameOfSuperTbl[count] =
            benchCalloc(1, TSDB_TABLE_NAME_LEN + 3, true);
        childTblNameOfSuperTbl[count][0] = '`';
        strncpy(childTblNameOfSuperTbl[count] + 1, row[0], lengths[0]);
        childTblNameOfSuperTbl[count][lengths[0] + 1] = '`';
        childTblNameOfSuperTbl[count][lengths[0] + 2] = '\0';
        debugPrint("childTblNameOfSuperTbl[%" PRId64 "]: %s\n", count,
                childTblNameOfSuperTbl[count]);
        count++;
    }
    taos_free_result(res);

    // random swap order
    if (count < 4) {
        return 0;
    }

    int32_t swapCnt = count/2;
    for(int32_t i = 0; i < swapCnt; i++ ) {
        int32_t j = swapCnt + RD(swapCnt);
        swapItem(childTblNameOfSuperTbl, i, j);
    }
    return 0;
}

void prompt(bool nonStopMode) {
    if (!g_arguments->answer_yes) {
        g_arguments->in_prompt = true;
        if (nonStopMode) {
            printf(
                    "\n\n         Current is the Non-Stop insertion mode. "
                    "benchmark will continuously "
                    "insert data unless you press "
                    "Ctrl-C to end it.\n\n         "
                    "press enter key to continue and "
                    "Ctrl-C to "
                    "stop\n\n");
            (void)getchar();
        } else {
            printf(
                    "\n\n         Press enter key to continue or Ctrl-C to "
                    "stop\n\n");
            (void)getchar();
        }
        g_arguments->in_prompt = false;
    }
}

static void appendResultBufToFile(char *resultBuf, char * filePath) {
    FILE* fp = fopen(filePath, "at");
    if (fp == NULL) {
        errorPrint(
                "failed to open result file: %s, result will not save "
                "to file\n", filePath);
        return;
    }
    fprintf(fp, "%s", resultBuf);
    tmfclose(fp);
}

int32_t replaceChildTblName(char *inSql, char *outSql, int tblIndex) {
    // child table mark
    char mark[32] = "xxxx";
    char *pos = strstr(inSql, mark);
    if (0 == pos) {
        errorPrint("sql format error, sql not found mark string '%s'", mark);
        return -1;
    }        

    char subTblName[TSDB_TABLE_NAME_LEN];
    snprintf(subTblName, TSDB_TABLE_NAME_LEN,
            "`%s`.%s", g_queryInfo.dbName,
            g_queryInfo.superQueryInfo.childTblName[tblIndex]);

    tstrncpy(outSql, inSql, pos - inSql + 1);
    snprintf(outSql + (pos - inSql), TSDB_MAX_ALLOWED_SQL_LEN - 1,
             "%s%s", subTblName, pos + strlen(mark));
    return 0;         
}

int64_t toolsGetTimestamp(int32_t precision) {
    if (precision == TSDB_TIME_PRECISION_MICRO) {
        return toolsGetTimestampUs();
    } else if (precision == TSDB_TIME_PRECISION_NANO) {
        return toolsGetTimestampNs();
    } else {
        return toolsGetTimestampMs();
    }
}

int regexMatch(const char *s, const char *reg, int cflags) {
    regex_t regex;
    char    msgbuf[100] = {0};

    /* Compile regular expression */
    if (regcomp(&regex, reg, cflags) != 0)
        ERROR_EXIT("Failed to regex compile\n");

    /* Execute regular expression */
    int reti = regexec(&regex, s, 0, NULL, 0);
    if (!reti) {
        regfree(&regex);
        return 1;
    } else if (reti == REG_NOMATCH) {
        regfree(&regex);
        return 0;
    } else {
        regerror(reti, &regex, msgbuf, sizeof(msgbuf));
        regfree(&regex);
        printf("Regex match failed: %s\n", msgbuf);
        exit(EXIT_FAILURE);
    }
    return 0;
}

SBenchConn* initBenchConnImpl() {
    SBenchConn* conn = benchCalloc(1, sizeof(SBenchConn), true);
    char     show[256] = "\0";
    char *   host = NULL;
    uint16_t port = 0;
    char *   user = NULL;
    char *   pwd  = NULL;
    int32_t  code = 0;
    char *   dsnc = NULL;

    // set mode
    if (g_arguments->connMode != CONN_MODE_NATIVE && g_arguments->dsn) {
        dsnc = strToLowerCopy(g_arguments->dsn);
        if (dsnc == NULL) {
            tmfree(conn);
            return NULL;
        }

        char *cport = NULL;
        char error[512] = "\0";
        code = parseDsn(dsnc, &host, &cport, &user, &pwd, error);
        if (code) {
            errorPrint("%s dsn=%s\n", error, dsnc);
            tmfree(conn);
            tmfree(dsnc);
            return NULL;
        }

        // default ws port
        if (cport == NULL) {
            if (user)
                port = DEFAULT_PORT_WS_CLOUD;
            else
                port = DEFAULT_PORT_WS_LOCAL;
        } else {
            port = atoi(cport);
        }

        // websocket
        memcpy(show, g_arguments->dsn, 20);
        memcpy(show + 20, "...", 3);
        memcpy(show + 23, g_arguments->dsn + strlen(g_arguments->dsn) - 10, 10);

    } else {

        host = g_arguments->host;
        user = g_arguments->user;
        pwd  = g_arguments->password;

        if (g_arguments->port_inputted) {
            port = g_arguments->port;
        } else {
            port = defaultPort(g_arguments->connMode, g_arguments->dsn);
        }

        sprintf(show, "host:%s port:%d ", host, port);
    }

    // connect main
    conn->taos = taos_connect(host, user, pwd, NULL, port);
    if (conn->taos == NULL) {
        errorPrint("failed to connect %s:%d, "
                    "code: 0x%08x, reason: %s\n",
                g_arguments->host, g_arguments->port,
                taos_errno(NULL), taos_errstr(NULL));
        tmfree(conn);
        if (dsnc) {
            tmfree(dsnc);
        }
        return NULL;
    }
    succPrint("%s connect successfully.\n", show);

    // check write correct connect
    conn->ctaos = taos_connect(host, user, pwd, NULL, port);

    if (dsnc) {
        tmfree(dsnc);
    }
    return conn;
}

SBenchConn* initBenchConn() {

    SBenchConn* conn = NULL;
    int32_t keep_trying = 0;
    while(1) {
        conn = initBenchConnImpl();
        if(conn || ++keep_trying > g_arguments->keep_trying  || g_arguments->terminate) {
            break;
        }

        infoPrint("sleep %dms and try to connect... %d/%d  \n", g_arguments->trying_interval, keep_trying, g_arguments->keep_trying);
        if(g_arguments->trying_interval > 0) {
            toolsMsleep(g_arguments->trying_interval);
        }        
    } 

    return conn;
}

void closeBenchConn(SBenchConn* conn) {
    if(conn == NULL)
       return ;

    if(conn->taos) {
        taos_close(conn->taos);
        conn->taos = NULL;
    }

    if (conn->ctaos) {
        taos_close(conn->ctaos);
        conn->ctaos = NULL;
    }
    tmfree(conn);
}

int32_t queryDbExecCall(SBenchConn *conn, char *command) {
    int32_t code = 0;
    TAOS_RES *res = taos_query(conn->taos, command);
    code = taos_errno(res);
    if (code) {
        printErrCmdCodeStr(command, code, res);
    } else {
        taos_free_result(res);
    }
    return code;
}

void encodeAuthBase64() {
    char        userpass_buf[INPUT_BUF_LEN];
    static char base64[] = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};
    snprintf(userpass_buf, INPUT_BUF_LEN, "%s:%s", g_arguments->user,
            g_arguments->password);

    int mod_table[] = {0, 2, 1};

    size_t userpass_buf_len = strlen(userpass_buf);
    size_t encoded_len = 4 * ((userpass_buf_len + 2) / 3);

    memset(g_arguments->base64_buf, 0, INPUT_BUF_LEN);
    for (int n = 0, m = 0; n < userpass_buf_len;) {
        uint32_t oct_a =
            n < userpass_buf_len ? (unsigned char)userpass_buf[n++] : 0;
        uint32_t oct_b =
            n < userpass_buf_len ? (unsigned char)userpass_buf[n++] : 0;
        uint32_t oct_c =
            n < userpass_buf_len ? (unsigned char)userpass_buf[n++] : 0;
        uint32_t triple = (oct_a << 0x10) + (oct_b << 0x08) + oct_c;

        g_arguments->base64_buf[m++] = base64[(triple >> 3 * 6) & 0x3f];
        g_arguments->base64_buf[m++] = base64[(triple >> 2 * 6) & 0x3f];
        g_arguments->base64_buf[m++] = base64[(triple >> 1 * 6) & 0x3f];
        g_arguments->base64_buf[m++] = base64[(triple >> 0 * 6) & 0x3f];
    }

    for (int l = 0; l < mod_table[userpass_buf_len % 3]; l++)
        g_arguments->base64_buf[encoded_len - 1 - l] = '=';
}

// fetch result fo file or nothing
int64_t fetchResult(TAOS_RES *res, char * filePath) {
    TAOS_ROW    row        = NULL;
    int         num_fields = 0;
    int64_t     totalLen   = 0;
    TAOS_FIELD *fields     = 0;
    int64_t     rows       = 0;
    char       *databuf    = NULL;
    bool        toFile     = strlen(filePath) > 0;
    

    if(toFile) {
        num_fields = taos_field_count(res);
        fields     = taos_fetch_fields(res);
        databuf    = (char *)benchCalloc(1, FETCH_BUFFER_SIZE, true);
    }

    // fetch the records row by row
    while ((row = taos_fetch_row(res))) {
        if (toFile) {
            if (totalLen >= (FETCH_BUFFER_SIZE - HEAD_BUFF_LEN * 2)) {
                // buff is full
                appendResultBufToFile(databuf, filePath);
                totalLen = 0;
                memset(databuf, 0, FETCH_BUFFER_SIZE);
            }

            // format row
            char temp[HEAD_BUFF_LEN] = {0};
            int  len = taos_print_row(temp, row, fields, num_fields);
            len += snprintf(temp + len, HEAD_BUFF_LEN - len, "\n");
            //debugPrint("query result:%s\n", temp);
            memcpy(databuf + totalLen, temp, len);
            totalLen += len;
        }
        rows ++;
        //if not toFile , only loop call taos_fetch_row
    }

    // end
    if (toFile) {
        appendResultBufToFile(databuf, filePath);
        free(databuf);
    }
    return rows;
}

char *convertDatatypeToString(int type) {
    switch (type) {
        case TSDB_DATA_TYPE_BINARY:
            return "binary";
        case TSDB_DATA_TYPE_NCHAR:
            return "nchar";
        case TSDB_DATA_TYPE_TIMESTAMP:
            return "timestamp";
        case TSDB_DATA_TYPE_TINYINT:
            return "tinyint";
        case TSDB_DATA_TYPE_UTINYINT:
            return "tinyint unsigned";
        case TSDB_DATA_TYPE_SMALLINT:
            return "smallint";
        case TSDB_DATA_TYPE_USMALLINT:
            return "smallint unsigned";
        case TSDB_DATA_TYPE_INT:
            return "int";
        case TSDB_DATA_TYPE_UINT:
            return "int unsigned";
        case TSDB_DATA_TYPE_BIGINT:
            return "bigint";
        case TSDB_DATA_TYPE_UBIGINT:
            return "bigint unsigned";
        case TSDB_DATA_TYPE_BOOL:
            return "bool";
        case TSDB_DATA_TYPE_FLOAT:
            return "float";
        case TSDB_DATA_TYPE_DOUBLE:
            return "double";
        case TSDB_DATA_TYPE_JSON:
            return "json";    
        case TSDB_DATA_TYPE_VARBINARY:
            return "varbinary";
        case TSDB_DATA_TYPE_GEOMETRY:
            return "geometry";
        default:
            break;
    }
    return "unknown type";
}

int convertTypeToLength(uint8_t type) {
    int ret = 0;
    switch (type) {
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_UBIGINT:
        case TSDB_DATA_TYPE_BIGINT:
            ret = sizeof(int64_t);
            break;
        case TSDB_DATA_TYPE_BOOL:
        case TSDB_DATA_TYPE_TINYINT:
        case TSDB_DATA_TYPE_UTINYINT:
            ret = sizeof(int8_t);
            break;
        case TSDB_DATA_TYPE_SMALLINT:
        case TSDB_DATA_TYPE_USMALLINT:
            ret = sizeof(int16_t);
            break;
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_UINT:
            ret = sizeof(int32_t);
            break;
        case TSDB_DATA_TYPE_FLOAT:
            ret = sizeof(float);
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            ret = sizeof(double);
            break;
        case TSDB_DATA_TYPE_JSON:
            ret = JSON_FIXED_LENGTH;
            break;
        default:
            break;
    }
    return ret;
}

int64_t convertDatatypeToDefaultMin(uint8_t type) {
    int64_t ret = 0;
    switch (type) {
        case TSDB_DATA_TYPE_BOOL:
        case TSDB_DATA_TYPE_GEOMETRY:
            ret = 0;
            break;
        case TSDB_DATA_TYPE_TINYINT:
            ret = -127;
            break;
        case TSDB_DATA_TYPE_SMALLINT:
            ret = -32767;
            break;
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_FLOAT:
        case TSDB_DATA_TYPE_DOUBLE:
            ret = -1 * (RAND_MAX >> 1);
            break;
        default:
            break;
    }
    return ret;
}

int64_t convertDatatypeToDefaultMax(uint8_t type) {
    int64_t ret = 0;
    switch (type) {
        case TSDB_DATA_TYPE_BOOL:
            ret = 1;
            break;
        case TSDB_DATA_TYPE_TINYINT:
            ret = 128;
            break;
        case TSDB_DATA_TYPE_UTINYINT:
            ret = 254;
            break;
        case TSDB_DATA_TYPE_SMALLINT:
        case TSDB_DATA_TYPE_GEOMETRY:
            ret = 32767;
            break;
        case TSDB_DATA_TYPE_USMALLINT:
            ret = 65534;
            break;
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_FLOAT:
        case TSDB_DATA_TYPE_DOUBLE:
            ret = RAND_MAX >> 1;
            break;
        case TSDB_DATA_TYPE_UINT:
        case TSDB_DATA_TYPE_UBIGINT:
        case TSDB_DATA_TYPE_TIMESTAMP:
            ret = RAND_MAX;
            break;
        default:
            break;
    }
    return ret;
}

// compare str with length
int32_t strCompareN(char *str1, char *str2, int length) {
    if (length == 0) {
        return strcasecmp(str1, str2);
    } else {
        return strncasecmp(str1, str2, length);
    }
}

int convertStringToDatatype(char *type, int length) {
    // compare with length
    if (0 == strCompareN(type, "binary", length)) {
        return TSDB_DATA_TYPE_BINARY;
    } else if (0 == strCompareN(type, "nchar", length)) {
        return TSDB_DATA_TYPE_NCHAR;
    } else if (0 == strCompareN(type, "timestamp", length)) {
        return TSDB_DATA_TYPE_TIMESTAMP;
    } else if (0 == strCompareN(type, "bool", length)) {
        return TSDB_DATA_TYPE_BOOL;
    } else if (0 == strCompareN(type, "tinyint", length)) {
        return TSDB_DATA_TYPE_TINYINT;
    } else if (0 == strCompareN(type, "tinyint unsigned", length)) {
        return TSDB_DATA_TYPE_UTINYINT;
    } else if (0 == strCompareN(type, "utinyint", length)) {
        return TSDB_DATA_TYPE_UTINYINT;
    } else if (0 == strCompareN(type, "smallint", length)) {
        return TSDB_DATA_TYPE_SMALLINT;
    } else if (0 == strCompareN(type, "smallint unsigned", length)) {
        return TSDB_DATA_TYPE_USMALLINT;
    } else if (0 == strCompareN(type, "usmallint", length)) {
        return TSDB_DATA_TYPE_USMALLINT;
    } else if (0 == strCompareN(type, "int", length)) {
        return TSDB_DATA_TYPE_INT;
    } else if (0 == strCompareN(type, "int unsigned", length)) {
        return TSDB_DATA_TYPE_UINT;
    } else if (0 == strCompareN(type, "uint", length)) {
        return TSDB_DATA_TYPE_UINT;
    } else if (0 == strCompareN(type, "bigint", length)) {
        return TSDB_DATA_TYPE_BIGINT;
    } else if (0 == strCompareN(type, "bigint unsigned", length)) {
        return TSDB_DATA_TYPE_UBIGINT;
    } else if (0 == strCompareN(type, "ubigint", length)) {
        return TSDB_DATA_TYPE_UBIGINT;
    } else if (0 == strCompareN(type, "float", length)) {
        return TSDB_DATA_TYPE_FLOAT;
    } else if (0 == strCompareN(type, "double", length)) {
        return TSDB_DATA_TYPE_DOUBLE;
    } else if (0 == strCompareN(type, "json", length)) {
        return TSDB_DATA_TYPE_JSON;
    } else if (0 == strCompareN(type, "varchar", length)) {
        return TSDB_DATA_TYPE_BINARY;
    } else if (0 == strCompareN(type, "varbinary", length)) {
        return TSDB_DATA_TYPE_VARBINARY;
    } else if (0 == strCompareN(type, "geometry", length)) {
        return TSDB_DATA_TYPE_GEOMETRY;
    } else {
        errorPrint("unknown data type: %s\n", type);
        exit(EXIT_FAILURE);
    }
}


int compare(const void *a, const void *b) {
    return *(int64_t *)a - *(int64_t *)b;
}

//
// --------------------  BArray operator -------------------
//

BArray* benchArrayInit(size_t size, size_t elemSize) {
    assert(elemSize > 0);

    if (size < BARRAY_MIN_SIZE) {
        size = BARRAY_MIN_SIZE;
    }

    BArray* pArray = (BArray *)benchCalloc(1, sizeof(BArray), true);

    pArray->size = 0;
    pArray->pData = benchCalloc(size, elemSize, true);

    pArray->capacity = size;
    pArray->elemSize = elemSize;
    return pArray;
}

static int32_t benchArrayEnsureCap(BArray* pArray, size_t newCap) {
    if (newCap > pArray->capacity) {
        size_t tsize = (pArray->capacity << 1u);
        while (newCap > tsize) {
            tsize = (tsize << 1u);
        }

        void* pData = realloc(pArray->pData, tsize * pArray->elemSize);
        if (pData == NULL) {
            return -1;
        }
        pArray->pData = pData;
        pArray->capacity = tsize;
    }
    return 0;
}

void* benchArrayAddBatch(BArray* pArray, void* pData, int32_t elems, bool free) {
    if (pData == NULL || elems <=0) {
        if (free) {
            tmfree(pData);
        }
        return NULL;
    }

    if (benchArrayEnsureCap(pArray, pArray->size + elems) != 0) {
        if (free) {
            tmfree(pData);
        }
        return NULL;
    }

    void* dst = BARRAY_GET_ELEM(pArray, pArray->size);
    memcpy(dst, pData, pArray->elemSize * elems);
    if (free) {
        tmfree(pData);
    }
    pArray->size += elems;
    return dst;
}

FORCE_INLINE void* benchArrayPush(BArray* pArray, void* pData) {
    return benchArrayAddBatch(pArray, pData, 1, true);
}

FORCE_INLINE void* benchArrayPushNoFree(BArray* pArray, void* pData) {
    return benchArrayAddBatch(pArray, pData, 1, false);
}


void* benchArrayDestroy(BArray* pArray) {
    if (pArray) {
        tmfree(pArray->pData);
        tmfree(pArray);
    }
    return NULL;
}

void benchArrayClear(BArray* pArray) {
    if (pArray == NULL) return;
    pArray->size = 0;
}

void* benchArrayGet(const BArray* pArray, size_t index) {
    if (index >= pArray->size) {
        errorPrint("benchArrayGet index(%zu) greater than BArray size(%zu)\n",
                   index, pArray->size);
        exit(EXIT_FAILURE);
    }
    return BARRAY_GET_ELEM(pArray, index);
}

bool searchBArray(BArray *pArray, const char *field_name, int32_t name_len, uint8_t field_type) {
    if (pArray == NULL || field_name == NULL) {
        return false;
    }
    for (int i = 0; i < pArray->size; i++) {
        Field *field = benchArrayGet(pArray, i);
        if (strlen(field->name) == name_len && strncasecmp(field->name, field_name, name_len) == 0) {
            if (field->type == field_type) {
                return true;
            }
            return false;
        }
    }
    return false;
}

//
// malloc a new and copy data from array
// return value must call benchArrayDestroy to free
//
BArray * copyBArray(BArray *pArray) {
    BArray * pNew = benchArrayInit(pArray->size, pArray->elemSize);
    benchArrayAddBatch(pNew, pArray->pData, pArray->size, false);
    return pNew;
}

//
//  ---------------- others ------------------------
//

#ifdef LINUX
int32_t bsem_wait(sem_t* sem) {
    int ret = 0;
    do {
        ret = sem_wait(sem);
    } while (ret != 0 && errno  == EINTR);
    return ret;
}

void benchSetSignal(int32_t signum, ToolsSignalHandler sigfp) {
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    act.sa_flags = SA_SIGINFO | SA_RESTART;
    act.sa_sigaction = (void (*)(int, siginfo_t *, void *)) sigfp;
    sigaction(signum, &act, NULL);
}
#endif


FORCE_INLINE void printErrCmdCodeStr(char *cmd, int32_t code, TAOS_RES *res) {    
    char buff[512];
    char *msg = cmd;
    if (strlen(cmd) >= sizeof(buff)) {
        memcpy(buff, cmd, 500);
        buff[500] = 0;
        strcat(buff, "...");
        msg = buff;
    }
    errorPrint("%s error code: 0x%08x, reason: %s command %s\n", TIP_ENGINE_ERR,
               code, taos_errstr(res), msg);
    taos_free_result(res);
}

int32_t benchGetTotalMemory(int64_t *totalKB) {
#ifdef WINDOWS
  MEMORYSTATUSEX memsStat;
  memsStat.dwLength = sizeof(memsStat);
  if (!GlobalMemoryStatusEx(&memsStat)) {
    return -1;
  }

  *totalKB = memsStat.ullTotalPhys / 1024;
  return 0;
#elif defined(_TD_DARWIN_64)
  *totalKB = 0;
  return 0;
#else
  int64_t tsPageSizeKB = sysconf(_SC_PAGESIZE) / 1024;
  *totalKB = (int64_t)(sysconf(_SC_PHYS_PAGES) * tsPageSizeKB);
  return 0;
#endif
}

// geneate question mark string , using insert into ... values(?,?,?...)
// return value must call tmfree to free memory
char* genQMark( int32_t QCnt) {
    char * buf = benchCalloc(4, QCnt, false);
    for (int32_t i = 0; i < QCnt; i++) {
        if (i == 0)
            strcat(buf, "?");
        else
            strcat(buf, ",?");
    }
    return buf;
}

// get colNames , first is tbname if tbName is true
char *genColNames(BArray *cols, bool tbName) {
    // reserve tbname,ts and "," space
    char * buf = benchCalloc(TSDB_TABLE_NAME_LEN + 1, cols->size + 1, false);
    if (tbName) {
        strcpy(buf, "tbname,ts");
    } else {
        strcpy(buf, "ts");
    }
   
    for (int32_t i = 0; i < cols->size; i++) {
        Field * col = benchArrayGet(cols, i);
        strcat(buf, ",");
        strcat(buf, col->name);
    }
    return buf;
}

//
//  STMT2  
//

// create
TAOS_STMT2_BINDV* createBindV(int32_t capacity, int32_t tagCnt, int32_t colCnt) {
    // calc total size
    int32_t tableSize = sizeof(char *) + sizeof(TAOS_STMT2_BIND *) + sizeof(TAOS_STMT2_BIND *) + 
                        sizeof(TAOS_STMT2_BIND) * tagCnt + sizeof(TAOS_STMT2_BIND) * colCnt;
    int32_t size = sizeof(TAOS_STMT2_BINDV) + tableSize * capacity;
    TAOS_STMT2_BINDV *bindv = benchCalloc(1, size, false);
    resetBindV(bindv, capacity, tagCnt, colCnt);

    return bindv;
}

// reset tags and cols poitner
void resetBindV(TAOS_STMT2_BINDV *bindv, int32_t capacity, int32_t tagCnt, int32_t colCnt) {
    unsigned char *p = (unsigned char *)bindv;
    // tbnames
    p += sizeof(TAOS_STMT2_BINDV); // skip BINDV
    bindv->tbnames = (char **)p;
    // tags
    if(tagCnt == 0 ) {
        bindv->tags = NULL;
    } else {
        p += sizeof(char *) * capacity; // skip tbnames
        bindv->tags = (TAOS_STMT2_BIND **)p;
    }
    // bind_cols
    p += sizeof(TAOS_STMT2_BIND *) * capacity; // skip tags
    bindv->bind_cols = (TAOS_STMT2_BIND **)p;
    p += sizeof(TAOS_STMT2_BIND *) * capacity; // skip cols

    int32_t i;
    // tags body
    if (tagCnt > 0) {
        for (i = 0; i < capacity; i++) {
            bindv->tags[i] = (TAOS_STMT2_BIND *)p;
            p += sizeof(TAOS_STMT2_BIND) * tagCnt; // skip tag bodys
        }
    }
    // bind_cols body
    for (i = 0; i < capacity; i++) {
        bindv->bind_cols[i] = (TAOS_STMT2_BIND*)p;
        p += sizeof(TAOS_STMT2_BIND) * colCnt; // skip cols bodys
    }
}

// clear bindv
void clearBindV(TAOS_STMT2_BINDV *bindv) {
    if (bindv == NULL)
        return ;
    for(int32_t i = 0; i < bindv->count; i++) {
        bindv->tags[i]      = NULL;
        bindv->bind_cols[i] = NULL;
    }
    bindv->count = 0;
}

// free
void freeBindV(TAOS_STMT2_BINDV *bindv) {
    tmfree(bindv);
}

//
//   debug show 
//

void showBind(TAOS_STMT2_BIND* bind) {
    // loop each column
    int32_t pos = 0;
    char* buff  = bind->buffer;
    for(int32_t n=0; n<bind->num; n++) {
        switch (bind->buffer_type) {
        case TSDB_DATA_TYPE_TIMESTAMP:
            debugPrint("   n=%d value=%" PRId64 "\n", n, *(int64_t *)(buff + pos));
            pos += sizeof(int64_t);
            break;
        case TSDB_DATA_TYPE_FLOAT:
            debugPrint("   n=%d value=%f\n", n, *(float *)(buff + pos));
            pos += sizeof(float);
            break;
        case TSDB_DATA_TYPE_INT:
            debugPrint("   n=%d value=%d\n", n, *(int32_t *)(buff + pos));
            pos += sizeof(int32_t);
            break;
        default:
            break;
        } 
    }

}

void showTableBinds(char* label, TAOS_STMT2_BIND* binds, int32_t cnt) {
    for (int32_t j = 0; j < cnt; j++) {
        if(binds == NULL) {
            debugPrint("  %d %s is NULL \n", j, label);
        } else {
            debugPrint("  %d %s type=%d buffer=%p \n", j, label, binds[j].buffer_type, binds[j].buffer);
            showBind(&binds[j]);
        }
    }
}

// show bindv
void showBindV(TAOS_STMT2_BINDV *bindv, BArray *tags, BArray *cols) {
    // num and base info
    debugPrint("show bindv table count=%d names=%p tags=%p bind_cols=%p\n", 
                bindv->count, bindv->tbnames, bindv->tags, bindv->bind_cols);
    
    for(int32_t i=0; i< bindv->count; i++) {
        debugPrint(" show bindv table index=%d name=%s \n", i, bindv->tbnames[i]);
        if(bindv->tags)
            showTableBinds("tag",    bindv->tags[i],      tags->size);
        if(bindv->bind_cols)    
            showTableBinds("column", bindv->bind_cols[i], cols->size + 1);
    }
}

// engine util/src/thashutil.c
uint32_t MurmurHash3_32(const char *key, uint32_t len);
// get group index about dbname.tbname
int32_t calcGroupIndex(char* dbName, char* tbName, int32_t groupCnt) {
    // check valid
    if (dbName == NULL || tbName == NULL) {
        return -1;
    }
    char key[1024];
    snprintf(key, sizeof(key), "1.%s.%s", dbName, tbName);
    uint32_t hash = MurmurHash3_32(key, strlen(key));
    uint32_t step = UINT32_MAX / groupCnt;
    for (int32_t i = 0; i < groupCnt; i++) {
        if (hash < (i + 1) * step)
        {
            return i;
        }
    }
    return groupCnt - 1;
}

// windows no export MurmurHash3_32 function from engine
#ifdef WINDOWS
// define
#define ROTL32(x, r) ((x) << (r) | (x) >> (32u - (r)))
#define FMIX32(h)      \
  do {                 \
    (h) ^= (h) >> 16;  \
    (h) *= 0x85ebca6b; \
    (h) ^= (h) >> 13;  \
    (h) *= 0xc2b2ae35; \
    (h) ^= (h) >> 16;  \
  } while (0)

// impl MurmurHash3_32
uint32_t MurmurHash3_32(const char *key, uint32_t len) {
  const uint8_t *data = (const uint8_t *)key;
  const int32_t  nblocks = len >> 2u;

  uint32_t h1 = 0x12345678;

  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;

  const uint32_t *blocks = (const uint32_t *)(data + nblocks * 4);

  for (int32_t i = -nblocks; i; i++) {
    uint32_t k1 = blocks[i];

    k1 *= c1;
    k1 = ROTL32(k1, 15u);
    k1 *= c2;

    h1 ^= k1;
    h1 = ROTL32(h1, 13u);
    h1 = h1 * 5 + 0xe6546b64;
  }

  const uint8_t *tail = (data + nblocks * 4);

  uint32_t k1 = 0;

  switch (len & 3u) {
    case 3:
      k1 ^= tail[2] << 16;
    case 2:
      k1 ^= tail[1] << 8;
    case 1:
      k1 ^= tail[0];
      k1 *= c1;
      k1 = ROTL32(k1, 15u);
      k1 *= c2;
      h1 ^= k1;
  };

  h1 ^= len;

  FMIX32(h1);

  return h1;
}
#endif


//
// ---------------- benchQuery util ----------------------
//

// init conn
int32_t initQueryConn(qThreadInfo * pThreadInfo, int iface) {
    // create conn
    pThreadInfo->conn = initBenchConn();
    if (pThreadInfo->conn == NULL) {
        return -1;
    }

    return 0;
}

// close conn
void closeQueryConn(qThreadInfo * pThreadInfo, int iface) {
    closeBenchConn(pThreadInfo->conn);
    pThreadInfo->conn = NULL;
}


// free g_queryInfo.specailQueryInfo memory , can re-call
void freeSpecialQueryInfo() {
    // can re-call
    if (g_queryInfo.specifiedQueryInfo.sqls == NULL) {
        return;
    }

    // loop free each item memory
    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqls->size; ++i) {
        SSQL *sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, i);
        tmfree(sql->command);
        tmfree(sql->delay_list);
    }

    // free Array
    benchArrayDestroy(g_queryInfo.specifiedQueryInfo.sqls);
    g_queryInfo.specifiedQueryInfo.sqls = NULL;
}


#define KILLID_LEN  64

void *queryKiller(void *arg) {
    char host[MAX_HOSTNAME_LEN] = {0};
    tstrncpy(host, g_arguments->host, MAX_HOSTNAME_LEN);

    while (true) {
        TAOS *taos = taos_connect(g_arguments->host, g_arguments->user,
                g_arguments->password, NULL, g_arguments->port);
        if (NULL == taos) {
            errorPrint("Slow query killer thread "
                    "failed to connect to the server %s\n",
                    g_arguments->host);
            return NULL;
        }

        char command[TSDB_MAX_ALLOWED_SQL_LEN] =
            "SELECT kill_id,exec_usec,sql FROM performance_schema.perf_queries";
        TAOS_RES *res = taos_query(taos, command);
        int32_t code = taos_errno(res);
        if (code) {
            printErrCmdCodeStr(command, code, res);
        }

        TAOS_ROW row = NULL;
        while ((row = taos_fetch_row(res)) != NULL) {
            int32_t *lengths = taos_fetch_lengths(res);
            if (lengths[0] <= 0) {
                infoPrint("No valid query found by %s\n", command);
            } else {
                int64_t execUSec = *(int64_t*)row[1];

                if (execUSec > g_queryInfo.killQueryThreshold * 1000000) {
                    char sql[SHORT_1K_SQL_BUFF_LEN] = {0};
                    tstrncpy(sql, (char*)row[2],
                             min(strlen((char*)row[2])+1,
                                 SHORT_1K_SQL_BUFF_LEN));

                    char killId[KILLID_LEN] = {0};
                    tstrncpy(killId, (char*)row[0],
                            min(strlen((char*)row[0])+1, KILLID_LEN));
                    char killCommand[KILLID_LEN + 32] = {0};
                    snprintf(killCommand, sizeof(killCommand), "KILL QUERY '%s'", killId);
                    TAOS_RES *resKill = taos_query(taos, killCommand);
                    int32_t codeKill = taos_errno(resKill);
                    if (codeKill) {
                        printErrCmdCodeStr(killCommand, codeKill, resKill);
                    } else {
                        infoPrint("%s succeed, sql: %s killed!\n",
                                  killCommand, sql);
                        taos_free_result(resKill);
                    }
                }
            }
        }

        taos_free_result(res);
        taos_close(taos);
        toolsMsleep(g_queryInfo.killQueryInterval*1000);
    }

    return NULL;
}

// kill show
int killSlowQuery() {
    pthread_t pidKiller = {0};
    int32_t ret = pthread_create(&pidKiller, NULL, queryKiller, NULL);
    if (ret != 0) {
        errorPrint("pthread_create failed create queryKiller thread. error code =%d \n", ret);
        return -1;
    }
    pthread_join(pidKiller, NULL);
    toolsMsleep(1000);
    return 0;
}

// fetch super table child name from server
int fetchChildTableName(char *dbName, char *stbName) {
    SBenchConn* conn = initBenchConn();
    if (conn == NULL) {
        return -1;
    }

    // get child count
    char  cmd[SHORT_1K_SQL_BUFF_LEN] = "\0";
    if (3 == g_majorVersionOfClient) {
        snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
                "SELECT COUNT(*) FROM( SELECT DISTINCT(TBNAME) FROM `%s`.`%s`)",
                dbName, stbName);
    } else {
        snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
                    "SELECT COUNT(TBNAME) FROM `%s`.`%s`",
                dbName, stbName);
    }
    TAOS_RES *res = taos_query(conn->taos, cmd);
    int32_t   code = taos_errno(res);
    if (code) {
        printErrCmdCodeStr(cmd, code, res);
        closeBenchConn(conn);
        return -1;
    }

    TAOS_ROW    row = NULL;
    int         num_fields = taos_num_fields(res);
    TAOS_FIELD *fields = taos_fetch_fields(res);
    while ((row = taos_fetch_row(res)) != NULL) {
        if (0 == strlen((char *)(row[0]))) {
            errorPrint("stable %s have no child table\n", stbName);
            taos_free_result(res);
            closeBenchConn(conn);
            return -1;
        }
        char temp[256] = {0};
        taos_print_row(temp, row, fields, num_fields);

        // set child table count
        g_queryInfo.superQueryInfo.childTblCount = (int64_t)atol(temp);
    }
    infoPrint("%s's childTblCount: %" PRId64 "\n", stbName, g_queryInfo.superQueryInfo.childTblCount);
    taos_free_result(res);

    // malloc memory with child table count
    g_queryInfo.superQueryInfo.childTblName =
        benchCalloc(g_queryInfo.superQueryInfo.childTblCount,
                sizeof(char *), false);
    // fetch child table name
    if (getAllChildNameOfSuperTable(
                conn->taos, dbName, stbName,
                g_queryInfo.superQueryInfo.childTblName,
                g_queryInfo.superQueryInfo.childTblCount)) {
        // faild            
        tmfree(g_queryInfo.superQueryInfo.childTblName);
        closeBenchConn(conn);
        return -1;
    }
    closeBenchConn(conn);

    // succ
    return 0;
}

// skip prefix suffix blank
int trimCaseCmp(char *str1, char *str2) {
    // Skip leading whitespace in str1
    while (isblank((unsigned char)*str1)) {
        str1++;
    }

    // Compare characters case-insensitively
    while (*str2 != '\0') {
        if (tolower((unsigned char)*str1) != tolower((unsigned char)*str2)) {
            return -1;
        }
        str1++;
        str2++;
    }

    // Check if the remaining characters in str1 are all whitespace
    while (*str1 != '\0') {    
        if (!isblank((unsigned char)*str1)) {
            return -1;
        }
        str1++;
    }

    return 0;
}