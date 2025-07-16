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
#include "decimal.h"
#include "pub.h"
#if defined(_TD_DARWIN_64)
#include <sys/sysctl.h>
#include <mach/mach.h>
#include <mach/mach_host.h>
#include <mach/vm_page_size.h> 
#endif

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
        return NULL;
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

FORCE_INLINE bool isRest(int32_t iface) { 
    return REST_IFACE == iface || SML_REST_IFACE == iface;
}

void engineError(char * module, char * fun, int32_t code) {
    errorPrint("%s API:%s error code:0x%08X %s\n", TIP_ENGINE_ERR, fun, code, module);
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

int convertHostToServAddr(char *host, uint16_t port,
        struct sockaddr_in *serv_addr) {
    if (!host) {
        errorPrint("%s", "convertHostToServAddr host is null.");
        return -1;
    }
    debugPrint("convertHostToServAddr(host: %s, port: %d)\n", host,
            port);
#ifdef WINDOWS
    WSADATA wsaData;
    int ret = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (ret) {
        return ret;
    }
#endif
    struct hostent *server = gethostbyname(host);
    if ((server == NULL) || (server->h_addr == NULL)) {
        errorPrint("%s", "no such host");
        return -1;
    }
    memset(serv_addr, 0, sizeof(struct sockaddr_in));
    serv_addr->sin_family = AF_INET;
    serv_addr->sin_port = htons(port);

#ifdef WINDOWS
    struct addrinfo  hints = {0};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *pai = NULL;

    if (!getaddrinfo(server->h_name, NULL, &hints, &pai)) {
        serv_addr->sin_addr.s_addr =
               ((struct sockaddr_in *) pai->ai_addr)->sin_addr.s_addr;
        freeaddrinfo(pai);
    }
    WSACleanup();
#else
    serv_addr->sin_addr.s_addr = inet_addr(host);
    memcpy(&(serv_addr->sin_addr.s_addr), server->h_addr, server->h_length);
#endif
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

void appendResultBufToFile(char *resultBuf, char * filePath) {
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

    TOOLS_STRNCPY(outSql, inSql, pos - inSql + 1);
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
        case TSDB_DATA_TYPE_DECIMAL:
        case TSDB_DATA_TYPE_DECIMAL64:
            return "decimal";
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
        case TSDB_DATA_TYPE_DECIMAL:
            ret = sizeof(Decimal128);
            break;
        case TSDB_DATA_TYPE_DECIMAL64:
            ret = sizeof(Decimal64);
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
        case TSDB_DATA_TYPE_DECIMAL:
        case TSDB_DATA_TYPE_DECIMAL64:
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
        case TSDB_DATA_TYPE_DECIMAL:
        case TSDB_DATA_TYPE_DECIMAL64:
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


void doubleToDecimal64(double val, uint8_t precision, uint8_t scale, Decimal64* dec) {
    char buf[DECIMAL64_BUFF_LEN] = {0};
    (void)snprintf(buf, sizeof(buf), "%.*f", scale, val);
    decimal64FromStr(buf, strlen(buf), precision, scale, dec);
}


void doubleToDecimal128(double val, uint8_t precision, uint8_t scale, Decimal128* dec) {
    char buf[DECIMAL_BUFF_LEN] = {0};
    (void)snprintf(buf, sizeof(buf), "%.*f", scale, val);
    decimal128FromStr(buf, strlen(buf), precision, scale, dec);
}


void stringToDecimal64(const char* str, uint8_t precision, uint8_t scale, Decimal64* dec) {
    decimal64FromStr(str, strlen(str), precision, scale, dec);
}


void stringToDecimal128(const char* str, uint8_t precision, uint8_t scale, Decimal128* dec) {
    decimal128FromStr(str, strlen(str), precision, scale, dec);
}


int decimal64ToString(const Decimal64* dec, uint8_t precision, uint8_t scale, char* buf, size_t size) {
    return decimalToStr(dec, TSDB_DATA_TYPE_DECIMAL64, precision, scale, buf, size);
}


int decimal128ToString(const Decimal128* dec, uint8_t precision, uint8_t scale, char* buf, size_t size) {
    return decimalToStr(dec, TSDB_DATA_TYPE_DECIMAL, precision, scale, buf, size);
}


void getDecimal64DefaultMax(uint8_t precision, uint8_t scale, Decimal64* dec) {
    char maxStr[DECIMAL64_BUFF_LEN];

    precision = MIN(precision, DECIMAL64_BUFF_LEN - 1);
    for(int i = 0; i < precision; ++i) {
        maxStr[i] = '9';
    }
    maxStr[precision] = '\0';
    
    stringToDecimal64(maxStr, precision, scale, dec);
    return;
}


void getDecimal64DefaultMin(uint8_t precision, uint8_t scale, Decimal64* dec) {
    char minStr[DECIMAL64_BUFF_LEN];

    precision = MIN(precision, DECIMAL64_BUFF_LEN - 2);
    minStr[0] = '-';
    for(int i = 1; i <= precision; ++i) {
        minStr[i] = '9';
    }
    minStr[precision + 1] = '\0';
    
    stringToDecimal64(minStr, precision, scale, dec);
    return;
}


void getDecimal128DefaultMax(uint8_t precision, uint8_t scale, Decimal128* dec) {
    char maxStr[DECIMAL_BUFF_LEN];

    precision = MIN(precision, DECIMAL_BUFF_LEN - 1);
    for(int i = 0; i < precision; ++i) {
        maxStr[i] = '9';
    }
    maxStr[precision] = '\0';
    
    stringToDecimal128(maxStr, precision, scale, dec);
    return;
}


void getDecimal128DefaultMin(uint8_t precision, uint8_t scale, Decimal128* dec) {
    char minStr[DECIMAL_BUFF_LEN];

    precision = MIN(precision, DECIMAL_BUFF_LEN - 2);
    minStr[0] = '-';
    for(int i = 1; i <= precision; ++i) {
        minStr[i] = '9';
    }
    minStr[precision + 1] = '\0';
    
    stringToDecimal128(minStr, precision, scale, dec);
    return;
}


int decimal64BCompare(const Decimal64* a, const Decimal64* b) {
    return DECIMAL64_GET_VALUE(a) < DECIMAL64_GET_VALUE(b) ? -1 : (DECIMAL64_GET_VALUE(a) > DECIMAL64_GET_VALUE(b));
}


int decimal128BCompare(const Decimal128* a, const Decimal128* b) {
    const uint64_t sign_mask = (uint64_t)1 << 63;
    int a_sign = (DECIMAL128_HIGH_WORD(a) & sign_mask) >> 63;
    int b_sign = (DECIMAL128_HIGH_WORD(b) & sign_mask) >> 63;

    if (a_sign != b_sign) {
        return a_sign < b_sign ? 1 : -1;
    }

    if (DECIMAL128_HIGH_WORD(a) != DECIMAL128_HIGH_WORD(b)) {
        return DECIMAL128_HIGH_WORD(a) < DECIMAL128_HIGH_WORD(b) ? -1 : 1;
    } else {
        return DECIMAL128_LOW_WORD(a) < DECIMAL128_LOW_WORD(b) ? -1 : (DECIMAL128_LOW_WORD(a) > DECIMAL128_LOW_WORD(b));
    }
}


// compare str with length
int32_t strCompareN(char *str1, char *str2, int length) {
    if (length == 0) {
        return strcasecmp(str1, str2);
    } else {
        return strncasecmp(str1, str2, length);
    }
}

int convertStringToDatatype(char *type, int length, void* ctx) {
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
    } else if (0 == strCompareN(type, "decimal", length)) {
        uint8_t precision = *(uint8_t*)ctx;
        if (precision > TSDB_DECIMAL64_MAX_PRECISION)
            return TSDB_DATA_TYPE_DECIMAL;
        else
            return TSDB_DATA_TYPE_DECIMAL64;
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

int convertServAddr(int iface, bool tcp, int protocol) {
    // get host
    char * host = g_arguments->host ? g_arguments->host :DEFAULT_HOST;

    // covert
    if (tcp
            && iface == SML_REST_IFACE
            && protocol == TSDB_SML_TELNET_PROTOCOL) {
        // telnet_tcp_port        
        if (convertHostToServAddr(host,
                    g_arguments->telnet_tcp_port,
                    &(g_arguments->serv_addr))) {
            errorPrint("failed to convertHostToServAddr host=%s telnet_tcp_port:%d iface=%d \n", 
                    host, g_arguments->telnet_tcp_port, iface);
            return -1;
        }
        infoPrint("restful connect -> convertServAddr host=%s telnet_tcp_port:%d to serv_addr=%p iface=%d \n", 
                host, g_arguments->telnet_tcp_port, &g_arguments->serv_addr, iface);
    } else {
        // port
        int port = g_arguments->port_inputted ? g_arguments->port:DEFAULT_REST_PORT;
        if (convertHostToServAddr(host,
                                    port,
                    &(g_arguments->serv_addr))) {
            errorPrint("%s\n", "convert host to server address");
            return -1;
        }
        infoPrint("restful connect -> convertServAddr host=%s port:%d to serv_addr=%p iface=%d \n", 
                host, port, &g_arguments->serv_addr, iface);
    }
    return 0;
}

static void errorPrintSocketMsg(char *msg, int result) {
#ifdef WINDOWS
    errorPrint("%s: %d\n", msg, WSAGetLastError());
#else
    errorPrint("%s: %d\n", msg, result);
#endif
}

int createSockFd() {
#ifdef WINDOWS
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
    SOCKET sockfd;
#else
    int sockfd;
#endif
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        errorPrintSocketMsg("Could not create socket : ", sockfd);
        return -1;
    }

    int retConn = connect(
            sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
            sizeof(struct sockaddr));
    infoPrint("createSockFd call connect serv_addr=%p retConn=%d\n", &g_arguments->serv_addr, retConn);
    if (retConn < 0) {
        errorPrint("%s\n", "failed to connect");
#ifdef WINDOWS
        closesocket(sockfd);
        WSACleanup();
#else
        close(sockfd);
#endif
        return -1;
    }
    return sockfd;
}

static void closeSockFd(int sockfd) {
#ifdef WINDOWS
    closesocket(sockfd);
    WSACleanup();
#else
    close(sockfd);
#endif
}

void destroySockFd(int sockfd) {
    // check valid
    if (sockfd < 0) {
        return;
    }

    // shutdown the connection since no more data will be sent
    int result;
    result = shutdown(sockfd, SHUT_WR);
    if (SOCKET_ERROR == result) {
        errorPrintSocketMsg("Socket shutdown failed with error: ", result);
        closeSockFd(sockfd);
        return;
    }
    // Receive until the peer closes the connection
    do {
        int recvbuflen = LARGE_BUFF_LEN;
        char recvbuf[LARGE_BUFF_LEN];
        result = recv(sockfd, recvbuf, recvbuflen, 0);
        if ( result > 0 ) {
            debugPrint("Socket bytes received: %d\n", result);
        } else if (result == 0) {
            infoPrint("Connection closed with result %d\n", result);
        } else {
            errorPrintSocketMsg("Socket recv failed with error: ", result);
        }
    } while (result > 0);

    closeSockFd(sockfd);
}

FORCE_INLINE void printErrCmdCodeStr(char *cmd, int32_t code, TAOS_RES *res) {    
    char buff[530];
    char *msg = cmd;
    if (strlen(cmd) >= sizeof(buff)) {
        snprintf(buff, sizeof(buff), "%s", cmd);
        msg = buff;
    }
    errorPrint("%s error code: 0x%08x, reason: %s command %s\n", TIP_ENGINE_ERR,
               code, taos_errstr(res), msg);
    taos_free_result(res);
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
char *genColNames(BArray *cols, bool tbName, char * primaryKeyName) {
    // reserve tbname,ts and "," space
    char * buf = benchCalloc(TSDB_TABLE_NAME_LEN + 1, cols->size + 1, false);
    if (tbName) {
        snprintf(buf, TSDB_TABLE_NAME_LEN, "tbname,%s", primaryKeyName);
    } else {
        strcpy(buf, primaryKeyName);
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

//
// ---------------- benchQuery util ----------------------
//

// init conn
int32_t initQueryConn(qThreadInfo * pThreadInfo, int iface) {
    // create conn
    if (iface == REST_IFACE) {
        int sockfd = createSockFd();
        if (sockfd < 0) {
            return -1;
        }
        pThreadInfo->sockfd = sockfd;
    } else {
        pThreadInfo->conn = initBenchConn();
        if (pThreadInfo->conn == NULL) {
            return -1;
        }
    }

    return 0;
}

// close conn
void closeQueryConn(qThreadInfo * pThreadInfo, int iface) {
    if (iface == REST_IFACE) {
#ifdef WINDOWS
        closesocket(pThreadInfo->sockfd);
        WSACleanup();
#else
        close(pThreadInfo->sockfd);
#endif
    } else {
        closeBenchConn(pThreadInfo->conn);
        pThreadInfo->conn = NULL;
    }
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
    int32_t loop = 0;
    while (!g_arguments->terminate && ++loop < 3) {
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
                    TOOLS_STRNCPY(sql, (char*)row[2],
                             min(strlen((char*)row[2])+1,
                                 SHORT_1K_SQL_BUFF_LEN));

                    char killId[KILLID_LEN] = {0};
                    TOOLS_STRNCPY(killId, (char*)row[0],
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
    snprintf(cmd, SHORT_1K_SQL_BUFF_LEN,
            "SELECT COUNT(*) FROM( SELECT DISTINCT(TBNAME) FROM `%s`.`%s`)",
            dbName, stbName);
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

int32_t queryDbExecRest(char *command, char* dbName, int precision,
                    int iface, int protocol, bool tcp, int sockfd) {
    int32_t code = postProcessSql(command,
                         dbName,
                         precision,
                         iface,
                         protocol,
                         g_arguments->port,
                         tcp,
                         sockfd,
                         NULL);
    return code;
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

    // auth
    char *user     = g_arguments->user     ? g_arguments->user     : TSDB_DEFAULT_USER;
    char *password = g_arguments->password ? g_arguments->password : TSDB_DEFAULT_PASS;
    snprintf(userpass_buf, INPUT_BUF_LEN, "%s:%s", user, password);

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

int postProceSqlImpl(char *sqlstr, char* dbName, int precision, int iface,
                     int protocol, uint16_t rest_port, bool tcp, int sockfd,
                     char* filePath,
                     char *responseBuf, int64_t response_length) {
    int32_t      code = -1;
    char *       req_fmt =
        "POST %s HTTP/1.1\r\nHost: %s:%d\r\nAccept: */*\r\nAuthorization: "
        "Basic %s\r\nContent-Length: %d\r\nContent-Type: "
        "application/x-www-form-urlencoded\r\n\r\n%s";
    char url[URL_BUFF_LEN] = {0};
    if (iface == REST_IFACE) {
        snprintf(url, URL_BUFF_LEN, "/rest/sql/%s", dbName);
    } else if (iface == SML_REST_IFACE
            && protocol == TSDB_SML_LINE_PROTOCOL) {
        snprintf(url, URL_BUFF_LEN,
                 "/influxdb/v1/write?db=%s&precision=%s", dbName,
                precision == TSDB_TIME_PRECISION_MILLI
                ? "ms"
                : precision == TSDB_TIME_PRECISION_NANO
                ? "ns"
                : "u");
    } else if (iface == SML_REST_IFACE
            && protocol == TSDB_SML_TELNET_PROTOCOL) {
        snprintf(url, URL_BUFF_LEN, "/opentsdb/v1/put/telnet/%s", dbName);
    } else if (iface == SML_REST_IFACE
            && (protocol == TSDB_SML_JSON_PROTOCOL
                || protocol == SML_JSON_TAOS_FORMAT)) {
        snprintf(url, URL_BUFF_LEN, "/opentsdb/v1/put/json/%s", dbName);
    }

    int      bytes, sent, received, req_str_len, resp_len;
    char *   request_buf = NULL;
    int req_buf_len = (int)strlen(sqlstr) + REQ_EXTRA_BUF_LEN;

    if (g_arguments->terminate) {
        goto free_of_postImpl;
    }
    request_buf = benchCalloc(1, req_buf_len, false);

    int r;
    if (protocol == TSDB_SML_TELNET_PROTOCOL && tcp) {
        r = snprintf(request_buf, req_buf_len, "%s", sqlstr);
    } else {
        r = snprintf(request_buf, req_buf_len, req_fmt, url, g_arguments->host,
                rest_port, g_arguments->base64_buf, strlen(sqlstr),
                sqlstr);
    }
    if (r >= req_buf_len) {
        free(request_buf);
        errorPrint("%s","too long request");
        goto free_of_postImpl;
    }

    req_str_len = (int)strlen(request_buf);
    debugPrint("request buffer: %s\n", request_buf);
    sent = 0;
    do {
        bytes = send(sockfd, request_buf + sent,
                req_str_len - sent, 0);
        if (bytes < 0) {
            errorPrint("%s", "writing no message to socket\n");
            goto free_of_postImpl;
        }
        if (bytes == 0) break;
        sent += bytes;
    } while ((sent < req_str_len) && !g_arguments->terminate);

    if (protocol == TSDB_SML_TELNET_PROTOCOL
            && iface == SML_REST_IFACE && tcp) {
        code = 0;
        goto free_of_postImpl;
    }

    resp_len = response_length - 1;
    received = 0;

    bool chunked = false;

    if (g_arguments->terminate) {
        goto free_of_postImpl;
    }
    do {
        bytes = recv(sockfd, responseBuf + received,
                resp_len - received, 0);
        responseBuf[resp_len] = 0;
        debugPrint("response buffer: %s\n", responseBuf);
        if (NULL != strstr(responseBuf, resEncodingChunk)) {
            chunked = true;
        }
        int64_t index = strlen(responseBuf) - 1;
        while (responseBuf[index] == '\n' || responseBuf[index] == '\r') {
            index--;
        }
        debugPrint("index: %" PRId64 "\n", index);
        if (chunked && responseBuf[index] == '0') {
            code = 0;
            break;
        }
        if (!chunked && responseBuf[index] == '}') {
            code = 0;
            break;
        }

        if (bytes <= 0) {
            errorPrint("%s", "reading no response from socket\n");
            goto free_of_postImpl;
        }

        received += bytes;

        if (g_arguments->test_mode == INSERT_TEST) {
            if (strlen(responseBuf)) {
                if (((NULL != strstr(responseBuf, resEncodingChunk)) &&
                            (NULL != strstr(responseBuf, resHttp))) ||
                        ((NULL != strstr(responseBuf, resHttpOk)) ||
                         (NULL != strstr(responseBuf, influxHttpOk)) ||
                         (NULL != strstr(responseBuf, opentsdbHttpOk)))) {
                    break;
                }
            }
        }
    } while ((received < resp_len) && !g_arguments->terminate);

    if (received == resp_len) {
        errorPrint("%s", "storing complete response from socket\n");
        goto free_of_postImpl;
    }

    if (NULL == strstr(responseBuf, resHttpOk) &&
            NULL == strstr(responseBuf, influxHttpOk) &&
            NULL == strstr(responseBuf, succMessage) &&
            NULL == strstr(responseBuf, opentsdbHttpOk)) {
        errorPrint("Response:\n%s\n", responseBuf);
        goto free_of_postImpl;
    }

    code = 0;
free_of_postImpl:
    if (filePath && strlen(filePath) > 0 && !g_arguments->terminate) {
        appendResultBufToFile(responseBuf, filePath);
    }
    tmfree(request_buf);
    return code;
}

static int getServerVersionRestImpl(int16_t rest_port, int sockfd) {
    int server_ver = -1;
    char       command[SHORT_1K_SQL_BUFF_LEN] = "\0";
    snprintf(command, SHORT_1K_SQL_BUFF_LEN, "SELECT SERVER_VERSION()");
    char *responseBuf = benchCalloc(1, RESP_BUF_LEN, false);
    int code = postProceSqlImpl(command,
                                NULL,
                                0,
                                REST_IFACE,
                                0,
                                rest_port,
                                false,
                                sockfd,
                                NULL, responseBuf, RESP_BUF_LEN);
    if (code != 0) {
        errorPrint("Failed to execute command: %s\n", command);
        goto free_of_getversion;
    }
    debugPrint("response buffer: %s\n", responseBuf);
    if (NULL != strstr(responseBuf, resHttpOk)) {
        char* start = strstr(responseBuf, "{");
        if (start == NULL) {
            errorPrint("Invalid response format: %s\n", responseBuf);
            goto free_of_getversion;
        }
        tools_cJSON* resObj = tools_cJSON_Parse(start);
        if (resObj == NULL) {
            errorPrint("Cannot parse response into json: %s\n", start);
        }
        tools_cJSON* dataObj = tools_cJSON_GetObjectItem(resObj, "data");
        if (!tools_cJSON_IsArray(dataObj)) {
            char* pstr = tools_cJSON_Print(resObj);
            errorPrint("Invalid or miss 'data' key in json: %s\n", pstr ? pstr : "null");
            tmfree(pstr);
            tools_cJSON_Delete(resObj);
            goto free_of_getversion;
        }
        tools_cJSON *versionObj = tools_cJSON_GetArrayItem(dataObj, 0);
        tools_cJSON *versionStrObj = tools_cJSON_GetArrayItem(versionObj, 0);
        server_ver = atoi(versionStrObj->valuestring);
        char* pstr = tools_cJSON_Print(versionStrObj);        
        debugPrint("versionStrObj: %s, version: %s, server_ver: %d\n",
                   pstr ? pstr : "null",
                   versionStrObj->valuestring, server_ver);
        tmfree(pstr);
        tools_cJSON_Delete(resObj);
    }
free_of_getversion:
    free(responseBuf);
    return server_ver;
}

int getServerVersionRest(int16_t rest_port) {
    int sockfd = createSockFd();
    if (sockfd < 0) {
        return -1;
    }

    int server_version = getServerVersionRestImpl(rest_port, sockfd);

    destroySockFd(sockfd);
    return server_version;
}

int getCodeFromResp(char *responseBuf) {
    int code = -1;
    char* start = strstr(responseBuf, "{");
    if (start == NULL) {
        errorPrint("Invalid response format: %s\n", responseBuf);
        return -1;
    }
    tools_cJSON* resObj = tools_cJSON_Parse(start);
    if (resObj == NULL) {
        errorPrint("Cannot parse response into json: %s\n", start);
        return -1;
    }
    tools_cJSON* codeObj = tools_cJSON_GetObjectItem(resObj, "code");
    if (!tools_cJSON_IsNumber(codeObj)) {
        char* pstr = tools_cJSON_Print(resObj);
        errorPrint("Invalid or miss 'code' key in json: %s\n", pstr ? pstr : "null");
        tmfree(pstr);
        tools_cJSON_Delete(resObj);
        return -1;
    }

    code = codeObj->valueint;

    if (codeObj->valueint != 0) {
        tools_cJSON* desc = tools_cJSON_GetObjectItem(resObj, "desc");
        if (!tools_cJSON_IsString(desc)) {
            char* pstr = tools_cJSON_Print(resObj);
            errorPrint("Invalid or miss 'desc' key in json: %s\n", pstr ? pstr : "null");
            tmfree(pstr);
            return -1;
        }
        errorPrint("response, code: %d, reason: %s\n",
                   (int)codeObj->valueint, desc->valuestring);
    }

    tools_cJSON_Delete(resObj);
    return code;
}

int postProcessSql(char *sqlstr, char* dbName, int precision, int iface,
                 int protocol, uint16_t rest_port,
                 bool tcp, int sockfd, char* filePath) {
    uint64_t response_length;
    if (g_arguments->test_mode == INSERT_TEST) {
        response_length = RESP_BUF_LEN;
    } else {
        response_length = g_queryInfo.response_buffer;
    }

    char *responseBuf = benchCalloc(1, response_length, false);
    int code = postProceSqlImpl(sqlstr, dbName, precision, iface, protocol,
                                rest_port,
                                tcp, sockfd, filePath, responseBuf,
                                response_length);
    debugPrint("sqls:%s db:%s iface:%d protocal:%d rest_port:%d tcp:%d sockfd:%d response=%s\n",
           sqlstr, dbName, iface, protocol, rest_port, tcp, sockfd, responseBuf);

    if (NULL != strstr(responseBuf, resHttpOk) && iface == REST_IFACE) {
        code = 0;
        // if taosd is not starting , rest_server_ver_major can't be got by 'select server_version()' , so is -1
        if (-1 == g_arguments->rest_server_ver_major || 3 <= g_arguments->rest_server_ver_major) {
            code = getCodeFromResp(responseBuf);
        }
        goto free_of_post;
    }

    // influx
    if (NULL != strstr(responseBuf, influxHttpOk) &&
            protocol == TSDB_SML_LINE_PROTOCOL && iface == SML_REST_IFACE) {
        code = 0;
        goto free_of_post;
    }

    // opentsdb
    if (NULL != strstr(responseBuf, opentsdbHttpOk)
            && (protocol == TSDB_SML_TELNET_PROTOCOL
            || protocol == TSDB_SML_JSON_PROTOCOL
            || protocol == SML_JSON_TAOS_FORMAT)
            && iface == SML_REST_IFACE) {
        code = 0;
        goto free_of_post;
    }

    if (g_arguments->test_mode == INSERT_TEST) {
        debugPrint("Response: \n%s\n", responseBuf);
        char* start = strstr(responseBuf, "{");
        if ((start == NULL)
                && (TSDB_SML_TELNET_PROTOCOL != protocol)
                && (TSDB_SML_JSON_PROTOCOL != protocol)
                && (SML_JSON_TAOS_FORMAT != protocol)
                ) {
            errorPrint("Invalid response format: %s\n", responseBuf);
            goto free_of_post;
        }
        tools_cJSON* resObj = tools_cJSON_Parse(start);
        if ((resObj == NULL)
                && (TSDB_SML_TELNET_PROTOCOL != protocol)
                && (TSDB_SML_JSON_PROTOCOL != protocol)
                && (SML_JSON_TAOS_FORMAT != protocol)
                ) {
            errorPrint("Cannot parse response into json: %s\n", start);
        }
        tools_cJSON* codeObj = tools_cJSON_GetObjectItem(resObj, "code");
        if ((!tools_cJSON_IsNumber(codeObj))
                && (TSDB_SML_TELNET_PROTOCOL != protocol)
                && (TSDB_SML_JSON_PROTOCOL != protocol)
                && (SML_JSON_TAOS_FORMAT != protocol)
                ) {
            char* pstr = tools_cJSON_Print(resObj);
            errorPrint("Invalid or miss 'code' key in json: %s\n", pstr ? pstr : "null");
            tmfree(pstr);
            tools_cJSON_Delete(resObj);
            goto free_of_post;
        }

        if ((SML_REST_IFACE == iface) && codeObj
                && (200 == codeObj->valueint)) {
            code = 0;
            tools_cJSON_Delete(resObj);
            goto free_of_post;
        }

        if ((iface == SML_REST_IFACE)
                && (protocol == TSDB_SML_LINE_PROTOCOL)
                && codeObj
                && (codeObj->valueint != 0) && (codeObj->valueint != 200)) {
            tools_cJSON* desc = tools_cJSON_GetObjectItem(resObj, "desc");
            if (!tools_cJSON_IsString(desc)) {
                char* pstr = tools_cJSON_Print(resObj);
                errorPrint("Invalid or miss 'desc' key in json: %s\n", pstr ? pstr : "null");
                tmfree(pstr);
            } else {
                errorPrint("insert mode response, code: %d, reason: %s\n",
                       (int)codeObj->valueint, desc->valuestring);
            }
        } else {
            code = 0;
        }
        tools_cJSON_Delete(resObj);
    }
free_of_post:
    free(responseBuf);
    return code;
}

int check_write_permission(const char *path) {
    FILE *fp = fopen(path, "w");
    if (fp == NULL) {
        errorPrint("Error: %s No write permission\n", path);
        return -1;
    }
    fclose(fp);
    
    return 0;
}