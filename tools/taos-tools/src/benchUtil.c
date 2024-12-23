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

#include <bench.h>
#include "benchLog.h"

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

FORCE_INLINE bool isRest(int32_t iface) { 
    return REST_IFACE == iface || SML_REST_IFACE == iface;
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
             "select distinct tbname from %s.`%s` limit %" PRId64 "",
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

void replaceChildTblName(char *inSql, char *outSql, int tblIndex) {
    char sourceString[32] = "xxxx";
    char *pos = strstr(inSql, sourceString);
    if (0 == pos) return;

    char subTblName[TSDB_TABLE_NAME_LEN];
    snprintf(subTblName, TSDB_TABLE_NAME_LEN,
            "%s.%s", g_queryInfo.dbName,
            g_queryInfo.superQueryInfo.childTblName[tblIndex]);

    tstrncpy(outSql, inSql, pos - inSql + 1);
    snprintf(outSql + strlen(outSql), TSDB_MAX_ALLOWED_SQL_LEN -1,
             "%s%s", subTblName, pos + strlen(sourceString));
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
#ifdef WEBSOCKET
    if (g_arguments->websocket) {
        conn->taos_ws = ws_connect(g_arguments->dsn);
        char maskedDsn[256] = "\0";
        memcpy(maskedDsn, g_arguments->dsn, 20);
        memcpy(maskedDsn+20, "...", 3);
        memcpy(maskedDsn+23,
               g_arguments->dsn + strlen(g_arguments->dsn)-10, 10);
        if (conn->taos_ws == NULL) {
            errorPrint("failed to connect %s, reason: %s\n",
                    maskedDsn, ws_errstr(NULL));
            tmfree(conn);
            return NULL;
        }

        succPrint("%s conneced\n", maskedDsn);
    } else {
#endif
        conn->taos = taos_connect(g_arguments->host,
                g_arguments->user, g_arguments->password,
                NULL, g_arguments->port);
        if (conn->taos == NULL) {
            errorPrint("failed to connect native %s:%d, "
                       "code: 0x%08x, reason: %s\n",
                    g_arguments->host, g_arguments->port,
                    taos_errno(NULL), taos_errstr(NULL));
            tmfree(conn);
            return NULL;
        }

        conn->ctaos = taos_connect(g_arguments->host,
                                   g_arguments->user,
                                   g_arguments->password,
                                   NULL, g_arguments->port);
#ifdef WEBSOCKET
    }
#endif
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

        infoPrint("sleep %dms and try to connect... %d  \n", g_arguments->trying_interval, keep_trying);
        if(g_arguments->trying_interval > 0) {
            toolsMsleep(g_arguments->trying_interval);
        }        
    } 

    return conn;
}

void closeBenchConn(SBenchConn* conn) {
    if(conn == NULL)
       return ;
#ifdef WEBSOCKET
    if (g_arguments->websocket) {
        ws_close(conn->taos_ws);
    } else {
#endif
        if(conn->taos) {
            taos_close(conn->taos);
            conn->taos = NULL;
        }
        if (conn->ctaos) {
            taos_close(conn->ctaos);
            conn->ctaos = NULL;
        }
#ifdef WEBSOCKET
    }
#endif
    tmfree(conn);
}

int32_t queryDbExecRest(char *command, char* dbName, int precision,
                    int iface, int protocol, bool tcp, int sockfd) {
    int32_t code = postProceSql(command,
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
#ifdef WEBSOCKET
    if (g_arguments->websocket) {
        WS_RES* res = ws_query_timeout(conn->taos_ws,
                                       command, g_arguments->timeout);
        code = ws_errno(res);
        if (code != 0) {
            errorPrint("Failed to execute <%s>, code: 0x%08x, reason: %s\n",
                       command, code, ws_errstr(res));
        }
        ws_free_result(res);
    } else {
#endif
        TAOS_RES *res = taos_query(conn->taos, command);
        code = taos_errno(res);
        if (code) {
            printErrCmdCodeStr(command, code, res);
        } else {
            taos_free_result(res);
        }
#ifdef WEBSOCKET
    }
#endif
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
        ERROR_EXIT("too long request");
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

static int getCodeFromResp(char *responseBuf) {
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

int postProceSql(char *sqlstr, char* dbName, int precision, int iface,
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
    // compatibility 2.6
    if (-1 == g_arguments->rest_server_ver_major) {
        // confirm version is 2.x according to "succ"
        if (NULL != strstr(responseBuf, succMessage) && iface == REST_IFACE) {
            g_arguments->rest_server_ver_major = 2;
        }
    }

    if (NULL != strstr(responseBuf, resHttpOk) && iface == REST_IFACE) {
        // if taosd is not starting , rest_server_ver_major can't be got by 'select server_version()' , so is -1
        if (-1 == g_arguments->rest_server_ver_major || 3 <= g_arguments->rest_server_ver_major) {
            code = getCodeFromResp(responseBuf);
        } else {
            code = 0;
        }
        goto free_of_post;
    }

    if (2 == g_arguments->rest_server_ver_major) {
        if (NULL != strstr(responseBuf, succMessage) && iface == REST_IFACE) {
            code = getCodeFromResp(responseBuf);
        } else {
            code = 0;
        }
        goto free_of_post;
    }

    if (NULL != strstr(responseBuf, influxHttpOk) &&
            protocol == TSDB_SML_LINE_PROTOCOL && iface == SML_REST_IFACE) {
        code = 0;
        goto free_of_post;
    }

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

// fetch result fo file or nothing
int64_t fetchResult(TAOS_RES *res, threadInfo *pThreadInfo) {
    TAOS_ROW    row        = NULL;
    int         num_fields = 0;
    int64_t     totalLen   = 0;
    TAOS_FIELD *fields     = 0;
    int64_t     rows       = 0;
    char       *databuf    = NULL;
    bool        toFile     = strlen(pThreadInfo->filePath) > 0;
    

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
                appendResultBufToFile(databuf, pThreadInfo->filePath);
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
        appendResultBufToFile(databuf, pThreadInfo->filePath);
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
    } else if (0 == strCompareN(type, "utinyint", length)) {
        return TSDB_DATA_TYPE_UTINYINT;
    } else if (0 == strCompareN(type, "smallint", length)) {
        return TSDB_DATA_TYPE_SMALLINT;
    } else if (0 == strCompareN(type, "usmallint", length)) {
        return TSDB_DATA_TYPE_USMALLINT;
    } else if (0 == strCompareN(type, "int", length)) {
        return TSDB_DATA_TYPE_INT;
    } else if (0 == strCompareN(type, "uint", length)) {
        return TSDB_DATA_TYPE_UINT;
    } else if (0 == strCompareN(type, "bigint", length)) {
        return TSDB_DATA_TYPE_BIGINT;
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
    if (pData == NULL) {
        return NULL;
    }

    if (benchArrayEnsureCap(pArray, pArray->size + elems) != 0) {
        return NULL;
    }

    void* dst = BARRAY_GET_ELEM(pArray, pArray->size);
    memcpy(dst, pData, pArray->elemSize * elems);
    if (free) {
        tmfree(pData); // TODO remove this
    }
    pArray->size += elems;
    return dst;
}

FORCE_INLINE void* benchArrayPush(BArray* pArray, void* pData) {
    return benchArrayAddBatch(pArray, pData, 1, true);
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
    if (tcp
            && iface == SML_REST_IFACE
            && protocol == TSDB_SML_TELNET_PROTOCOL) {
        // telnet_tcp_port        
        if (convertHostToServAddr(g_arguments->host,
                    g_arguments->telnet_tcp_port,
                    &(g_arguments->serv_addr))) {
            errorPrint("%s\n", "convert host to server address");
            return -1;
        }
        infoPrint("convertServAddr host=%s telnet_tcp_port:%d to serv_addr=%p iface=%d \n", 
                g_arguments->host, g_arguments->telnet_tcp_port, &g_arguments->serv_addr, iface);
    } else {
        int port = g_arguments->port_inputted ? g_arguments->port:DEFAULT_REST_PORT;
        if (convertHostToServAddr(g_arguments->host,
                                    port,
                    &(g_arguments->serv_addr))) {
            errorPrint("%s\n", "convert host to server address");
            return -1;
        }
        infoPrint("convertServAddr host=%s port:%d to serv_addr=%p iface=%d \n", 
                g_arguments->host, port, &g_arguments->serv_addr, iface);
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
    char buff[512];
    char *msg = cmd;
    if (strlen(cmd) > sizeof(msg)) {
        memcpy(buff, cmd, 500);
        buff[500] = 0;
        strcat(buff, "...");
        msg = buff;
    }
    errorPrint("failed to run error code: 0x%08x, reason: %s command %s\n",
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