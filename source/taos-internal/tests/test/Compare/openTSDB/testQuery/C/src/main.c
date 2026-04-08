#include <stdio.h>
#include <error.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <argp.h>
#include <curl/curl.h>
#include <curl/curl.h>
#include <json/json.h>

#include "comp_lib.h"

#define MAX_QUERY_COMMANDS 20
#define MAX_HOST_SIZE 64

char *query_commands[MAX_QUERY_COMMANDS];
int   number_of_querys;

int loadQueryCommand(char * query_file);
void * query_routine(void * args);

const char *argp_program_bug_address = "<support@taosdata.com>";
static char doc[] = "";
static char args_doc[] = "[OPTIONS]...";

#define OPT_ABORT  1            /* –abort */

#define OPT_ABORT  1            /* –abort */

typedef struct arguments {
    // connection option
    char* host;
    // query information
    int  query_time;
    int nconnections;
    char *command_file;
    int abort;
} SArguments;

typedef struct {
    int threadID;
    int query_time;
    char host[MAX_HOST_SIZE];
} SThreadArg;

static struct argp_option options[] = {
    // connection option
    {"host"     , 'h' , "HOST"       , 0 , "Server host dumping data from. Default is localhost."     , 0} ,
    // query information
    {"query-time"   , 'Q' , "QUERY_TIME"   , 0 , "Query time for each command" , 1} ,
    {"connections"  , 'C' , "CONNECTIONS"  , 0 , "Threads to write data"       , 1} ,
    {"command-file" , 'F' , "COMMAND_FILE" , 0 , "Command file to query"       , 1} ,
    { 0 }
};

static error_t parse_opt (int key, char *arg, struct argp_state *state)
{
    /* Get the input argument from argp_parse, which we
       know is a pointer to our arguments structure. */
    struct arguments *arguments = state->input;

    switch (key) {
        // connection option
        case 'h':
            arguments->host = arg;
            break;
        // schema information
        case 'Q':
            arguments->query_time = atoi(arg);
            break;
        case 'C':
            arguments->nconnections = atoi(arg);
            break;
        case 'F':
            arguments->command_file = arg;
            break;
        case OPT_ABORT:
            arguments->abort = 1;
            break;
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static struct argp argp = { options, parse_opt, args_doc, doc };

int main(int argc, char *argv[]) {

    char * end_point = "/api/query";

    SArguments arguments = {
        "http://localhost:4242", // host
        1,                   // query time
        1,                   // nconnections
        "/home/taos/Documents/Comparison/openTSDB/testQuery/query_cmd.txt", // command_file
        0
    };

    memset(query_commands, 0, sizeof(char *)*MAX_QUERY_COMMANDS);
    number_of_querys = 0;

    argp_parse (&argp, argc, argv, 0, 0, &arguments);

    if (arguments.abort)
        error (10, 0, "ABORTED");

    if (loadQueryCommand(arguments.command_file) < 0) {
        fprintf(stderr, "Failed to load query commands\n");
        exit(EXIT_FAILURE);
    }

    pthread_t * threads = (pthread_t *)calloc(arguments.nconnections, sizeof(pthread_t));
    if (threads == NULL) {
        fprintf(stderr, "failed to allocate thread memory\n");
        exit(EXIT_FAILURE);
    }

    SThreadArg * threadArgs = (SThreadArg *)calloc(arguments.nconnections, sizeof(SThreadArg));
    if (threadArgs == NULL);
    if (threadArgs == NULL) {
        fprintf(stderr, "failed to allocate thread memory\n");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < arguments.nconnections; i++) {
        SThreadArg * pArg = threadArgs + i;
        pArg->threadID = i;
        pArg->query_time = arguments.query_time;
        sprintf(pArg->host, "%s%s", arguments.host, end_point);
        pthread_create(threads+i, NULL, query_routine, pArg);
    }

    for (int i = 0; i < arguments.nconnections; i++) {
        pthread_join(threads[i], NULL);
    }

    taosMemoryFree(threads);
    taosMemoryFree(threadArgs);
    for (int i = 0; i < number_of_querys; i++) {
        taosMemoryFree(query_commands[i]);
    }

    return 0;
}

size_t write_function(void *ptr, size_t size, size_t nmemb, void *s) {
    /* printf("Total return data size: %ld\n", size * nmemb); */
    return size * nmemb;
}

CURLcode sendHttpRequest(CURL * handle, char *url, const char * data) {
    // reset curl handle
    curl_easy_reset(handle);

    // set options
    curl_easy_setopt(handle, CURLOPT_URL, url);
    curl_easy_setopt(handle, CURLOPT_POST, 1L);
    curl_easy_setopt(handle, CURLOPT_POSTFIELDS, data);
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, write_function);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, NULL);

    return curl_easy_perform(handle);
}

void * query_routine(void * args) {

    SThreadArg * pthreadArg = (SThreadArg *) args;
    CURL * curl = NULL;
    CURLcode res;

    curl = curl_easy_init();
    if (curl == NULL) return NULL;

    for (int i = 0; i < number_of_querys; i++) {
        printf("THREADID: %d Command %d: %s----------\n", pthreadArg->threadID, i, query_commands[i]);
        for (int k = 0; k < pthreadArg->query_time; k++) {
            /* printf("host: %s\n", pthreadArg->host); */
            double st = get_curr_time_in_sec();
            res = sendHttpRequest(curl, pthreadArg->host, query_commands[i]);
            if (res != CURLE_OK) {
                fprintf(stderr, "failed to send http request\n");
                curl_easy_cleanup(curl);
                return NULL;
            }

            double et = get_curr_time_in_sec() - st;
            printf("    THREADID: %d Spent %f seconds to retrieve data\n", pthreadArg->threadID, et);
        }
    }
    
    curl_easy_cleanup(curl);
    return NULL;
}

int loadQueryCommand(char * query_file) {

    ssize_t read_size = 0;
    char * line = NULL;
    size_t line_size = 0;

    FILE * fp = fopen(query_file, "r");
    if (fp == NULL) return -1;

    while ((read_size = getline(&line, &line_size, fp)) > 0) {
        line[read_size-1] = '\0';
        if (regex_match(line, "^#.*$") || regex_match(line, "^\\s*$")) continue;
        query_commands[number_of_querys++] = strdup(line);
        if (number_of_querys >= MAX_QUERY_COMMANDS) break;
    }

    if (line) taosMemoryFree(line);
}
