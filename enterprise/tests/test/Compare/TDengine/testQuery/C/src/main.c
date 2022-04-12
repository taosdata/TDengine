/* Code
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <argp.h>
#include <error.h>
#include <pthread.h>

#include <taos/taos.h>

#include "comp_lib.h"

#define MAX_QUERY_COMMANDS 100

int load_query_command();
void free_query_command();
void * query_routine(void *args);

void free_query_command();
int query_command_size = 0;
char *query_commands[MAX_QUERY_COMMANDS];

const char *argp_program_bug_address = "<support@taosdata.com>";
static char doc[] = "";
static char args_doc[] = "[OPTIONS]...";

#define OPT_ABORT  1            /* –abort */

typedef struct arguments {
    // connection option
    char* host;
    char* user;
    char* password;
    int   port;
    char * config;
    // query information
    char * db_name;
    int  query_time;
    int nconnections;
    char *command_file;
    int abort;
} SArguments;

typedef struct {
    int threadId;
    SArguments * args;
} SThreadInfo;

static struct argp_option options[] = {
    // connection option
    {"host"     , 'h' , "HOST"       , 0 , "Server host dumping data from. Default is localhost."     , 0} ,
    {"user"     , 'u' , "USER"       , 0 , "User name used to connect to server. Default is root."    , 0} ,
    {"password" , 'p' , "PASSWORD"   , 0 , "User password to connect to server. Default is taosdata." , 0} ,
    {"port"     , 'P' , "PORT"       , 0 , "Port to connect"                                          , 0} ,
    {"config"   , 'c' , "CONFIG_DIR" , 0 , "Configure directory. Default is /etc/taos/taos.cfg."      , 0} ,
    // query information
    {"db_name"      , 'B' , "DB"           , 0 , "DB name"                     , 1} ,
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
        case 'u':
            arguments->user = arg;
            break;
        case 'p':
            arguments->password = arg;
            break;
        case 'P':
            arguments->port = atoi(arg);
            break;
        case 'c':
            arguments->config = arg;
            break;
        // schema information
        case 'B':
            arguments->db_name = arg;
            break;
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

/* Our argp parser. */
static struct argp argp = { options, parse_opt, args_doc, doc };

int main(int argc, char *argv[]) {
    TAOS_RES * result = NULL;
    TAOS_ROW   row    = NULL;

    /* Configuration */
    SArguments arguments = {
        NULL,        // host
        "root",      // user
        "taosdata",  // password
        0,           // port
        NULL,        // config
        "db",        // db_name
        5,           // query_time
        1,           // nconnections
        "/home/taos/Documents/Comparison/TDengine/testQuery/query_cmd.txt",          // command_file
        0            // abort
    };

    argp_parse (&argp, argc, argv, 0, 0, &arguments);

    if (arguments.abort)
        error (10, 0, "ABORTED");

    /* ------------------------------------------Load query commands------------------------------------------ */
    if (load_query_command(arguments.command_file) < 0) {
        fprintf(stderr, "failed to load query commands\n");
        exit(EXIT_FAILURE);
    }

    if (arguments.config) strcpy(configDir, arguments.config);

    pthread_t * threads = (pthread_t *)calloc(arguments.nconnections, sizeof(pthread_t));
    SThreadInfo * threadInfo = (SThreadInfo *)calloc(arguments.nconnections, sizeof(SThreadInfo));
    for (int i = 0; i < arguments.nconnections; i++) {
        threadInfo[i].threadId = i+1;
        threadInfo[i].args = &arguments;
        pthread_create(threads+i, NULL, query_routine, (void *)(threadInfo+i));
    }
    for (int i = 0; i < arguments.nconnections; i++) {
        pthread_join(threads[i], NULL);
    }

    taosMemoryFree(threads);
    taosMemoryFree(threadInfo);

_exit:
    free_query_command();
}

void * query_routine(void *args) {
    TAOS_RES * result = NULL;
    TAOS_ROW row = NULL;

    SThreadInfo * threadInfo = (SThreadInfo *)args;

    SArguments * arguments = threadInfo->args;

    TAOS * taos = taos_connect(arguments->host, arguments->user, arguments->password, arguments->db_name, arguments->port);
    if (taos == NULL) {
        fprintf(stderr, "failed to connect to server\n");
        return NULL;
    }

    for (int i = 0; i < query_command_size; i++) {
        printf("THREADID: %d Command %d: %s----------\n", threadInfo->threadId, i, query_commands[i]);
        double tt = 0;

        for (int j = 0; j < arguments->query_time; j++) {
            double st = get_curr_time_in_sec();

            if (taos_query(taos, query_commands[i]) < 0) {
                fprintf(stderr, "THREADID: %d failed to run command: %s\n", threadInfo->threadId, query_commands[i]);
                break;
            }

            result = taos_use_result(taos);
            if (result == NULL) {
                fprintf(stderr, "failed to use result\n");
                break;
            }

            int count = 0;
            while (1) {
                row = taos_fetch_row(result);
                if (row == NULL) break;
                count ++;
            }

            taos_free_result(result);
            result = NULL;

            double et = get_curr_time_in_sec();

            printf("    THREADID: %d Query %d, using %f seconds to retrieve %d records\n", threadInfo->threadId, j+1, et-st, count);
            tt += (et - st);
        }
        printf("    THREADID: %d Average time: %f seconds===============\n", threadInfo->threadId, tt / arguments->query_time);
    }

    taos_close(taos);
    return NULL;
}

int load_query_command(char *fquery) {
    ssize_t read_size;

    FILE * f = fopen(fquery, "r");
    if (f == NULL) return -1;

    while (1) {
        size_t  line_size = 0;

        read_size = getline(&(query_commands[query_command_size]), &line_size, f);
        if (read_size < 0) break;
        query_commands[query_command_size][read_size-1] = '\0';
        query_command_size ++;
    }

    return 0;
}

void free_query_command() {
    for (int i = 0; i < MAX_QUERY_COMMANDS; i++) {
        if (query_commands[i] != NULL) 
            taosMemoryFree(query_commands[i]);
    }
}
    
