/* This code is used to insert data to TDengine.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <argp.h>
#include <semaphore.h>
#include <stdbool.h>
#include <error.h>

#include <taos/taos.h>

#include "insert.h"
#include "comp_lib.h"

#define MAX_TAG_STR_SIZE 128
#define MAX_TAG_VALUE_SIZE 64
#define MAX_TAG_NUMBER 6

typedef struct {
    char value[MAX_TAG_VALUE_SIZE];
    double percent;
} STag;

typedef struct {
    int  number_of_tag_values;
    char tag_str[MAX_TAG_STR_SIZE];
    STag tags[];
} STagInfo;

typedef struct {
    int   number_of_tags;
    char * data_schema;
    STagInfo *tags[MAX_TAG_NUMBER];
} SSchemaInfo;

SSchemaInfo schema_info;
char *sample_data[MAX_SAMPLE_DATA_SIZE];
int   sample_data_size = 0;

int loadSampleData(char *fschema, char *fsample);

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
    // Schema information
    char * db_name;
    char * db_property;
    char * tb_prefix;
    char * fschema;
    char * fsample;
    char * stable;
    // Data information
    int ntables;
    int64_t nrecords_per_table;
    int nrecords_per_request;
    int nconnections;
    // Time information
    int64_t start_time;
    int64_t time_interval;
    bool is_real_situation;
    // Insert mode
    enum INSERT_MODE insert_mode;
    int abort;
} SArguments;

static struct argp_option options[] = {
    // connection option
    {"host"     , 'h' , "HOST"       , 0 , "Server host dumping data from. Default is localhost."     , 0} ,
    {"user"     , 'u' , "USER"       , 0 , "User name used to connect to server. Default is root."    , 0} ,
    {"password" , 'p' , "PASSWORD"   , 0 , "User password to connect to server. Default is taosdata." , 0} ,
    {"port"     , 'P' , "PORT"       , 0 , "Port to connect"                                          , 0} ,
    {"config"   , 'c' , "CONFIG_DIR" , 0 , "Configure directory. Default is /etc/taos/taos.cfg."      , 0} ,
    // schema information
    {"db_name"     , 'B' , "DB"          , 0 , "DB name"           , 1} ,
    {"db_property" , 'T' , "DB_PROPERTY" , 0 , "Database property" , 1} ,
    {"tb_prefix"   , 't' , "TB_PREFIX"   , 0 , "Table prefix"      , 1} ,
    {"schem_file"  , 's' , "SCHEMA_FILE" , 0 , "Schema file"       , 1} ,
    {"sample_file" , 'm' , "SAMPLE_FILE" , 0 , "Sample file"       , 1} ,
    {"stable"      , 'b' , "SUPER_TABLE" , 0 , "Super table"       , 1} ,
    // data information
    {"ntables"             , 'n' , "NTABLES"             , 0 , "Number of tables."     , 2} ,
    {"records_per_table"   , 'i' , "RECORDS_PER_TABLE"   , 0 , "Records per table."    , 2} ,
    {"records_pre_request" , 'e' , "RECORDS_PER_REQUEST" , 0 , "Records per request"   , 2} ,
    {"connections"         , 'C' , "CONNECTIONS"         , 0 , "Threads to write data" , 2} ,
    // dump format options
    {"start_time"      , 'S' , "START_TIME"    , 0 , "Start time"      , 3} ,
    {"time_interval"   , 'v' , "TIME_INTERVAL" , 0 , "Time interval"   , 3} ,
    {"real_similation" , 'r' , 0               , 0 , "Real simulation" , 3} ,
    {"insert_mode"     , 'M' , "INSERT_MODE"   , 0 , "Insert mode"     , 3} ,
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
        case 'T':
            printf("%s\n", arg);
            arguments->db_property = arg;
            break;
        case 't':
            arguments->tb_prefix = arg;
            break;
        case 's':
            arguments->fschema = arg;
            break;
        case 'm':
            arguments->fsample = arg;
            break;
        case 'b':
            arguments->stable = arg;
            break;
        // data information
        case 'n':
            arguments->ntables = atoi(arg);
            break;
        case 'i':
            arguments->nrecords_per_table = atol(arg);
            break;
        case 'e':
            arguments->nrecords_per_request = atoi(arg);
            break;
        case 'C':
            arguments->nconnections = atoi(arg);
            break;
        case 'S':
            arguments->start_time = atol(arg);
            break;
        case 'v':
            arguments->time_interval = atol(arg);
            break;
        case 'r':
            arguments->is_real_situation = true;
            break;
        case 'M':
            arguments->insert_mode = atoi(arg);
            break;
        case OPT_ABORT:
            arguments->abort = 1;
            break;
        /* case ARGP_KEY_ARG: */
        /*     arguments->arg_list = &state->argv[state->next-1]; */
        /*     arguments->arg_list_len = state->argc - state->next + 1; */
        /*     state->next = state->argc; */
        /*     break; */
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

/* Our argp parser. */
static struct argp argp = { options, parse_opt, args_doc, doc };

#ifndef __TEST__
int main(int argc, char * argv[]) {

    char command[BUFFER_SIZE] = "\0";

    /* Configuration */
    SArguments arguments = {
        NULL,           // host
        "root",         // user
        "taosdata",     // password
        0,              // port
        NULL,           // config
        "db",           // db_name
        NULL,           // db_property
        "machine",      // tb_prefix
        "/home/taos/Documents/Comparison/data/schema.txt",  // fschema
        "/home/taos/Documents/Comparison/data/sample.txt", // fsample
        "mt",           // stable
        100,            // ntables
        1000000,         // nrecords_per_table
        200,            // nrecords_per_request
        5,              // nconnections
        1493568000000,  // start_time
        2,              // time_interval
        false,          // is_real_situation
        TABLE_BY_TABLE,  // insert_mode
        /* TIME_BY_TIME,  // insert_mode */
        0
    };

    argp_parse (&argp, argc, argv, 0, 0, &arguments);

    if (arguments.abort)
        error (10, 0, "ABORTED");

    /* printf("host:%s\n", arguments.host); */
    /* printf("user:%s\n", arguments.user); */
    /* printf("password:%s\n", arguments.password); */
    /* printf("config:%s\n", arguments.config); */
    /* printf("db:%s\n", arguments.db_name); */
    /* printf("db_property:%s\n", arguments.db_property); */
    /* printf("tb_prefix:%s\n", arguments.tb_prefix); */
    /* printf("ntables:%d\n", arguments.ntables); */
    /* printf("nrecords_per_table:%ld\n", arguments.nrecords_per_table); */
    /* printf("nrecords_per_request:%d\n", arguments.nrecords_per_request); */
    /* printf("nconnections:%d\n", arguments.nconnections); */
    /* printf("start_time:%ld\n", arguments.start_time); */
    /* printf("time_interval:%ld\n", arguments.time_interval); */
    /* printf("is_real_situation:%d\n", (int)(arguments.is_real_situation)); */
    /* printf("insert_mode:%d\n", (int)(arguments.insert_mode)); */

    // load sample data
    if (loadSampleData(arguments.fschema, arguments.fsample) < 0) {
        fprintf(stderr, "failed to load sample data\n");
        exit(EXIT_FAILURE);
    }

    /* ==================================Create Database and tables================================== */
    if (arguments.config) strcpy(configDir, arguments.config);
    TAOS * taos = taos_connect(arguments.host, arguments.user, arguments.password, NULL, arguments.port);
    if (taos == NULL) {
        fprintf(stderr,"Failed to connect to TDengine, reason:%s\n", taos_errstr(taos));
        exit(EXIT_FAILURE);
    }

    // Create database
    if (arguments.db_property) {
        sprintf(command, "create database if not exists %s %s", arguments.db_name, arguments.db_property);
    } else {
        sprintf(command, "create database if not exists %s", arguments.db_name);
    }
    if (taos_query(taos, command) != 0) {
        fprintf(stderr, "failed to create database, reason: %s\n", taos_errstr(taos));
        exit(EXIT_FAILURE);
    }

    sprintf(command, "use %s", arguments.db_name);
    taos_query(taos, command);

    // create metric
    char *pstr = command;
    pstr += sprintf(pstr, "create table if not exists %s (ts timestamp, %s) tags (", arguments.stable, schema_info.data_schema);
    if (schema_info.number_of_tags == 0) {
        pstr += sprintf(pstr, "tablename binary(50))");
    } else {
        for (int i = 0; i < schema_info.number_of_tags; i++) {
            if (i < schema_info.number_of_tags -1 ) {
                pstr += sprintf(pstr, "%s, ", schema_info.tags[i]->tag_str);
            } else {
                pstr += sprintf(pstr, "%s", schema_info.tags[i]->tag_str);
            }
        }
        pstr += sprintf(pstr, ")");
    }
    if (taos_query(taos, command)) {
        fprintf(stderr, "failed to create metric, reason: %s\n", taos_errstr(taos));
        exit(EXIT_FAILURE);
    }


    // Create all the tables;
    printf("Creating %d tables......\n", arguments.ntables);
    for (int i = 0; i < arguments.ntables; i++) {
        pstr = command;
        pstr += sprintf(pstr, "create table if not exists %s%d using %s tags (", arguments.tb_prefix, i, arguments.stable);
        if (schema_info.number_of_tags == 0) {
            pstr += sprintf(pstr, "'%s%d')", arguments.tb_prefix, i);
        } else {
            for (int j = 0; j < schema_info.number_of_tags; j++) {
                STagInfo * ttag = schema_info.tags[j];
                double spercent = 0;
                for (int k = 0; k < ttag->number_of_tag_values; k++) {
                    spercent += ttag->tags[k].percent;
                    if (i+1 <= arguments.ntables * spercent) {
                        if (j < schema_info.number_of_tags -1) {
                            pstr += sprintf(pstr, "%s, ", ttag->tags[k].value);
                        } else {
                            pstr += sprintf(pstr, "%s", ttag->tags[k].value);
                        }

                        break;
                    } else {
                        continue;
                    }
                }
            }
            pstr += sprintf(pstr, ")");
        }

        if (taos_query(taos, command) != 0) {
            fprintf(stderr, "failed to create table, reason: %s\n", taos_errstr(taos));
        }
    }

    taos_close(taos);

    printf("TDengine tables are created. Sleep 2 seconds and starting to insert data...\n\n");

    sleep(2);

    /* ==================================Inserting data================================== */
    double ts = get_curr_time_in_sec();
    srand(time(NULL));

    printf("Inserting data......\n");
    pthread_t * pids = taosMemoryMalloc(arguments.nconnections * sizeof(pthread_t));
    info * infos = taosMemoryMalloc(arguments.nconnections * sizeof(info));
    int a = arguments.ntables / arguments.nconnections;
    int b = arguments.ntables % arguments.nconnections;
    int last = 0;
    for (int i = 0; i < arguments.nconnections; i++) {
        info * t_info = infos+i;
        t_info->threadID = i;
        strcpy(t_info->db_name, arguments.db_name);
        strcpy(t_info->tb_prefix, arguments.tb_prefix);
        t_info->nrecords_per_table = arguments.nrecords_per_table;
        t_info->start_time = arguments.start_time;
        t_info->time_interval = arguments.time_interval;
        t_info->insert_mode = arguments.insert_mode;
        t_info->is_real_situation = arguments.is_real_situation;
        t_info->taos = taos_connect(arguments.host, arguments.user, arguments.password, arguments.db_name, arguments.port);
        if (t_info->taos == NULL) {
            fprintf(stderr, "failed to connect to server\n");
            exit(EXIT_FAILURE);
        }
        t_info->nrecords_per_request = arguments.nrecords_per_request;
        t_info->start_table_id = last;
        t_info->end_table_id = i < b? last + a: last + a - 1;
        last = t_info->end_table_id + 1;

        sem_init(&(t_info->mutex_sem), 0, 1);
        t_info->notFinished = t_info->end_table_id - t_info->start_table_id + 1;
        sem_init(&(t_info->lock_sem), 0, 0);

        pthread_create(pids+i, NULL, sync_write, t_info);
    }

    for (int i = 0; i < arguments.nconnections; i++){
        pthread_join(pids[i], NULL);
    }

    double t = get_curr_time_in_sec() - ts;
    printf("Done! Spent %10.4f seconds to insert %ld records, speed: %12.2f R/s\n", t, 1L * arguments.ntables*arguments.nrecords_per_table, (1L*arguments.ntables*arguments.nrecords_per_table)/t);

    for (int i = 0; i < arguments.nconnections; i++){
        info * t_info = infos + i;

        sem_destroy(&(t_info->mutex_sem));
        sem_destroy(&(t_info->lock_sem));
        taos_close(t_info->taos);
    }

    taosMemoryFree(pids);
    taosMemoryFree(infos);

    return 0;
}
#endif

void queryDB(TAOS * taos, char * command) {
    if (taos_query(taos, command) != 0) {
        fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(taos) );
        taos_close(taos);
        exit(EXIT_FAILURE);
    }
}

/* int randValue() { */
/*     return rand() % 5; */
/* } */
// sync insertion
void * sync_write(void * sarg) {
    info * winfo = (info * )sarg;
    char * buffer = taosMemoryMalloc(65536);
    char *pStr = NULL;
    int   count = 0;
    int   sample_data_counter = 0;
    double st = 0;
    double et = 0;

    printf("ThreadID: %d start table ID: %d end table ID: %d\n", winfo->threadID, winfo->start_table_id, winfo->end_table_id);

    pStr = buffer;

    if (winfo->insert_mode == TABLE_BY_TABLE) {
        pStr += sprintf(pStr, "insert into ");

        for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++) {

            long time_counter=winfo->start_time;
            pStr += sprintf(pStr, "%s%d values ", winfo->tb_prefix, tID);

            for (int64_t i = 0; i < winfo->nrecords_per_table; i++) {
                time_counter = winfo->start_time + i * winfo->time_interval;
                pStr += sprintf(pStr, "(%ld, %s) ", time_counter, sample_data[sample_data_counter]);
                sample_data_counter = (sample_data_counter + 1) % sample_data_size;

                count++;
                if (count >= winfo->nrecords_per_request) {
                    queryDB(winfo->taos, buffer);

                    count = 0;
                    pStr = buffer;
                    if (i != winfo->nrecords_per_table -1)
                        pStr += sprintf(pStr, "insert into %s%d values ", winfo->tb_prefix, tID);
                    else
                        pStr += sprintf(pStr, "insert into ");
                }

            }

        }

        if (count > 0) {
            queryDB(winfo->taos, buffer);
        }

    } else if (winfo->insert_mode == TIME_BY_TIME) {
        long time_counter=winfo->start_time;
        pStr += sprintf(pStr, "insert into ");

        for (int64_t i = 0; i < winfo->nrecords_per_table; i++){
            time_counter = winfo->start_time + i * winfo->time_interval;

            for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++){

                /* int val = randValue(); */
                pStr += sprintf(pStr, "%s%d values (%ld, %s) ", winfo->tb_prefix, tID, time_counter, sample_data[sample_data_counter]);
                sample_data_counter = (sample_data_counter + 1) % sample_data_size;
                count++;

                if (count >= winfo->nrecords_per_request) {
                    queryDB(winfo->taos, buffer);

                    count = 0;
                    pStr = buffer;
                    pStr += sprintf(pStr, "insert into ");
                }
            }

            if (winfo->is_real_situation) {
                sleep(winfo->time_interval/1000);
            }
        }

        if (count > 0) {
            queryDB(winfo->taos, buffer);
        }
    } 

    taosMemoryFree(buffer);

    return NULL;
}

int loadSampleData(char *fschema, char *fsample) {
    // TODO : Change directory here configurable
    char * line = NULL;
    size_t  line_size = 0;
    ssize_t read_size = 0;
    memset(&schema_info, 0, sizeof(SSchemaInfo));
    char * saveptr1 = NULL;
    char * saveptr2 = NULL;
    char * saveptr3 = NULL;
    char * saveptr4 = NULL;
    char * token1 = NULL;
    char * token2 = NULL;
    char * token3 = NULL;

    // Read schema file
    FILE * f = fopen(fschema, "r");
    if (f == NULL) return -1;

    while ((read_size = getline(&line, &line_size, f)) > 0) {
        line[read_size-1] = '\0';
        if (regex_match(line, "^#.*$") || regex_match(line, "^\\s*$")) continue;
        /* printf("%s\n", line); */

        // Get value
        if (strchr(line, ':') == NULL) {
            // This is the data shcema
            if (schema_info.data_schema) {
                fprintf(stderr, "Duplicate data schema definition\n");
                fclose(f);
                return -1;
            }
            schema_info.data_schema = strdup(line);
        } else {
            // This is the tag schema
            if (schema_info.number_of_tags >= MAX_TAG_NUMBER) break;

            token1 = strtok_r(line, ":", &saveptr1);
            token2 = strtok_r(NULL, ":", &saveptr1);
            /* printf("token1: %s, token2: %s\n", token1, token2); */

            int number_of_values = 0;
            for (char * c = token2; *c != '\0'; c++) {
                if (*c == ',') number_of_values ++;
            }

            if (number_of_values == 0) {
                fprintf(stderr, "Please assign tag values for tag\n");
                return -1;
            }

            number_of_values++;

            schema_info.tags[schema_info.number_of_tags] = (STagInfo *) taosMemoryMalloc(sizeof(STagInfo)+sizeof(STag)*number_of_values);
            schema_info.tags[schema_info.number_of_tags]->number_of_tag_values = number_of_values;

            strcpy(schema_info.tags[schema_info.number_of_tags]->tag_str, token1);
            double total_percent = 0;
            for (int idx = 0;;token2 = NULL) {
                token3 = strtok_r(token2, ",", &saveptr3);
                if (token3 == NULL) break;

                char * token4 = strtok_r(token3, " ", &saveptr4);
                char * token5 = strtok_r(NULL, " ", &saveptr4);
                strcpy(schema_info.tags[schema_info.number_of_tags]->tags[idx].value, token4);
                schema_info.tags[schema_info.number_of_tags]->tags[idx].percent = atof(token5);
                total_percent += atof(token5);

                idx ++;
            }

            if (total_percent != 1) {
                fprintf(stderr, "Invalid tag percent distribution\n");
                taosMemoryFree(line);
                return -1;
            }

            schema_info.number_of_tags ++;
        }

    }
    taosMemoryFree(line);
    fclose(f);
    f = NULL;

    // Read sample data file
    f = fopen(fsample, "r");
    if (f == NULL) return -1;
    while (1) {
        if (sample_data_size >= MAX_SAMPLE_DATA_SIZE) break;
        line_size = 0;

        if ((read_size = getline(&sample_data[sample_data_size], &line_size, f)) < 0) break;
        sample_data[sample_data_size][read_size-1] = '\0';
        sample_data_size ++;
    }
    fclose(f);
    return 0;
}

void freeSampleData() {
    if (schema_info.data_schema != NULL) taosMemoryFree(schema_info.data_schema);
    for (int i = 0; i < schema_info.number_of_tags; i++) {
        if (schema_info.tags[i] != NULL) taosMemoryFree(schema_info.tags[i]);
    }
    for (int i = 0; i < sample_data_size; i++) taosMemoryFree(sample_data[i]); 
}

#ifdef __TEST__
int main(int argc, char * argv[]) {
    loadSampleData("/home/taos/Documents/Comparison/data/schema.txt", 
            "/home/taos/Documents/Comparison/data/sample.txt");
}
#endif
