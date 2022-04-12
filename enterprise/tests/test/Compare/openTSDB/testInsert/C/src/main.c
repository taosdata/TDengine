#include <stdio.h>
#include <stdlib.h>
#include <error.h>
#include <pthread.h>
#include <string.h>
#include <argp.h>
#include <curl/curl.h>
#include <json/json.h>

#include "comp_lib.h"

#define MAX_URL_LENGTH 64
#define MAX_TAG_PREFIX_LENGTH 32
#define MAX_TAG_VALUE_LEN 32
#define MAX_TAG_VALUES    12
#define MAX_NUM_OF_TAGS   6
#define MAX_METRIC_NAME_LENGTH 128
#define MAX_SAMPLE_DATA_SIZE 1000

enum data_type {DATA_TYPE_INT, DATA_TYPE_FLOAT, DATA_TYPE_STRING};

typedef struct {
    char value[MAX_TAG_VALUE_LEN];
    double percent;
} STagValue;

typedef struct {
    char key[MAX_TAG_VALUE_LEN];
    int  number_of_values;
    STagValue values[MAX_TAG_VALUES];
} STagInfo;

typedef struct {
    int number_of_tags;
    STagInfo tags[MAX_NUM_OF_TAGS];
} STags;

typedef struct {
    char metric_name[MAX_METRIC_NAME_LENGTH];
    char *sample_value[MAX_SAMPLE_DATA_SIZE];
    enum data_type dtype;
} SMetricInfo;

typedef struct {
    int  threadId;
    char tag_prefix[MAX_TAG_PREFIX_LENGTH];  // tag prefix
    char url[MAX_URL_LENGTH];  // tag prefix
    int  sId;                                // start tag ID
    int  eId;                                // end tag ID
    int  num_of_detectors;
    int  points_per_detector;
    int  points_per_request;
    int64_t start_time;
    int64_t time_inteval;
    int  nfields; 
    int  sample_size;
    SMetricInfo * sample_data;
} SWriteInfo;

CURLcode sendHttpRequest(CURL * handle, char *url, const char * data);
int loadSampleData(char * fschema, char * fsample, SMetricInfo ** sample_data, int *nfields, int *sample_size);
void freeSampleData(SMetricInfo *sample_data, int nfields);
void * write_data(void * arg);

const char *argp_program_bug_address = "<support@taosdata.com>";
static char doc[] = "";
static char args_doc[] = "[OPTIONS]...";

#define OPT_ABORT  1            /* –abort */

typedef struct arguments {
    // connection option
    char* host;
    // Schema information
    char * fschema;
    char * fsample;
    // Data information
    int num_of_detectors;
    int points_per_detector;
    int points_per_request;
    int connections;
    char * tag_prefix;
    // Time information
    int64_t start_time;
    int64_t time_interval;
    int abort;
} SArguments;

static struct argp_option options[] = {
    // connection option
    {"host"     , 'h' , "HOST"       , 0 , "Server host dumping data from. Default is localhost."     , 0} ,
    // schema information
    {"schem_file"  , 's' , "SCHEMA_FILE" , 0 , "Schema file"       , 1} ,
    {"sample_file" , 'm' , "SAMPLE_FILE" , 0 , "Sample file"       , 1} ,
    // data information
    {"ntables"             , 'n' , "NTABLES"             , 0 , "Number of detectors."  , 2} ,
    {"points_per_detector" , 'i' , "POINTS_PER_DETECTOR" , 0 , "Points per detector."  , 2} ,
    {"points_per_request"  , 'e' , "POINTS_PER_REQUEST"  , 0 , "Points per request"    , 2} ,
    {"connections"         , 'C' , "CONNECTIONS"         , 0 , "Threads to write data" , 2} ,
    {"tag_prefix"          , 't' , "TAG_PREFIX"          , 0 , "Tag prefix"            , 2} ,
    // dump format options
    {"start_time"      , 'S' , "START_TIME"    , 0 , "Start time"      , 3} ,
    {"time_interval"   , 'v' , "TIME_INTERVAL" , 0 , "Time interval"   , 3} ,
    { 0 }
};

char * trim_string(char * s) {
    char * res = s;
    int len = strlen(s);
    char *end = s+len;

    while (1) {
        if (*res != '\0' && *res != ' ' && *res != '\'') break;
        res++;
    }

    while (1) {
        if (*end != '\0' && *end != ' ' && *end != '\'') {
            *(end+1) = '\0';
            break;
        }
        end--;
    }

    return res;
}

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
        case 't':
            arguments->tag_prefix = arg;
            break;
        case 's':
            arguments->fschema = arg;
            break;
        case 'm':
            arguments->fsample = arg;
            break;
        // data information
        case 'n':
            arguments->num_of_detectors = atoi(arg);
            break;
        case 'i':
            arguments->points_per_detector = atoi(arg);
            break;
        case 'e':
            arguments->points_per_request = atoi(arg);
            break;
        case 'C':
            arguments->connections = atoi(arg);
            break;
        case 'S':
            arguments->start_time = atol(arg);
            break;
        case 'v':
            arguments->time_interval = atol(arg);
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

static struct argp argp = { options, parse_opt, args_doc, doc };

STags tag_info;

int main(int argc, char *argv[]) {

    char * end_point = "/api/put";
    char url[MAX_URL_LENGTH];
    SMetricInfo * sample_data = NULL;
    int  nfields = 0;
    int  sample_size = 0;

    SArguments arguments = {
        "http://localhost:4242",     // host
        "/home/taos/Documents/Comparison/data/schema.txt",                          // fschema
        "/home/taos/Documents/Comparison/data/sample.txt",                          // fsample
        100,                         // num_of_detectors
        10000,                        // points_per_detector
        10,                          // points_per_request
        1,                           // connections
        "monitor",                   // tag_prefix
        1545054247000,               // start_time
        1000,                       // time_interval
        0                            // abort
    };

    argp_parse (&argp, argc, argv, 0, 0, &arguments);

    if (arguments.abort)
        error (10, 0, "ABORTED");

    // Load sample data
    if (loadSampleData(arguments.fschema, arguments.fsample, &sample_data, &nfields, &sample_size) < 0) {
        fprintf(stderr, "failed to load sample data\n");
        exit(EXIT_FAILURE);
    }

    // Allocate multi-thread resources
    pthread_t * threads = (pthread_t *) calloc(arguments.connections, sizeof(pthread_t));
    if (threads == NULL) {
        fprintf(stderr, "failed to allocate memory\n");
        goto _exit_error;
    }

    SWriteInfo * pWrite = (SWriteInfo *) calloc(arguments.connections, sizeof(SWriteInfo));
    if (pWrite == NULL) {
        fprintf(stderr, "failed to allocate memory\n");
        taosMemoryFree(threads);
        goto _exit_error;
    }

    int a = arguments.num_of_detectors / arguments.connections;
    int b = arguments.num_of_detectors % arguments.connections;
    int last = 0;
    double st = get_curr_time_in_sec();
    for (int i = 0; i < arguments.connections; i++) {
        SWriteInfo * pWrite_t = pWrite + i;

        pWrite_t->threadId = i;
        strcpy(pWrite_t->tag_prefix, arguments.tag_prefix);
        sprintf(pWrite_t->url, "%s%s", arguments.host, end_point);
        pWrite_t->points_per_detector = arguments.points_per_detector;
        pWrite_t->num_of_detectors = arguments.num_of_detectors;
        pWrite_t->sample_data = sample_data;
        pWrite_t->points_per_request = arguments.points_per_request;
        pWrite_t->nfields = nfields;
        pWrite_t->sample_size = sample_size;
        pWrite_t->start_time = arguments.start_time;
        pWrite_t->time_inteval = arguments.time_interval;
        pWrite_t->sId = last;
        pWrite_t->eId = i < b ? last + a: last + a -1;
        last = pWrite_t->eId + 1;

        pthread_create(&threads[i], NULL, write_data, (void *) pWrite_t);
    }

    for (int i = 0; i < arguments.connections; i++) {
        pthread_join(threads[i], NULL);
    }

    double et = get_curr_time_in_sec() - st; 

    printf("Done! Spent %10.4f seconds to insert %ld records, speed: %12.2f R/s\n", et, 1L * arguments.num_of_detectors*arguments.points_per_detector, (1L*arguments.num_of_detectors*arguments.points_per_detector)/et);

    taosMemoryFree(pWrite);
    taosMemoryFree(threads);
    freeSampleData(sample_data, nfields);
    return 0;

_exit_error:
    freeSampleData(sample_data, nfields);
    return -1;
}

void freeSampleData(SMetricInfo *sample_data, int nfields) {
    for (int i = 0; i < nfields; i++) {
        for (int j = 0; j < MAX_SAMPLE_DATA_SIZE; j++) {
            if (sample_data[i].sample_value[j] != NULL) {
                taosMemoryFree(sample_data[i].sample_value[j]);
            } else {
                break;
            }
        }
    }

    taosMemoryFree(sample_data);

}

void * write_data(void * arg) {
    SWriteInfo * pWInfo = (SWriteInfo *) arg;
    SMetricInfo * sample_data = pWInfo->sample_data;
    CURL * curl = NULL;
    CURLcode res;
    char tagname[MAX_TAG_PREFIX_LENGTH];

    printf("Thread: %d write sId %d to eId %d\n", pWInfo->threadId, pWInfo->sId, pWInfo->eId);

    curl = curl_easy_init();
    if (curl == NULL) {
        fprintf(stderr, "threadId: %d, failed to init curl\n", pWInfo->threadId);
    }

    json_object * jobj = NULL;

    jobj = json_object_new_array();

    int count = 0;
    int sample_counter = 0;
    for (int i = 0; i < pWInfo->points_per_detector; i++) { // loop over time
        /* printf("%ld*****************************\n", pWInfo->start_time); */
        /* exit(0); */
        int64_t tt = pWInfo->start_time + i * pWInfo->time_inteval;
        for (int tID = pWInfo->sId; tID <= pWInfo->eId; tID++) { // loop over detector
            sprintf(tagname, "%s%d", pWInfo->tag_prefix, tID);
            int sample_idx = sample_counter;
            sample_counter = (sample_counter + 1) % pWInfo->sample_size;

            for (int j = 0; j < pWInfo->nfields; j++) {
                json_object * new_point = json_object_new_object();

                // Add metric name
                json_object_object_add(new_point, "metric", json_object_new_string(sample_data[j].metric_name));
                // Add timestamp
                json_object_object_add(new_point, "timestamp", json_object_new_int64(tt));
                // Add value
                switch (sample_data[j].dtype) {
                    case DATA_TYPE_INT:
                        json_object_object_add(new_point, "value", json_object_new_int(atoi(sample_data[j].sample_value[sample_counter])));
                        break;
                    case DATA_TYPE_FLOAT:
                        json_object_object_add(new_point, "value", json_object_new_double(atof(sample_data[j].sample_value[sample_counter])));
                        break;
                    case DATA_TYPE_STRING:
                        json_object_object_add(new_point, "value", json_object_new_string(sample_data[j].sample_value[sample_counter]));
                        break;
                }

                // Add tag
                json_object * tag_json = json_object_new_object();
                json_object_object_add(tag_json, "monitor", json_object_new_string(tagname));
                // Add other tags
                for (int ni = 0; ni < tag_info.number_of_tags; ni++) {
                    STagInfo * pTag = tag_info.tags + ni;
                    double spercent = 0;
                    for (int nj = 0; nj < pTag->number_of_values; nj++) {
                        spercent += pTag->values[nj].percent;
                        if ( tID <= spercent * pWInfo->num_of_detectors) {
                            json_object_object_add(tag_json, pTag->key, json_object_new_string(pTag->values[nj].value));
                            break;
                        }
                    }
                }
                json_object_object_add(new_point, "tags", tag_json);

                json_object_array_add(jobj, new_point);

                count++;

                if (count >= pWInfo->points_per_request) {

                    /* printf("%s\n", json_object_to_json_string(jobj)); */

                    /* printf("*****%s*****\n", json_object_to_json_string(jobj)); */
                    res = sendHttpRequest(curl, pWInfo->url, json_object_to_json_string(jobj));
                    if (res != CURLE_OK) {
                        fprintf(stderr, "threadId: %d, failed to send HTTP request, exit\n", pWInfo->threadId);
                        goto _exit_thread;
                    }

                    json_object_put(jobj);
                    jobj = NULL;
                    jobj = json_object_new_array();
                    count = 0;
                }
            }
        }
    }

_exit_thread:
    curl_easy_cleanup(curl);
    return NULL;

}

CURLcode sendHttpRequest(CURL * handle, char *url, const char * data) {
    // reset curl handle
    curl_easy_reset(handle);

    // set options
    curl_easy_setopt(handle, CURLOPT_URL, url);
    curl_easy_setopt(handle, CURLOPT_POST, 1L);
    curl_easy_setopt(handle, CURLOPT_POSTFIELDS, data);

    return curl_easy_perform(handle);
}

int loadSampleData(char * fschema, char * fsample, SMetricInfo ** sample_data, int *nfields, int *sample_size) {

    char *line = NULL;
    size_t line_size = 0;
    ssize_t read_size = 0;
    char * token = NULL;
    char * token_t = NULL;
    char * pstr = NULL;
    int    count = 0;
    char * line_t = NULL;

    *sample_data = NULL;
    memset(&tag_info, 0, sizeof(STags));

    // Read schema
    FILE * fs = fopen(fschema, "r");
    if (fs == NULL) return -1;

    while ((read_size = getline(&line, &line_size, fs)) > 0) {

        line[read_size-1] = '\0';
        if (regex_match(line, "^#.*$") || regex_match(line, "^\\s*$")) continue;

        // deal with the line
        if (strchr(line, ':') == NULL) {

            line_t = strdup(line);
            pstr = line_t;
            while ((token = strtok_r(pstr, ",", &pstr))) {
                count ++;
            }
            *nfields = count;
            taosMemoryFree(line_t);

            *sample_data = (SMetricInfo *)calloc(count, sizeof(SMetricInfo));

            count = 0;
            pstr = line;
            while ((token = strtok_r(pstr, ", ", &pstr))) {
                token_t = strtok_r(pstr, ", ", &pstr);
                strcpy((*sample_data)[count].metric_name, token);
                if (strncasecmp(token_t, "int", 3) == 0) {
                    (*sample_data)[count].dtype = DATA_TYPE_INT;
                } else if (strncasecmp(token_t, "float", 5) == 0 || strncasecmp(token_t, "double", 6) == 0) {
                    (*sample_data)[count].dtype = DATA_TYPE_FLOAT;
                } else if (strncasecmp(token_t, "binary", 6) == 0) {
                    (*sample_data)[count].dtype = DATA_TYPE_STRING;
                }
                count ++;
            }
        } else { // scheme 
            if (tag_info.number_of_tags >= MAX_NUM_OF_TAGS) break;
            char * saveptr1;
            char * token1 = strtok_r(line, ":", &saveptr1);
            char * token2 = strtok_r(NULL, ":", &saveptr1);

            char * saveptr2;
            char * key = strtok_r(token1, " ", &saveptr2);

            STagInfo * pTag = tag_info.tags + tag_info.number_of_tags;

            strcpy(pTag->key, key);

            char * saveptr3;
            char * ptemp = token2;

            for (;;ptemp = NULL) {
                char * token4 = strtok_r(ptemp, ",", &saveptr3);
                if (token4 == NULL) break;
                char * saveptr4;
                char * token5 = strtok_r(token4, " ", &saveptr4);
                char * token6 = strtok_r(NULL, " ", &saveptr4);
                // Trim binary token5
                strcpy(pTag->values[pTag->number_of_values].value, trim_string(token5));
                pTag->values[pTag->number_of_values].percent = atof(token6);

                pTag->number_of_values++;
            }

            tag_info.number_of_tags++;
        }
    }

    fclose(fs);

    // Read sample data
    fs = fopen(fsample, "r");
    if (fs == NULL) {
        goto _exit_failure;
    }

    int tcount = 0;
    *sample_size = 0;
    while (1) {
        read_size = getline(&line, &line_size, fs);
        if (read_size < 0) break;
        line[read_size-1] = '\0';
        // TODO : deal with the sample data
        pstr = line;
        (*sample_size) ++;

        count = 0;
        while ((token = strtok_r(pstr, ", ", &pstr))) {
            (*sample_data)[count].sample_value[tcount] = strdup(token);
            count++;
        }
        tcount++;
    }

    fclose(fs);
    if (line != NULL) taosMemoryFree(line);
    return 0;

_exit_failure:
    if (line != NULL) taosMemoryFree(line);
    return -1;

}
