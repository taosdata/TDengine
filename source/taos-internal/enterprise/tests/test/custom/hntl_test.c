#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <argp.h>

#include <taos.h>
#include <stdbool.h>
#include <taosdebug.h>

extern char configDir[];
int64_t stimestamp = 1524206642;
int  starttable = 1;
bool not_create_table = false;
int64_t interval = 1;

#define   MAX_IP_LEN            20
#define   MAX_DB_NAME_LEN       20
#define   MAX_METRIC_NAME_LEN   20
#define   MAX_TABLE_NAME_LEN    20
#define   MAX_DIR_NAME_LEN      128
#define   MAX_PATTERN_LEN       512
#define   MAX_COMMAND_SIZE      10240*10
#define   MAX_DATA_SIZE         10240*10

enum MODE {SYNC, ASYNC};
struct arguments {
    enum MODE mode;
    char * ip;
    char database[MAX_DB_NAME_LEN];
    char * metric;
    char table[MAX_TABLE_NAME_LEN];
    char pattern[MAX_PATTERN_LEN];
    int  ntables;
    int  nrecords;
    int  qps;
    int  threads;
    int  icycle;
    int  days_to_keep;
    int  replica;
    int  ctonly;              // create table only?
};

struct insert_args {
    enum MODE mode;
    TAOS * taos;
    char   database[MAX_DB_NAME_LEN];
    char   table[MAX_TABLE_NAME_LEN];
    int    stid;
    int    etid;
    int    nrecords;
    int    qps;
    int    icycle;
    char  pattern[MAX_PATTERN_LEN];
    int   threadId;
};

extern void taosMsleep(int);
int taos_create_table(TAOS * taos, struct arguments * args);
double get_curr_time();
void * sync_insert(void * tharg);
void * async_insert(void * tharg);
void sync_query(TAOS * taos, char * sqlstr);
void generate_data(const char * patter, char * data, int dataLen, int64_t timestamp, int value);

const char * argp_program_version = "1.1.6";
const char * argp_program_bug_address = "hzcheng@taosdata.com";
static char doc[] = "Test program for TDengine";
static char args_doc[] = "...";
static struct argp_option options[] = {
    {"mode",      'm', "MODE",         0, "insert mode, 0 for SYNC, 1 for ASYNC"},
    {"ip",        'h', "IP_ADDR",      0, "ip address of the cluster"},
    {"database",  'd', "DB_NAME",      0, "database name"},
    {"metric",    'e', "METRIC_NAME",  0, "metirc name"},
    {"table",     't', "TB_PREFIX",    0, "table prefix"},
    {"pattern",   'p', "PATTERN",      0, "ts timestamp, age int"},
    {"ntables",   'a', "NTABLES",      0, "number of tables"},
    {"nrecords",  'b', "NRECORDS",     0, "number of records per table to insert"},
    {"qps",       'q', "QPS",          0, "number of records to insert per query"},
    {"config",    'c', "CONFIGDIR",    0, "configuration directory"},
    {"threads",   'r', "NTHREADS",     0, "number of threads"},
    {"icycle",    'i', "CYCLE",        0, "insert cycle in seconds"},
    {"daystokeep",'k', "DAYS_TO_KEEP", 0, "days to keep"},
    {"replica",   's', "REPLICA",      0, "database replica"},
    {"ctonly",    'y', NULL,           0, "create tables only"},
    {"stimestamp",'f', "STIMESTAMP",   0, "start timestamp"},
    {"starttable",'g', "STARTTABLE",   0, "start table index" },
    {"noctables", 'n', "NOCTABLES",    0, "start table index" },
	{"interval",  'j', "INTERVAL",     0, "interval in seconds" }
};

static error_t opt_parser(int key, char * arg, struct argp_state * state) {

    struct arguments * arguments = state->input;

    switch (key) {
        case 'm':
            arguments->mode = atoi(arg) ? ASYNC : SYNC;
            break;
        case 'h':
            arguments->ip = (char *) taosMemoryMalloc(MAX_IP_LEN);
            strcpy(arguments->ip, arg);
            break;
        case 'd':
            strcpy(arguments->database, arg);
            break;
        case 'e':
            arguments->metric = (char *) taosMemoryMalloc(MAX_METRIC_NAME_LEN);
            strcpy(arguments->metric, arg);
            break;
        case 't':
            strcpy(arguments->table, arg);
            break;
        case 'p':
            strcpy(arguments->pattern, arg);
            break;
        case 'a':
            arguments->ntables = atoi(arg);
            break;
        case 'b':
            arguments->nrecords = atoi(arg);
            break;
        case 'q':
            arguments->qps = atoi(arg);
            break;
        case 'c':
            strcpy(configDir, arg);
            break;
        case 'r':
            arguments->threads = atoi(arg);
        case 'i':
            arguments->icycle = atoi(arg);
            break;
        case 'k':
            arguments->days_to_keep = atoi(arg);
            break;
        case 's':
            arguments->replica = atoi(arg);
            break;
        case 'y':
            arguments->ctonly = 1;
            break;
        case 'f':
            stimestamp = atol(arg);
            break;
        case 'g':
            starttable = atol(arg);
            break;
        case 'n':
            not_create_table = true;
            break;
		case 'j':
            interval = atol(arg);
            break;	
        default:
            return ARGP_ERR_UNKNOWN;
    }
    
    return 0;
}

static struct argp argp = {options, opt_parser, args_doc, doc};

int main(int argc, char * argv[]) {
    srand(time(NULL));
    stimestamp = (int64_t)(get_curr_time() * 1000);

    /* Parse the option */
    struct arguments arguments = {
        SYNC,   // mode
        NULL,   // ip
        "db",   // database
        NULL,   // metric
        "t",    // table
        "ts timestamp, f1 double, f2 double, f3 double, f4 double",  // pattern
        20,     // ntables
        10000,  // nrecords
        1,      // qps
        1,      // threads
        0,      // icycle
        3650,   // days_to_keep
        1,      // replica
        0       // ctonly
    };
    struct arguments * args = &arguments;

    if (argp_parse(&argp, argc, argv, 0, 0, args)) {
        fprintf(stderr, "Failed to parse arguments");
        exit(1);
    }

    /* TODO: Check and adjust the option */

    /* Connect to the database */
    taos_init();
    TAOS * taos = taos_connect(args->ip, "root", "taosdata", NULL, 0);
    if (taos == NULL) {
        fprintf(stderr, "Connect to TDengine, reason:%s\n", taos_errstr(taos));
        goto __exit;
    }

    if (taos_create_table(taos, args)) {
        fprintf(stderr, "Failed to create tables\n");
        taos_close(taos);
        goto __exit;
    }
    taos_close(taos);

    if (args->ctonly) {
        goto __exit;
    }

    /* Inserting data */
    pthread_t * pids = (pthread_t *)taosMemoryMalloc(args->threads*sizeof(pthread_t));
    memset(pids, 0, args->threads * sizeof(pthread_t));
    struct insert_args * iargs = (struct insert_args *) taosMemoryMalloc(args->threads * sizeof(struct insert_args));
    memset(iargs, 0, args->threads * sizeof(struct insert_args));

    assert(args->ntables >= args->threads);


    int a = args->ntables / args->threads;
    int b = args->ntables % args->threads;
    int last = starttable;
    for (int i = 0; i < args->threads; i++) {
        pthread_t * pid = pids + i;
        struct insert_args * iarg = iargs + i;
        iarg->taos = taos_connect(args->ip, "root", "taosdata", NULL, 0);
        if (iarg->taos == NULL) {
            fprintf(stderr, "Connect to TDengine, reason:%s\n", taos_errstr(taos));
            goto __exit;
        }
        strcpy(iarg->database, args->database);
        strcpy(iarg->table, args->table);
        iarg->stid = last;
        iarg->etid = i < b? last + a: last + a - 1;;
        iarg->nrecords = args->nrecords;
        iarg->qps = args->qps;
        iarg->mode = args->mode;
        iarg->icycle = args->icycle;
        iarg->threadId = i+1;
        strcpy(iarg->pattern, args->pattern);
        last = iarg->etid + 1;

        if (args->mode == SYNC) {
            pthread_create(pid, NULL, sync_insert, iarg);
        }
        else{
            pthread_create(pid, NULL, async_insert, iarg);
        }
    }

    for (int i = 0; i < args->threads; i++) {
        pthread_join(pids[i], NULL);
    }

    for (int i = 0; i < args->threads; i++) {
        taos_close(iargs[i].taos);
    }

    taosMemoryFree(pids);
    taosMemoryFree(iargs);

__exit:
    return 0;
}

void * sync_insert(void * tharg) {
    struct insert_args * args = (struct insert_args *) tharg;
    char   buffer[MAX_COMMAND_SIZE];
    char   data[MAX_DATA_SIZE];

    int64_t ltimestamp = stimestamp;

    tPrint("Thread %d starts to insert table %s.%s%d--%s.%s%d...\n", 
            args->threadId, args->database, args->table, args->stid,
            args->database, args->table, args->etid);
    
    double start_time = get_curr_time();
    for (int i = 0; i < args->nrecords;) {
        double start_time = get_curr_time();

        int k = 0;
        for (int tid = args->stid; tid <= args->etid; tid++) {
            char * sptr = buffer;
            sptr += sprintf(sptr, "insert into %s.%s%d values ", args->database, args->table, tid);

            k = i;
            for (int j = 0; j < args->qps; j++) {
                //5 minute 900, or is 1
                generate_data(args->pattern, data, MAX_DATA_SIZE, ltimestamp+k*interval, k);
                sptr += sprintf(sptr, " %s ", data);

                if (++k >= args->nrecords) break;
            }
            sync_query(args->taos, buffer);
			
			if (tid > args->stid && (tid - args->stid)%10000 == 0) {
		       tPrint("Thread %d index:%d to insert table:%d finished\n", 
                 args->threadId, i, tid - args->stid);
			}
        }
        
        i = k;
		
		if (i % 10000 == 0)
        tPrint("Thread %d index:%d to insert table %s.%s%d--%s.%s%d finished\n", 
          args->threadId, i, args->database, args->table, args->stid,
          args->database, args->table, args->etid);

        if (args->icycle) {
            double elapsed_time = (get_curr_time() - start_time);
            double delta_time = args->icycle - elapsed_time;
            if (delta_time < 0) {
                //fprintf(stdout, "WARN: used %10.3f seconds to insert but icycle: %d seconds\n", elapsed_time, args->icycle);
            }
            else {
                taosMsleep(delta_time * 1000);
            }
        }
    }

    double elapsed_time = get_curr_time() - start_time;

    tPrint("Thread %d finished inserting table %s.%s%d--%s.%s%d\nRecords: %-d\nTime:    %-10.3fs\nSpeed:   %-10.3fR/s\n",
            args->threadId, args->database, args->table, args->stid,
            args->database, args->table, args->etid,
            args->nrecords * (args->etid - args->stid + 1), elapsed_time, args->nrecords * (args->etid - args->stid + 1)/elapsed_time);


    return NULL;
}

void * async_insert(void * tharg) {
    struct insert_args * args = (struct insert_args *) tharg;
    // TODO : Finish async write

    return NULL;
}


void sync_query(TAOS * taos, char * sqlstr) {
  for (int i = 0; i < 10000; ++i) {
    if (taos_query(taos, sqlstr)) {
      tError("Failed to run %d insert, reason: %s, sql:%s\n", i, taos_errstr(taos), sqlstr);
	  //printf("Failed to run %d insert, reason: %s, sql:%s\n", i, taos_errstr(taos), sqlstr);
      taosMsleep(1000);
      continue;
    }
    else {
      return;
    }
  }

  tError("Failed to run insert, reason: %s, sql:%s\n", taos_errstr(taos), sqlstr);
  taos_close(taos);
  exit(1);
}

int taos_create_table(TAOS * taos, struct arguments * args) {
    char tb_name[64], mt_name[64];

    if (not_create_table) {
      tPrint("===> not create any tables, insert direct....");
      return 0;
    }
    tPrint("===> Starting to create %d tables in DB %s with table prefix %s....", args->ntables, args->database, args->table);

    char sqlstr[MAX_COMMAND_SIZE];
    sprintf(sqlstr, "create database %s replica %d keep %d", args->database, args->replica, args->days_to_keep);
    taos_query(taos, sqlstr);

    /* TODO: Add async create table option here */
    double start_time = get_curr_time();
    if (args->metric) {
        sprintf(mt_name, "%s.%s", args->database, args->metric);
        sprintf(sqlstr, "create table %s (%s) tags (orgno int)", mt_name, args->pattern);
        taos_query(taos, sqlstr);

        for (int i = starttable; i < args->ntables + starttable; i++) {
            sprintf(tb_name, "%s.%s%d", args->database, args->table, i);
            sprintf(sqlstr, "create table %s using %s tags(%d)", tb_name, mt_name, i%100000);
            taos_query(taos, sqlstr);
            if ((i - starttable + 1) % 100000 == 0) {
              tPrint("create table tb_name:%s finished ", tb_name);
            }
        }
        tPrint("===>  create table finished ");
    }
    else {
        for (int i = starttable; i < args->ntables + starttable; i++) {
            sprintf(tb_name, "%s.%s%d", args->database, args->table, i);
            sprintf(sqlstr, "create table %s (%s)", tb_name, args->pattern);
            taos_query(taos, sqlstr);
            if ((i - starttable + 1) % 100000 == 0) {
              tPrint("create table tb_name:%s finished ", tb_name);
            }
        }
        tPrint("===>  create table finished ");
    }
    double end_time = get_curr_time();

    tPrint("Finished creating %d tables in DB %s with table prefix %s in %10.2f seconds\n",
            args->ntables, args->database, args->table, end_time - start_time);

    return 0;
}

double get_curr_time(){
    struct timeval tv;
    if (gettimeofday(&tv, NULL)) {
        fprintf(stderr, "Failed to get current time\n");
        exit(1);
    }

    return tv.tv_sec + tv.tv_usec * 1E-6;
}

void generate_data(const char * pattern, char * data, int dataLen, int64_t timestamp, int value) {

    memset(data, 0, dataLen);

    char * pstr = data;
    pstr += sprintf(pstr,"%s", "( ");

    char * dupstr = strdup(pattern);
    char * running = dupstr;

    char * token = strsep(&running, " ,");
    while (token != NULL) {
        // Parse the pattern
        if (*token != '\0') {
            if (strncasecmp(token, "timestamp", sizeof("timestamp")) == 0) {
              pstr += sprintf(pstr, "%lld000", timestamp);
            }
            else if(strncasecmp(token, "tinyint", sizeof("tinyint")) == 0) {
              pstr += sprintf(pstr, ", %d", value % 120);
            }
            else if(strncasecmp(token, "smallint", sizeof("smallint")) == 0) {
              pstr += sprintf(pstr, ", %d", value % 32760);
            }
            else if(strncasecmp(token, "int", sizeof("int")) == 0) {
              pstr += sprintf(pstr, ", %d", value);
            }
            else if(strncasecmp(token, "bigint", sizeof("bigint")) == 0) {
              pstr += sprintf(pstr, ", %ld", value);
            }
            else if(strncasecmp(token, "float", sizeof("float")) == 0) {
              pstr += sprintf(pstr, ", %f", (float)value);
            }
            else if(strncasecmp(token, "double", sizeof("double")) == 0) {
              pstr += sprintf(pstr, ", %lf", (double)value);
            }
            else {
            }
        }

        token = strsep(&running, ", ");
    }

    pstr += sprintf(pstr, " )");

    taosMemoryFree(dupstr);
}
