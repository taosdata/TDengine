#ifndef __TDENGINE_INSERT__
#define __TDENGINE_INSERT__

#include <stdbool.h>
#include <stdint.h>
#include <taos/taos.h>

#define BUFFER_SIZE 8192
#define MAX_DB_NAME_SIZE 64
#define MAX_TB_NAME_SIZE 64
#define MAX_SAMPLE_DATA_SIZE 10000

enum MODE{SYNC, ASYNC};
enum INSERT_MODE{TABLE_BY_TABLE, TIME_BY_TIME};

typedef struct {
    TAOS * taos;
    int threadID;
    char db_name[MAX_DB_NAME_SIZE];
    char tb_prefix[MAX_TB_NAME_SIZE];
    int start_table_id;
    int end_table_id;
    int64_t nrecords_per_table;
    int nrecords_per_request;
    long start_time;
    long time_interval;
    enum INSERT_MODE insert_mode;
    bool is_real_situation;

    sem_t mutex_sem;
    int notFinished;
    sem_t lock_sem;
} info;


typedef struct {
    TAOS * taos;

    char tb_name[MAX_TB_NAME_SIZE];
    long timestamp;
    int target;
    int counter;
    int nrecords_per_request;

    sem_t * mutex_sem;
    int * notFinished;
    sem_t * lock_sem;
} sTable;

void queryDB(TAOS * taos, char * command);
void * sync_write(void * sarg);
int loadSampleData();
void freeSampleData();

#endif

