#ifndef INC_DUMP_H_
#define INC_DUMP_H_


#ifdef WINDOWS
#include <argp.h>
#include <time.h>
#include <WinSock2.h>
#elif defined(DARWIN)
#include <ctype.h>
#include <argp.h>
#include <unistd.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <wordexp.h>
#else
#include <argp.h>
#include <unistd.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <wordexp.h>
#include <dirent.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <iconv.h>
#include <sys/stat.h>

#include <inttypes.h>
#include <limits.h>

#include <avro.h>
#include <jansson.h>

#include <taos.h>
#include <taoserror.h>
#include <toolsdef.h>
#include "../../inc/pub.h"


//
// ---------------- define ----------------
//


// use 256 as normal buffer length
#define BUFFER_LEN                 256

#define VALUE_BUF_LEN              4096
#define MAX_RECORDS_PER_REQ        32766
#define NEED_CALC_COUNT            UINT64_MAX
#define HUMAN_TIME_LEN             60
#define DUMP_DIR_LEN               (MAX_DIR_LEN - (TSDB_DB_NAME_LEN + 10))
#define TSDB_USET_PASSWORD_LONGLEN 256  // come from tdef.h
#define ITEM_SPACE                 50
#define NTABLE_FOLDER              "data0-0"



// stb schema KEY
#define VERSION_KEY     "version"
#define STBNAME_KEY      "name"

#define VERSION_VAL      1



#define NAME_KEY        "name"
#define FIELDS_KEY      "fields"


// file
#define MFILE_EXT       ".m"
#define STBNAME_FILE    "/stbname"


#define debugPrint(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) { \
      fprintf(stdout, "DEBG: "fmt, __VA_ARGS__); } } while (0)

#define debugPrint2(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) { \
      fprintf(stdout, ""fmt, __VA_ARGS__); } } while (0)

#define verbosePrint(fmt, ...) \
    do { if (g_args.verbose_print) { \
        fprintf(stdout, "VERB: "fmt, __VA_ARGS__); } } while (0)

#define perfPrint(fmt, ...) \
    do { if (g_args.performance_print) { \
        fprintf(stdout, "PERF: "fmt, __VA_ARGS__); } } while (0)

#define warnPrint(fmt, ...) \
    do { fprintf(stderr, "\033[33m"); \
        fprintf(stderr, "WARN: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while (0)

#define errorPrint(fmt, ...) \
    do { fprintf(stderr, "\033[31m"); \
        fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while (0)

#define okPrint(fmt, ...) \
    do { fprintf(stderr, "\033[32m"); \
        fprintf(stderr, "OK: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while (0)

#define infoPrint(fmt, ...) \
    do { \
        fprintf(stdout, "INFO: "fmt, __VA_ARGS__); \
    } while (0)

#define freeTbNameIfLooseMode(tbName) \
    do { \
        if (g_dumpInLooseModeFlag) tfree(tbName);   \
    } while (0)

// for tstrncpy buffer overflow
#define min(a, b) (((a) < (b)) ? (a) : (b))

#define tfree(x)         \
    do {                   \
        if (x) {             \
            free((void *)(x)); \
            x = 0;             \
        }                    \
    } while (0)

//
// ------------- struct  ---------------
//

typedef struct {
    int16_t bytes;
    int8_t  type;
} SOColInfo;


// -------------------------- SHOW DATABASE INTERFACE-----------------------
enum _show_db_index {
    TSDB_SHOW_DB_NAME_INDEX,
    TSDB_SHOW_DB_CREATED_TIME_INDEX,
    TSDB_SHOW_DB_NTABLES_INDEX,
    TSDB_SHOW_DB_VGROUPS_INDEX,
    TSDB_SHOW_DB_REPLICA_INDEX,
    TSDB_SHOW_DB_QUORUM_INDEX,
    TSDB_SHOW_DB_DAYS_INDEX,
    TSDB_SHOW_DB_KEEP_INDEX,
    TSDB_SHOW_DB_CACHE_INDEX,
    TSDB_SHOW_DB_BLOCKS_INDEX,
    TSDB_SHOW_DB_MINROWS_INDEX,
    TSDB_SHOW_DB_MAXROWS_INDEX,
    TSDB_SHOW_DB_WALLEVEL_INDEX,
    TSDB_SHOW_DB_FSYNC_INDEX,
    TSDB_SHOW_DB_COMP_INDEX,
    TSDB_SHOW_DB_CACHELAST_INDEX,
    TSDB_SHOW_DB_PRECISION_INDEX,
    TSDB_SHOW_DB_UPDATE_INDEX,
    TSDB_SHOW_DB_STATUS_INDEX,
    TSDB_MAX_SHOW_DB
};

// SHOW TABLES CONFIGURE -------------------------------------
enum _show_tables_index {
    TSDB_SHOW_TABLES_NAME_INDEX,
    TSDB_SHOW_TABLES_CREATED_TIME_INDEX,
    TSDB_SHOW_TABLES_COLUMNS_INDEX,
    TSDB_SHOW_TABLES_METRIC_INDEX,
    TSDB_SHOW_TABLES_UID_INDEX,
    TSDB_SHOW_TABLES_TID_INDEX,
    TSDB_SHOW_TABLES_VGID_INDEX,
    TSDB_MAX_SHOW_TABLES
};

// DESCRIBE STABLE CONFIGURE ------------------------------
enum _describe_table_index {
    TSDB_DESCRIBE_METRIC_FIELD_INDEX,
    TSDB_DESCRIBE_METRIC_TYPE_INDEX,
    TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
    TSDB_DESCRIBE_METRIC_NOTE_INDEX,
    TSDB_MAX_DESCRIBE_METRIC
};

#define COL_NOTE_LEN        32
#define COL_TYPEBUF_LEN     16
#define COL_VALUEBUF_LEN    32

typedef struct {
    char field[TSDB_COL_NAME_LEN];
    int type;
    int length;
    char note[COL_NOTE_LEN];
    char value[COL_VALUEBUF_LEN];
    char *var_value;
    int16_t idx;
} ColDes;

typedef struct {
    char name[TSDB_TABLE_NAME_LEN+1];
    int columns;
    int tags;
    ColDes cols[];
} TableDes;

#define DB_PRECISION_LEN   8
#define DB_STATUS_LEN      16

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    bool belongStb;
    char stable[TSDB_TABLE_NAME_LEN];
    TableDes *stbTableDes;
} TableInfo;

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    char stable[TSDB_TABLE_NAME_LEN];
} TableRecord;

typedef struct {
    bool isStb;
    bool belongStb;
    int64_t dumpNtbCount;
    TableRecord **dumpNtbInfos;
    TableRecord tableRecord;
} TableRecordInfo;

#define STRICT_LEN      16
#define DURATION_LEN    16
#define KEEPLIST_LEN    48

typedef struct {
    char     name[TSDB_DB_NAME_LEN];
    char     create_time[32];
    int64_t  ntables;
    int32_t  vgroups;
    int16_t  replica;
    char     strict[STRICT_LEN];
    int16_t  quorum;
    int16_t  days;
    char     duration[DURATION_LEN];
    char     keeplist[KEEPLIST_LEN];
    // int16_t  daysToKeep;
    // int16_t  daysToKeep1;
    // int16_t  daysToKeep2;
    int32_t  cache;   // MB
    int32_t  blocks;
    int32_t  minrows;
    int32_t  maxrows;
    int8_t   wallevel;
    int8_t   wal;     // 3.0 only
    int32_t  fsync;
    int8_t   comp;
    int8_t   cachelast;
    bool     cache_model;
    char     precision[DB_PRECISION_LEN];   // time resolution
    bool     single_stable_model;
    int8_t   update;
    char     status[DB_STATUS_LEN];
    int64_t  dumpTbCount;
    uint64_t uniqueID;
    char     dirForDbDump[MAX_DIR_LEN];
} SDbInfo;

enum enAVROTYPE {
    enAVRO_TBTAGS = 0,
    enAVRO_NTB,
    enAVRO_DATA,
    enAVRO_UNKNOWN,
    enAVRO_INVALID
};

//
// ------------------ hash map struct -----------------------
//

// Define the maximum number of buckets
#define HASH32_MAP_MAX_BUCKETS 1024

// Define the key-value pair structure
typedef struct HashMapEntry {
    char *key;
    void *value;
    struct HashMapEntry *next;
} HashMapEntry;

// Define the hash table structure
typedef struct HashMap {
    HashMapEntry *buckets[HASH32_MAP_MAX_BUCKETS];
    pthread_mutex_t lock;
} HashMap;


//
// --------------------- db changed struct ------------------------
//

// record db table schema changed
typedef struct StbChange {
    // main
    TableDes *tableDes;
    
    // bellow create by tableDes
    char *strTags;
    char *strCols;
    bool schemaChanged; // col or tag have changed is True else false
} StbChange;

// record db table schema changed
typedef struct DBChange {
    int16_t version;
    // record all stb
    HashMap  stbMap;
    const char *dbPath;
} DBChange;


typedef enum enAVROTYPE enAVROTYPE;

typedef struct {
    pthread_t threadID;
    int32_t   threadIndex;
    SDbInfo   *dbInfo;
    char      stbName[TSDB_TABLE_NAME_LEN];
    TableDes  *stbDes;
    char      **tbNameArr;
    int       precision;
    void      *taos;
    uint64_t  count;
    int64_t   from;
    int64_t   stbSuccess;
    int64_t   stbFailed;
    int64_t   ntbSuccess;
    int64_t   ntbFailed;
    int64_t   recSuccess;
    int64_t   recFailed;
    enAVROTYPE  avroType;
    char      dbPath[MAX_DIR_LEN];
    DBChange  *pDbChange;
} threadInfo;

typedef struct {
    volatile int64_t   totalRowsOfDumpOut;
    volatile int64_t   totalChildTblsOfDumpOut;
    volatile int64_t   totalSuperTblsOfDumpOut;
    volatile int64_t   totalDatabasesOfDumpOut;
} resultStatistics;


enum AVRO_CODEC {
    AVRO_CODEC_START = 0,
    AVRO_CODEC_NULL = AVRO_CODEC_START,
    AVRO_CODEC_DEFLATE,
    AVRO_CODEC_SNAPPY,
    AVRO_CODEC_LZMA,
    AVRO_CODEC_UNKNOWN,
    AVRO_CODEC_INVALID
};

/* avro section begin */
#define RECORD_NAME_LEN     65
#define TYPE_NAME_LEN       20

typedef struct FieldStruct_S {
    char name[TSDB_COL_NAME_LEN];
    int type;
    bool nullable;
    bool is_array;
    int array_type;
} FieldStruct;

typedef struct InspectStruct_S {
    char name[TSDB_COL_NAME_LEN];
    char type[TYPE_NAME_LEN];
    bool nullable;
    bool is_array;
    char array_type_str[TYPE_NAME_LEN];
} InspectStruct;

typedef struct RecordSchema_S {
    int version;
    char name[RECORD_NAME_LEN];
    char *fields;
    int  num_fields;

    // read stb_schema_for_db
    char stbName[TSDB_TABLE_NAME_LEN]; 
    TableDes *tableDes;    
} RecordSchema;

/* avro section end */

// rename db 
typedef struct SRenameDB {
    char* old;
    char* new;
    void* next;
}SRenameDB;

/* Used by main to communicate with parse_opt. */
typedef struct arguments {
    // connection option
    char    *host;
    char    *user;
    char     password[TSDB_USET_PASSWORD_LONGLEN];
    uint16_t port;
    // strlen(taosdump.) +1 is 10
    char     outpath[DUMP_DIR_LEN];
    char     inpath[DUMP_DIR_LEN];
    // result file
    char    *resultFile;
    // dump unit option
    bool     all_databases;
    bool     databases;
    char    *databasesSeq;
    // dump format option
    bool     schemaonly;
    bool     with_property;
    bool     avro;
    int      avro_codec;
    int64_t  start_time;
    char     humanStartTime[HUMAN_TIME_LEN];
    int64_t  end_time;
    char     humanEndTime[HUMAN_TIME_LEN];
    char     precision[8];

    int32_t  data_batch;
    bool     data_batch_input;
    int32_t  max_sql_len;
    bool     allow_sys;
    bool     escape_char;
    bool     db_escape_char;
    bool     loose_mode;
    bool     inspect;
    // other options
    int32_t  thread_num;
    int      abort;
    char   **arg_list;
    int      arg_list_len;
    bool     isDumpIn;
    bool     debug_print;
    bool     verbose_print;
    bool     performance_print;
    bool     dotReplace;
    int      dumpDbCount;

    int8_t   connMode;
    bool     port_inputted;
    char    *dsn;

    // put rename db string
    char      * renameBuf;
    SRenameDB * renameHead;
    // retry for call engine api
    int32_t     retryCount;
    int32_t     retrySleepMs;  
        
} SArguments;

bool isSystemDatabase(char *dbName);
int inDatabasesSeq(const char *dbName);
int processFieldsValueV2(
        int index,
        TableDes *tableDes,
        const void *value,
        int32_t len);
int processFieldsValueV3(
        int index,
        TableDes *tableDes,
        const void *value,
        int32_t len);

void constructTableDesFromStb(const TableDes *stbTableDes,
        const char *table,
        TableDes **ppTableDes);  
int typeStrToType(const char *type_str);      
int64_t queryDbForDumpOutCount(
        void **taos_v,
        const char *dbName,
        const char *tbName,
        const int precision);
avro_value_iface_t* prepareAvroWface(
        const char *avroFilename,
        char *jsonSchema,
        avro_schema_t *schema,
        RecordSchema **recordSchema,
        avro_file_writer_t *writer);

void *queryDbForDumpOutOffset(
        void **taos,
        const char *dbName,
        const char *tbName,
        const int precision,
        const int64_t start_time,
        const int64_t end_time,
        const int64_t limit,
        const int64_t offset);
int processValueToAvro(
        const int32_t col,
        avro_value_t record,
        avro_value_t avro_value,
        avro_value_t branch,
        const char *name,
        const uint8_t type,
        const int32_t bytes,
        const void *value,
        const int32_t len
        );
void printDotOrX(int64_t count, bool *printDot);
void freeRecordSchema(RecordSchema *recordSchema);  
int processResultValue(
        char *pstr,
        const int curr_sqlstr_len,
        const uint8_t type,
        const void *value,
        uint32_t len);

// dump table (stable/child table/normal table) meta and data
int64_t dumpTable(
        const int64_t index,
        void  **taos,
        const SDbInfo *dbInfo,
        const bool belongStb,
        const char *stable,
        const TableDes *stbDes,
        const char *tbName,
        const int precision,
        char *dumpFilename,
        FILE *fp
        );
int64_t dumpStbAndChildTb(
        void **taos_v, SDbInfo *dbInfo, const char *stable, FILE *fpDbs);
int64_t dumpNormalTable(
        int64_t index,
        void **taos_v, SDbInfo *dbInfo, char *ntbName);

// query
void* openQuery(void** taos_v , const char * sql);
void closeQuery(void* res);
int32_t readRow(void *res, int32_t idx, int32_t col, uint32_t *len, char **data);
void engineError(char * module, char * fun, int32_t code);

int getTableDes(TAOS *taos, const char* dbName, const char *table, TableDes *tableDes, const bool colOnly);

extern struct arguments g_args;

extern int g_majorVersionOfClient;
extern char g_escapeChar[2];
extern FILE *g_fpOfResult;
extern int64_t   g_tableCount;
extern int64_t   g_tableDone;
extern char      g_dbName[TSDB_DB_NAME_LEN];
extern char      g_stbName[TSDB_TABLE_NAME_LEN];
extern int64_t g_totalDumpOutRows;
extern SDbInfo **g_dbInfos;


#endif  // INC_DUMP_H_
