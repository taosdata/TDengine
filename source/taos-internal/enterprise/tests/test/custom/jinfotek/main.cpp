#include "jinfo.h"


enum FILE_CATEGORY
{
    FILE_CATEGORY_RD = 0,
    FILE_CATEGORY_CDR,
    FILE_CATEGORY_EVT,
    FILE_CATEGORY_SMS,
};

struct InputFile
{
    uint32_t ts;
    int8_t category;
    char path[120];
};

struct OutputFile
{
    uint32_t num_records;
    FILE* fp;
};


struct TableEntry
{
    uint32_t hash;
    char name[28];
    TableEntry *next;
};


static const char *g_inpath = ".";
static const char *g_outpath = ".";
static const char *g_database = "wjz";
static const char *g_stable = "tb_event";

static time_t g_now;

static InputFile *g_input_files = NULL;
static uint32_t g_num_input_files = 0;

#define MAX_NUM_OF_TABLE_ENTRY 4096
static TableEntry** g_tables = NULL;

#define MAX_NUM_OF_OUTPUT_FILE 20
static OutputFile* g_output_files = NULL;
static FILE* g_table_file = NULL;
static uint64_t g_records_written = 0;

#define MAX_NUM_OF_RECORD (1024 * 1024)
static Record* g_record_pool = NULL;
static Record* g_next_free_record = NULL;
static Record** g_records = NULL;
static int32_t g_num_records = 0;


static char* join_path( char* buf, const char* a, const char* b )
{
    size_t len = strlen( a );
    memcpy( buf, a, len );
    if( len > 0 && buf[len - 1] != '/' )
    {
        buf[len] = '/';
        ++len;
    }

    strcpy( buf + len, b );
    return buf;
}



static
bool iterate_file( const char* rel, void (*fp)(const char*, const char*) )
{
    char path[256];
    join_path( path, g_inpath, rel );

    DIR* dir = opendir( path );
    if( dir == NULL )
        return false;

    bool ret = true;
    for( dirent* de = readdir(dir); de != NULL; de = readdir(dir) )
    {
        const char* name = de->d_name;
        if( de->d_type == DT_REG )
        {
            (*fp)( rel, name );
            continue;
        }

        if( de->d_type != DT_DIR )
            continue;

        if( name[0] == '.' )
            if( name[1] == 0 || (name[1] == '.' && name[2] == 0) )
                continue;

        join_path( path, rel, name );
        if( !iterate_file(path, fp) )
        {
            ret = false;
            break;
        }
    }

    closedir( dir );
    return ret;
}



static int8_t detect_file_category( const char* name )
{
    //    CDR.20190808154420_1565250270.1565250411.1000.dat
    // Dx_CDR.20190808144000_1565246700.0001.dat
    // Dx_EVT.20190808152500_1565249400.0011.dat
    //    EVT.20190808152730_1565249260.1565249253.0001.dat
    // Dx_SMS.20190808135000_1565243700.0001.dat
    //    SMS.20190808154440_1565250290.1565250292.0001.dat
    // wjzEvt.6262410510020000_19700101137600.1565250354.828367.dat

    static struct
    {
        const char* prefix;
        int len;
        int8_t category;
    } p2c[] = {
        { "wjzEvt.", 7, FILE_CATEGORY_RD },
        { "CDR.", 4, FILE_CATEGORY_CDR },
        { "Dx_CDR.", 7, FILE_CATEGORY_CDR },
        { "EVT.", 4, FILE_CATEGORY_EVT },
        { "Dx_EVT.", 7, FILE_CATEGORY_EVT },
        { "SMS.", 4, FILE_CATEGORY_SMS },
        { "Dx_SMS.", 7, FILE_CATEGORY_SMS },
    };

    size_t len = strlen( name );
    if( len < 41 || strcasecmp(name + len - 4, ".dat") != 0 )
        return -1;

    for( size_t i = 0; i < DIMOF(p2c); ++i )
        if( strncasecmp(name, p2c[i].prefix, p2c[i].len) == 0 )
            return p2c[i].category;

    return -1;
}



static void count_file( const char* folder, const char* name )
{
    if( detect_file_category(name) != -1 )
        ++g_num_input_files;
}



static time_t parse_timestamp( const char* str )
{
    time_t t = 0;
    for( ; *str >= '0' && *str <= '9'; ++str )
    {
        t *= 10;
        t += *str - '0';
    }
    return t;
}



static void add_to_list( const char* folder, const char* name )
{
    int8_t category = detect_file_category( name );
    if( category == -1 )
        return;

    const char* s = strchr( name, '_' );
    if( s != NULL )
    {
        if( category == FILE_CATEGORY_RD )
            s = strchr( s + 1, '.' );
        else if( name[0] == 'D' )
            s = strchr( s + 1, '_' );
    }
    if( s == NULL )
    {
        printf( "skip: wrong format: %s\n", name );
        return;
    }

    time_t ts = parse_timestamp( s + 1 );
    // 946684800 is 2000-01-01 00:00:00
    if( ts < 946684800 || ts > g_now )
    {
        printf( "skip: invalid timestamp: %s\n", name );
        return;
    }

    InputFile* in = g_input_files + g_num_input_files++;
    in->category = category;
    in->ts = (uint32_t)ts;
    join_path( in->path, folder, name );
}



static int compare_input_file( const void* a, const void* b )
{
    return ((const InputFile*)a)->ts - ((const InputFile*)b)->ts;
}



static bool build_file_list()
{
    printf( "counting files... " );
    g_num_input_files = 0;
    if( !iterate_file("", &count_file) )
    {
        puts( "failed." );
        return false;
    }

    printf( "%d.\n", g_num_input_files );
    if( g_num_input_files == 0 )
        return true;

    puts( "loading file names..." );
    g_input_files = (InputFile*)taosMemoryMalloc( sizeof(InputFile) * g_num_input_files );
    if( g_input_files == NULL )
    {
        puts( "failed to allocate memory" );
        return false;
    }

    g_num_input_files = 0;
    if( !iterate_file("", &add_to_list ) )
    {
        puts( "failed to build file list." );
        return false;
    }

    puts( "sorting file names..." );
    qsort( g_input_files, g_num_input_files, sizeof(InputFile), compare_input_file);
    puts( "build file list succeeded." );
    return true;
}



static OutputFile* get_output_file( const Record* r )
{
    uint32_t hash = 0;
    for( uint32_t i = 0; r->tbname[i] != 0; ++i )
        hash ^= ((uint32_t)r->tbname[i]) << (i % 4);

    bool found = false;
    uint32_t index = hash % MAX_NUM_OF_TABLE_ENTRY;
    for( TableEntry* te = g_tables[index]; te != NULL; te = te->next )
    {
        if( te->hash == hash && strcmp(te->name, r->tbname) == 0 )
        {
            found = true;
            break;
        }
    }

    if( !found )
    {
        const char* sqlFmt = "CREATE TABLE %s USING %s TAGS ('%s', '', '%s', '');\n";

        fprintf( g_table_file, sqlFmt, r->tbname, g_stable, r->equid, r->type );
        TableEntry* te = (TableEntry*)taosMemoryMalloc( sizeof(TableEntry) );
        if( te == NULL )
        {
            puts( "failed to allocate memory for table entry." );
            return NULL;
        }
        te->hash = hash;
        strcpy( te->name, r->tbname );
        te->next = g_tables[index];
        g_tables[index] = te;
    }

    return g_output_files + (hash % MAX_NUM_OF_OUTPUT_FILE);
}


#define PRINT_INT64( x ) \
    if( (x) == INT64_NULL ) \
        fputs( "null,", fp ); \
    else \
        fprintf( fp, "%" PRId64 ",", (x) );

#define PRINT_STRING( x ) {fputc('\'', fp);fputs( (x), fp ); fputs( "',", fp );}

#define PRINT_DOUBLE( x ) \
    if( (x)[0] == 0 ) \
        fputs( "null,", fp ); \
    else \
        {fputs( (x), fp );fputc( ',', fp );}

static bool write_one_row( const Record* r )
{
    OutputFile* of = get_output_file( r );
    if( of == NULL )
        return false;
    FILE* fp = of->fp;

    if( of->num_records % 10 == 0 )
        fputs( "insert into", fp );

    fputc(' ', fp);fputs(r->tbname, fp); fputs(" values(", fp);
    PRINT_INT64( r->catchtime );
    fputs( "0,", fp );    // TIMESTAMP
    PRINT_INT64( r->seqnum );
    PRINT_INT64( r->imsi );
    PRINT_STRING( r->imei );
    PRINT_INT64( r->lac );
    PRINT_STRING( r->equid );
    PRINT_INT64( r->lacinctimer );
    PRINT_STRING( r->lacsettime );
    PRINT_STRING( r->homearea );
    PRINT_STRING( r->msisdn );
    PRINT_INT64( r->spcode );
    PRINT_STRING( r->imptime );
    PRINT_INT64( r->system );
    PRINT_DOUBLE( r->longitude );
    PRINT_DOUBLE( r->latitude );
    PRINT_INT64( r->pn );
    PRINT_INT64( r->freq );
    PRINT_STRING( r->mac );
    PRINT_INT64( r->smssendstatus );
    PRINT_INT64( r->rssi );
    PRINT_STRING( r->esn );
    PRINT_STRING( r->tmsi );
    PRINT_STRING( r->areacode );
    PRINT_STRING( r->recordtype );
    PRINT_STRING( r->relatenum );
    PRINT_STRING( r->relatehomeac );
    PRINT_STRING( r->curarea );
    PRINT_STRING( r->neid );
    PRINT_STRING( r->lai );
    PRINT_STRING( r->ci );
    PRINT_STRING( r->billtype );
    PRINT_STRING( r->calltype );
    PRINT_STRING( r->dtmf );
    PRINT_INT64( r->callduration );
    PRINT_INT64( r->cause );
    PRINT_INT64( r->rlgtime );
    PRINT_INT64( r->alerttime );
    PRINT_INT64( r->connecttime );
    PRINT_INT64( r->disconnecttime );
    PRINT_STRING( r->sid );
    PRINT_INT64( r->idflag );
    PRINT_STRING( r->rawrelatenum );
    PRINT_INT64( r->redirflag );
    PRINT_STRING( r->origcalledno );
    PRINT_INT64( r->disconnecttype );
    PRINT_STRING( r->newlai );
    PRINT_STRING( r->newci );
    PRINT_DOUBLE( r->newlongitude );
    PRINT_DOUBLE( r->newlatitude );
    PRINT_STRING( r->voiceflag );
    PRINT_STRING( r->voicekeya );
    PRINT_STRING( r->voicekeyb );
    PRINT_STRING( r->peersid );
    PRINT_STRING( r->oldlai );
    PRINT_STRING( r->oldci );
    PRINT_DOUBLE( r->oldlongitude );
    PRINT_DOUBLE( r->oldlatitude );
    PRINT_INT64( r->stated );
    PRINT_INT64( r->sendtime );
    PRINT_STRING( r->message );
    PRINT_STRING( r->msgtag );
    PRINT_INT64( r->BRAND );
    PRINT_INT64( r->PCI );
    PRINT_INT64( r->USERNAME );
    PRINT_INT64( r->TERMINATECAUSE );
    PRINT_INT64( r->WX_OPEN_ID );
    PRINT_INT64( r->WX_TID );
    PRINT_INT64( r->MSISDN_FY_TF );
    PRINT_INT64( r->MSISDN_FY_TIME );
    PRINT_INT64( r->IMSI_FY_TF );
    PRINT_INT64( r->IMSI_FY_TIME );
    PRINT_INT64( r->IMEI_FY_TF );
    PRINT_INT64( r->IMEI_FY_TIME );
    PRINT_INT64( r->MAC_FY_TF );
    PRINT_INT64( r->MAC_FY_TIME );
    PRINT_INT64( r->USERNAME_FY_TF );

    if( r->USERNAME_FY_TIME == INT64_NULL )
        fputs( "null)", fp );
    else
        fprintf( fp, "%" PRId64 ")", r->USERNAME_FY_TIME );

    of->num_records++;
    if( of->num_records % 10 == 0 )
        fputs( ";\n", fp );

    return true;
}



static int compare_record( const void* a, const void* b )
{
    const Record* x = *(const Record**)a;
    const Record* y = *(const Record**)b;
    if( x->catchtime != y->catchtime )
        return y->catchtime - x ->catchtime;
    return strcmp( x->equid, y->equid );
}



static bool dump_output( bool writeAll )
{
    // sort records by timestamp(in reverse order) and equipment id
    qsort( g_records, g_num_records, sizeof(Record*), compare_record );

    int32_t last = 0;
    if( !writeAll )
    {
        last = g_num_records / 4;
        for( int64_t ts = g_records[last]->catchtime; last > 0; --last )
            if( g_records[last - 1]->catchtime != ts )
                break;
    }

    int64_t lastTs = 0, lastTsRepeat = 0;
    const char* lastEquip = "";
    for( int32_t i = g_num_records - 1; i >= last; --i )
    {
        Record* r = g_records[i];
        if( r->catchtime == lastTs && strcmp(lastEquip, r->equid) == 0 )
        {
            r->catchtime += lastTsRepeat++;
        }
        else
        {
            lastTs = r->catchtime;
            lastEquip = r->equid;
            lastTsRepeat = 1;
        }
        
        if( !write_one_row( r ) )
            return false;
        r->next = g_next_free_record;
        g_next_free_record = r;
    }

    g_num_records = last;
    return true;
}



static bool parse_file( const char* path, int (*parser)(char*, Record*) )
{
    char buf[8192];
    join_path( buf, g_inpath, path );
    FILE* fp = fopen( buf, "r" );
    if( fp == NULL )
    {
        printf( "failed to open %s.\n", path );
        return false;
    }

    for( int line = 1; fgets(buf, sizeof(buf), fp) != NULL; ++line )
    {
        Record* r = g_next_free_record;
        g_next_free_record = r->next;
        g_records[g_num_records] = r;

        memset( r, 0, sizeof(*r) );
        int column = parser( buf, r );
        if( column != 0 )
        {
            printf( "parse error: %s (%d : %d).\n", path, line, column );
            r->next = g_next_free_record;
            g_next_free_record = r;
            continue;
        }
        if( r->catchtime < 946684800 || r->catchtime > g_now )
        {
            printf( "invalid timestamp: %s (%d).\n", path, line );
            r->next = g_next_free_record;
            g_next_free_record = r;
            continue;
        }
        r->catchtime *= 1000; // convert to ms
        g_num_records++;
    }

    fclose( fp );

    if( g_num_records > MAX_NUM_OF_RECORD / 8 * 7 )
        return dump_output( false );

    return true;
}



static bool parse_files()
{
    bool ok = true;

    for( uint32_t i = 0; ok && i < g_num_input_files; ++i )
    {
        int (*parser)( char*, Record* );

        InputFile* in = g_input_files + i;
        switch( in->category )
        {
        case FILE_CATEGORY_RD:
            parser = parse_line_rd;
            break;
        case FILE_CATEGORY_CDR:
            parser = parse_line_cdr;
            break;
        case FILE_CATEGORY_EVT:
            parser = parse_line_evt;
            break;
        case FILE_CATEGORY_SMS:
            parser = parse_line_sms;
            break;
        default:
            printf( "unknown category: %s\n", in->path );
            continue;
        }

        ok = parse_file( in->path, parser );
    }

    ok = ok && dump_output( true );

    return ok;
}



static void close_output_files()
{
    if( g_output_files != NULL ) {
        for( int i = 0; i < MAX_NUM_OF_OUTPUT_FILE; ++i )
        {
            OutputFile* of = g_output_files + i;
            if( of->fp != NULL )
            {
                if( of->num_records % 10 != 0 )
                    fprintf( of->fp, ";\n" );
                fclose( of->fp );
                of->fp = NULL;
            }
        }
    }

    if( g_table_file != NULL )
    {
        fclose( g_table_file );
        g_table_file = NULL;
    }

    taosMemoryFree( g_output_files );
    g_output_files = NULL;
}



static bool create_output_files()
{
    g_output_files = (OutputFile*)calloc( MAX_NUM_OF_OUTPUT_FILE, sizeof(OutputFile) );
    if( g_output_files == NULL )
    {
        printf( "failed to allocate memory for output files\n" );
        return false;
    }

    char path[256];
    join_path( path, g_outpath, "tables.sql" );
    g_table_file = fopen( path, "w+" );
    if( g_table_file == NULL )
    {
        printf( "failed to create table.sql\n" );
        return false;
    }
    fprintf(g_table_file, "use %s;\n", g_database);

    for( int i = 0; i < MAX_NUM_OF_OUTPUT_FILE; ++i )
    {
        char buf[16];
        sprintf( buf, "%04d.sql", i );
        join_path( path, g_outpath, buf );
        g_output_files[i].fp = fopen( path, "w+" );
        if( g_output_files[i].fp == NULL )
        {
            printf( "failed to create output files\n" );
            return false;
        }
        fprintf(g_output_files[i].fp, "use %s;\n", g_database);
    }

    return true;
}



static bool run()
{
    bool ret = false;
    if( !build_file_list() )
        goto clean;
    
    if( g_num_input_files == 0 )
    {
        ret = true;
        goto clean;
    }

    if( !create_output_files() )
        goto clean;

    g_records_written = 0;

    g_tables = (TableEntry**)calloc( MAX_NUM_OF_TABLE_ENTRY, sizeof(TableEntry*) );
    if( g_tables == NULL )
    {
        puts( "failed to allocate memory for tables." );
        goto clean;
    }

    g_record_pool = (Record*)taosMemoryMalloc( sizeof(Record) * MAX_NUM_OF_RECORD );
    if( g_record_pool == NULL )
    {
        puts( "failed to allocate record pool." );
        goto clean;
    }
    for( int i = 0; i < MAX_NUM_OF_RECORD; i++ )
        g_record_pool[i].next = g_record_pool + i + 1;
    g_record_pool[MAX_NUM_OF_RECORD - 1].next = NULL;
    g_next_free_record = g_record_pool;

    g_records = (Record**)taosMemoryMalloc( sizeof(Record*) * MAX_NUM_OF_RECORD );
    if( g_records == NULL )
    {
        puts( "failed to allocate memory for records." );
        goto clean;
    }

    g_num_records = 0;
    ret = parse_files();

clean:
    close_output_files();

    if (g_tables != NULL) {
        for( int i = 0; i < MAX_NUM_OF_TABLE_ENTRY; ++i )
        {
            TableEntry* te = g_tables[i];
            while( te != NULL )
            {
                TableEntry* next = te->next;
                taosMemoryFree( te );
                te = next;
            }
        }
        taosMemoryFree( g_tables );
        g_tables = NULL;
    }

    taosMemoryFree( g_records );
    g_records = NULL;
    taosMemoryFree( g_record_pool );
    g_record_pool = NULL;
    g_next_free_record = NULL;
    g_num_records = 0;

    taosMemoryFree( g_input_files );
    g_input_files = NULL;

    return ret;
}



static void show_usage()
{
    puts( "USAGE: jinfo -in=<input path> -out=<output path>" );
}



int main( int argc, char** argv )
{
    g_now = time( NULL );

    for( int i = 1; i < argc; ++i )
    {
        if( strncmp(argv[i], "-in=", 4) == 0 )
        {
            g_inpath = argv[i] + 4;
            if( g_inpath[0] != 0 )
                continue;
        }
        else if( strncmp(argv[i], "-out=", 5) == 0 )
        {
            g_outpath = argv[i] + 5;
            if( g_outpath[0] != 0 )
                continue;
        }
        else if( strncmp(argv[i], "-db=", 4) == 0 )
        {
            g_database = argv[i] + 4;
            if( g_database[0] != 0 )
                continue;
        }
        else if( strncmp(argv[i], "-stable=", 8) == 0 )
        {
            g_stable = argv[i] + 8;
            if( g_stable[0] != 0 )
                continue;
        }
        show_usage();
        return 1;
    }

    return run() ? 0 : 1;
}
