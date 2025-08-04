#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dlfcn.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/time.h>
#include <stdatomic.h>
#include <time.h>

typedef void* TAOS;
typedef void* TAOS_RES;
typedef TAOS* (*taos_connect_f)(const char*, const char*, const char*, const char*, int);
typedef int (*taos_query_a_f)(TAOS*, const char*, void (*)(void*, TAOS_RES*, int), void*);
typedef void (*taos_free_result_f)(TAOS_RES*);
typedef void (*taos_close_f)(TAOS*);
typedef int (*taos_errno_f)(TAOS_RES*);
typedef const char* (*taos_errstr_f)(TAOS_RES*);
static taos_connect_f taos_connect_func = NULL;
static taos_query_a_f taos_query_a_func = NULL;
static taos_free_result_f taos_free_result_func = NULL;
static taos_close_f taos_close_func = NULL;
static taos_errno_f taos_errno_func = NULL;
static taos_errstr_f taos_errstr_func = NULL;

typedef struct {
    int thread_id;
    TAOS* taos;
    char* sql;
    int queries_per_thread;
    sem_t* sem;
    pthread_mutex_t* mutex;
    struct timeval start_time;
    struct timeval end_time;
    double* thread_qps;
    int current_query;
} ThreadParam;

static struct timeval global_start_time;
static struct timeval global_end_time;

static double calculate_qps(int count, double duration) {
    return duration > 0 ? count / duration : 0.0;
}

static int load_taos_functions(const char* lib_path) {
    void* handle = dlopen(lib_path, RTLD_LAZY);
    if (!handle) {
        fprintf(stderr, "Failed to load library %s: %s\n", lib_path, dlerror());
        return -1;
    }

    taos_connect_func = (taos_connect_f)dlsym(handle, "taos_connect");
    taos_query_a_func = (taos_query_a_f)dlsym(handle, "taos_query_a");
    taos_free_result_func = (taos_free_result_f)dlsym(handle, "taos_free_result");
    taos_close_func = (taos_close_f)dlsym(handle, "taos_close");
    taos_errno_func = (taos_errno_f)dlsym(handle, "taos_errno");
    taos_errstr_func = (taos_errstr_f)dlsym(handle, "taos_errstr");

    if (!taos_connect_func || !taos_query_a_func || !taos_free_result_func || 
        !taos_close_func || !taos_errno_func || !taos_errstr_func) {
        fprintf(stderr, "Failed to load some functions: %s\n", dlerror());
        dlclose(handle);
        return -1;
    }

    return 0;
}

static void queryCallback(void* param, TAOS_RES* res, int code) {
    if (code != 0) {
        exit(1);
    }
    ThreadParam* thread_param = (ThreadParam*)param;
    
    if (res != NULL) {
        taos_free_result_func(res);
    }
    
    thread_param->current_query++;
    
    // 如果还有查询要执行，继续下一个查询
    if (thread_param->current_query < thread_param->queries_per_thread) {
        taos_query_a_func(thread_param->taos, thread_param->sql, queryCallback, thread_param);
    } else {
        // 所有查询完成，记录结束时间并计算QPS
        gettimeofday(&thread_param->end_time, NULL);
        
        double start_sec = thread_param->start_time.tv_sec + thread_param->start_time.tv_usec / 1000000.0;
        double end_sec = thread_param->end_time.tv_sec + thread_param->end_time.tv_usec / 1000000.0;
        double duration = end_sec - start_sec;
        *thread_param->thread_qps = calculate_qps(thread_param->queries_per_thread, duration);
        
        
        printf("Thread %d QPS: %.2f\n", thread_param->thread_id, *thread_param->thread_qps);
        
        // 通知主线程此线程完成
        sem_post(thread_param->sem);
    }
}

static void* query_thread(void* arg) {
    ThreadParam* param = (ThreadParam*)arg;
    
    // 初始化线程参数
    param->current_query = 0;
    
    gettimeofday(&param->start_time, NULL);
    
    // 开始第一个查询
    taos_query_a_func(param->taos, param->sql, queryCallback, param);
    
    return NULL;
}

int main(int argc, char* argv[]) {
    if (argc < 7) {
        printf("Usage: %s <lib_path> <host> <user> <pass> <database> <sql_prefix> [thread_count] [queries_per_thread]\n", argv[0]);
        printf("Example: %s /usr/lib/libtaos.so localhost root taosdata test \"select last(ts, r32) from test.d\" 4 10\n", argv[0]);
        return 1;
    }

    const char* lib_path = argv[1];
    const char* host = argv[2];
    const char* user = argv[3];
    const char* pass = argv[4];
    const char* db = argv[5];
    const char* sql = argv[6];
    int thread_count = (argc > 7) ? atoi(argv[7]) : 4;
    int queries_per_thread = (argc > 8) ? atoi(argv[8]) : 1;
    const char* sub_table_prefix = (argc > 9) ? argv[9] : "";

    if (load_taos_functions(lib_path) != 0) {
        fprintf(stderr, "Failed to load TDengine functions\n");
        return 1;
    }



    sem_t sem;
    sem_init(&sem, 0, 0);
    
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    ThreadParam* params = malloc(thread_count * sizeof(ThreadParam));
    pthread_t* threads = malloc(thread_count * sizeof(pthread_t));
    double* thread_qps = malloc(thread_count * sizeof(double));

    printf("Starting %d threads, each executing %d queries...\n", 
           thread_count, queries_per_thread);

    gettimeofday(&global_start_time, NULL);

    for (int i = 0; i < thread_count; i++) {
        int random_number = rand() % 10000 + 1;
        char* full_sql = malloc(strlen(sql) + strlen(sub_table_prefix) + 10);
        sprintf(full_sql, "%s%s%d", sql, sub_table_prefix, random_number);
        TAOS* taos = taos_connect_func(host, user, pass, db, 0);
        if (taos == NULL) {
            fprintf(stderr, "Failed to connect to TDengine\n");
            return 1;
        }
        params[i].thread_id = i;
        params[i].taos = taos;
        params[i].sql = full_sql;
        params[i].queries_per_thread = queries_per_thread;
        params[i].sem = &sem;
        params[i].mutex = &mutex;
        params[i].thread_qps = &thread_qps[i];
        thread_qps[i] = 0.0;

        printf("full_sql: %s\n", full_sql);

        if (pthread_create(&threads[i], NULL, query_thread, &params[i]) != 0) {
            fprintf(stderr, "Failed to create thread %d\n", i);
            return 1;
        }
    }

    for (int i = 0; i < thread_count; i++) {
        sem_wait(&sem);
    }

    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    gettimeofday(&global_end_time, NULL);

    double global_start_sec = global_start_time.tv_sec + global_start_time.tv_usec / 1000000.0;
    double global_end_sec = global_end_time.tv_sec + global_end_time.tv_usec / 1000000.0;
    double global_duration = global_end_sec - global_start_sec;
    int total_count = thread_count * queries_per_thread;
    
    // 计算每个线程QPS的总和
    double total_thread_qps = 0.0;
    for (int i = 0; i < thread_count; i++) {
        total_thread_qps += thread_qps[i];
    }

    printf("\nTotal QPS (sum of all threads): %.2f queries/second\n", total_thread_qps);

    free(params);
    free(threads);
    free(thread_qps);
    sem_destroy(&sem);
    pthread_mutex_destroy(&mutex);
    return 0;
} 