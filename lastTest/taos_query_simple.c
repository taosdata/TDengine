#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dlfcn.h>
#include <pthread.h>
#include <sys/time.h>

typedef void* TAOS;
typedef void* TAOS_RES;
typedef TAOS* (*taos_connect_f)(const char*, const char*, const char*, const char*, int);
typedef TAOS_RES* (*taos_query_f)(TAOS*, const char*);
typedef void (*taos_free_result_f)(TAOS_RES*);
typedef void (*taos_close_f)(TAOS*);
typedef int (*taos_errno_f)(TAOS_RES*);
typedef const char* (*taos_errstr_f)(TAOS_RES*);

static taos_connect_f taos_connect_func = NULL;
static taos_query_f taos_query_func = NULL;
static taos_free_result_f taos_free_result_func = NULL;
static taos_close_f taos_close_func = NULL;
static taos_errno_f taos_errno_func = NULL;
static taos_errstr_f taos_errstr_func = NULL;

typedef struct {
    int thread_id;
    const char* host;
    const char* user;
    const char* pass;
    const char* db;
    const char* sql;
    int queries_per_thread;
    pthread_mutex_t* mutex;
    int* total_success;
    int* total_queries;
} ThreadParam;

static int load_taos_functions(const char* lib_path) {
    void* handle = dlopen(lib_path, RTLD_LAZY);
    if (!handle) {
        fprintf(stderr, "Failed to load library %s: %s\n", lib_path, dlerror());
        return -1;
    }

    taos_connect_func = (taos_connect_f)dlsym(handle, "taos_connect");
    taos_query_func = (taos_query_f)dlsym(handle, "taos_query");
    taos_free_result_func = (taos_free_result_f)dlsym(handle, "taos_free_result");
    taos_close_func = (taos_close_f)dlsym(handle, "taos_close");
    taos_errno_func = (taos_errno_f)dlsym(handle, "taos_errno");
    taos_errstr_func = (taos_errstr_f)dlsym(handle, "taos_errstr");

    if (!taos_connect_func || !taos_query_func || !taos_free_result_func || 
        !taos_close_func || !taos_errno_func || !taos_errstr_func) {
        fprintf(stderr, "Failed to load some functions: %s\n", dlerror());
        dlclose(handle);
        return -1;
    }

    return 0;
}

static void* query_thread(void* arg) {
    ThreadParam* param = (ThreadParam*)arg;
    
    // 每个线程创建自己的连接
    TAOS* taos = taos_connect_func(param->host, param->user, param->pass, param->db, 0);
    if (taos == NULL) {
        fprintf(stderr, "Thread %d: Failed to connect to TDengine\n", param->thread_id);
        return NULL;
    }
    
    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    
    int success_count = 0;
    
    // 简单的循环执行查询
    for (int i = 0; i < param->queries_per_thread; i++) {
        TAOS_RES* res = taos_query_func(taos, param->sql);
        
        if (res != NULL && taos_errno_func(res) == 0) {
            success_count++;
        }
        
        if (res != NULL) {
            taos_free_result_func(res);
        }
        
        // 更新全局计数
        pthread_mutex_lock(param->mutex);
        (*param->total_queries)++;
        pthread_mutex_unlock(param->mutex);
    }
    
    gettimeofday(&end_time, NULL);
    
    double start_sec = start_time.tv_sec + start_time.tv_usec / 1000000.0;
    double end_sec = end_time.tv_sec + end_time.tv_usec / 1000000.0;
    double duration = end_sec - start_sec;
    double qps = duration > 0 ? param->queries_per_thread / duration : 0.0;
    
    printf("Thread %d QPS: %.2f\n", param->thread_id, qps);
    
    // 更新全局成功计数
    pthread_mutex_lock(param->mutex);
    (*param->total_success) += success_count;
    pthread_mutex_unlock(param->mutex);
    
    taos_close_func(taos);
    return NULL;
}

int main(int argc, char* argv[]) {
    if (argc < 7) {
        printf("Usage: %s <lib_path> <host> <user> <pass> <database> <sql> [thread_count] [queries_per_thread]\n", argv[0]);
        printf("Example: %s /usr/lib/libtaos.so localhost root taosdata test \"SELECT * FROM meters\" 4 10\n", argv[0]);
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

    if (load_taos_functions(lib_path) != 0) {
        fprintf(stderr, "Failed to load TDengine functions\n");
        return 1;
    }

    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    int total_success = 0;
    int total_queries = 0;

    ThreadParam* params = malloc(thread_count * sizeof(ThreadParam));
    pthread_t* threads = malloc(thread_count * sizeof(pthread_t));

    printf("Starting %d threads, each executing %d queries...\n", 
           thread_count, queries_per_thread);

    struct timeval global_start_time, global_end_time;
    gettimeofday(&global_start_time, NULL);

    // 创建线程
    for (int i = 0; i < thread_count; i++) {
        params[i].thread_id = i;
        params[i].host = host;
        params[i].user = user;
        params[i].pass = pass;
        params[i].db = db;
        params[i].sql = sql;
        params[i].queries_per_thread = queries_per_thread;
        params[i].mutex = &mutex;
        params[i].total_success = &total_success;
        params[i].total_queries = &total_queries;

        if (pthread_create(&threads[i], NULL, query_thread, &params[i]) != 0) {
            fprintf(stderr, "Failed to create thread %d\n", i);
            return 1;
        }
    }

    // 等待所有线程完成
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    gettimeofday(&global_end_time, NULL);

    double global_start_sec = global_start_time.tv_sec + global_start_time.tv_usec / 1000000.0;
    double global_end_sec = global_end_time.tv_sec + global_end_time.tv_usec / 1000000.0;
    double global_duration = global_end_sec - global_start_sec;
    double global_qps = global_duration > 0 ? total_queries / global_duration : 0.0;

    printf("\nTotal QPS: %.2f queries/second\n", global_qps);

    free(params);
    free(threads);
    pthread_mutex_destroy(&mutex);
    
    return 0;
}