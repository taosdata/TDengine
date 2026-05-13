//go:build windows

// Package asynctmq is a cgo wrapper for TDengine tmq API
package asynctmq

/*
#cgo CFLAGS: -IC:/TDengine/include -I/usr/include
#cgo linux LDFLAGS: -L/usr/lib -ltaosnative
#cgo windows LDFLAGS: -LC:/TDengine/driver -ltaosnative
#cgo darwin LDFLAGS: -L/usr/local/lib -ltaosnative


#include <windows.h>
#include <process.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

typedef enum {
    TAOSA_TMQ_POLL = 1,
    TAOSA_TMQ_FREE = 2,
    TAOSA_TMQ_COMMIT = 3,
    TAOSA_TMQ_FETCH_RAW_BLOCK = 4,
    TAOSA_TMQ_NEW_CONSUMER = 5,
    TAOSA_TMQ_SUBSCRIBE = 6,
    TAOSA_TMQ_UNSUBSCRIBE = 7,
    TAOSA_TMQ_CONSUMER_CLOSE = 8,
    TAOSA_TMQ_GET_RAW = 9,
    TAOSA_TMQ_GET_JSON_META = 10,
    TAOSA_TMQ_GET_TOPIC_ASSIGNMENT = 11,
    TAOSA_TMQ_OFFSET_SEEK = 12,
    TAOSA_TMQ_COMMIT_OFFSET = 13,
    TAOSA_TMQ_COMMITTED = 14,
    TAOSA_TMQ_POSITION = 15,
} TMQ_EVENT;

typedef struct tmq_thread {
    TMQ_EVENT event;
    int shutdown;
    void *task;
    CRITICAL_SECTION lock;
    CONDITION_VARIABLE notify;
    HANDLE thread;
} tmq_thread;

typedef void (*adapter_tmq_poll_a_cb)(uintptr_t param, void *res, int32_t code, const char* errstr);

typedef void(*adapter_tmq_free_result_cb)(uintptr_t param);

typedef void(*adapter_tmq_commit_cb)(uintptr_t param, int32_t code);

typedef void (*adapter_tmq_fetch_raw_block_cb)(uintptr_t param, int32_t code, int32_t block_size, void *pData);

typedef void(*adapter_tmq_new_consumer_cb)(uintptr_t param, tmq_t *tmq, char *errstr);

typedef void(*adapter_tmq_subscribe_cb)(uintptr_t param, int32_t errcode);

typedef void(*adapter_tmq_unsubscribe_cb)(uintptr_t param, int32_t errcode);

typedef void(*adapter_tmq_consumer_close_cb)(uintptr_t param, int32_t errcode);

typedef void(*adapter_tmq_get_raw_cb)(uintptr_t param, int32_t errcode);

typedef void(*adapter_tmq_get_json_meta_cb)(uintptr_t param, char *meta);

typedef void(*adapter_tmq_get_topic_assignment_cb)(uintptr_t param, char *topic, int32_t errcode, tmq_topic_assignment *assignment,
                                                   int32_t numOfAssignment);

typedef void(*adapter_tmq_offset_seek_cb)(uintptr_t param, char *topic, int32_t code);

typedef void(*adapter_tmq_commit_offset_cb)(uintptr_t param, char *topic_name, int32_t code);

typedef void(*adapter_tmq_committed_cb)(uintptr_t param, char *topic_name, int64_t errcode);

typedef void(*adapter_tmq_position_cb)(uintptr_t param, char *topic_name, int64_t errcode);

typedef struct poll_task {
    tmq_t *tmq;
    int64_t timeout;
    uintptr_t param;
    adapter_tmq_poll_a_cb cb;
} poll_task;

typedef struct free_task {
    TAOS_RES *res;
    adapter_tmq_free_result_cb cb;
    uintptr_t param;
} free_task;

typedef struct commit_task {
    tmq_t *tmq;
    TAOS_RES *msg;
    adapter_tmq_commit_cb cb;
    uintptr_t param;
} commit_task;

typedef struct fetch_raw_block_task {
    TAOS_RES *res;
    adapter_tmq_fetch_raw_block_cb cb;
    uintptr_t param;
} fetch_raw_block_task;

typedef struct new_consumer_task {
    tmq_conf_t *conf;
    char *errstr;
    int32_t errstrLen;
    adapter_tmq_new_consumer_cb cb;
    uintptr_t param;
} new_consumer_task;

typedef struct subscribe_task {
    tmq_t *tmq;
    tmq_list_t *topic_list;
    adapter_tmq_subscribe_cb cb;
    uintptr_t param;
} subscribe_task;

typedef struct unsubscribe_task {
    tmq_t *tmq;
    adapter_tmq_unsubscribe_cb cb;
    uintptr_t param;
} unsubscribe_task;

typedef struct consumer_close_task {
    tmq_t *tmq;
    adapter_tmq_consumer_close_cb cb;
    uintptr_t param;
} consumer_close_task;

typedef struct tmq_get_raw_task {
    TAOS_RES *res;
    tmq_raw_data *raw;
    adapter_tmq_get_raw_cb cb;
    uintptr_t param;
} tmq_get_raw_task;

typedef struct tmq_get_json_meta_task {
    TAOS_RES *res;
    adapter_tmq_get_json_meta_cb cb;
    uintptr_t param;
} tmq_get_json_meta_task;

typedef struct tmq_get_topic_assignment_task {
    tmq_t *tmq;
    char *topic_name;
    adapter_tmq_get_topic_assignment_cb cb;
    uintptr_t param;
} tmq_get_topic_assignment_task;

typedef struct tmq_offset_seek_task {
    tmq_t *tmq;
    char *topic_name;
    int32_t vgId;
    int64_t offset;
    adapter_tmq_offset_seek_cb cb;
    uintptr_t param;
} tmq_offset_seek_task;

typedef struct tmq_commit_offset_task {
    tmq_t *tmq;
    char *topic_name;
    int32_t vgId;
    int64_t offset;
    adapter_tmq_commit_offset_cb cb;
    uintptr_t param;
} tmq_commit_offset_task;

typedef struct tmq_committed_task {
    tmq_t *tmq;
    char *topic_name;
    int32_t vgId;
    adapter_tmq_committed_cb cb;
    uintptr_t param;
} tmq_committed_task;

typedef struct tmq_position_task {
    tmq_t *tmq;
    char *topic_name;
    int32_t vgId;
    adapter_tmq_position_cb cb;
    uintptr_t param;
} tmq_position_task;

unsigned int worker(void *arg) {
    tmq_thread *thread_info = (tmq_thread *) arg;
    while (1) {
        EnterCriticalSection(&thread_info->lock);
        while (thread_info->task == NULL && !thread_info->shutdown) {
            SleepConditionVariableCS(&thread_info->notify, &thread_info->lock, INFINITE);
        }
        if (thread_info->shutdown) {
            LeaveCriticalSection(&thread_info->lock);
            break;
        }
        int need_free = 1;
        switch (thread_info->event) {
            case TAOSA_TMQ_FETCH_RAW_BLOCK: {
                fetch_raw_block_task *task = (fetch_raw_block_task *) thread_info->task;
                int num_of_rows;
                void *data;
                int32_t err_code = taos_fetch_raw_block(task->res, &num_of_rows, &data);
                task->cb(task->param, err_code, num_of_rows, data);
                break;
            }
            case TAOSA_TMQ_POLL: {
                poll_task *task = (poll_task *) thread_info->task;
                void *res = tmq_consumer_poll(task->tmq, task->timeout);
                if (res == NULL) {
                    int32_t code = taos_errno(NULL);
                    if (code != 0) {
                        const char *errstr = taos_errstr(NULL);
                        task->cb(task->param, res, code, errstr);
                        break;
                    }
                }
                task->cb(task->param, res, 0, NULL);
                break;
            }
            case TAOSA_TMQ_FREE: {
                free_task *task = (free_task *) thread_info->task;
                taos_free_result(task->res);
                task->cb(task->param);
                break;
            }
            case TAOSA_TMQ_COMMIT: {
                commit_task *task = (commit_task *) thread_info->task;
                int32_t err_code = tmq_commit_sync(task->tmq, task->msg);
                task->cb(task->param, err_code);
                break;
            }
            case TAOSA_TMQ_NEW_CONSUMER: {
                new_consumer_task *task = (new_consumer_task *) thread_info->task;
                tmq_t *tmq = tmq_consumer_new(task->conf, task->errstr, task->errstrLen);
                task->cb(task->param, tmq, task->errstr);
                break;
            }
            case TAOSA_TMQ_SUBSCRIBE: {
                subscribe_task *task = (subscribe_task *) thread_info->task;
                int32_t error_code = tmq_subscribe(task->tmq, task->topic_list);
                task->cb(task->param, error_code);
                break;
            }
            case TAOSA_TMQ_UNSUBSCRIBE: {
                unsubscribe_task *task = (unsubscribe_task *) thread_info->task;
                int32_t error_code = tmq_unsubscribe(task->tmq);
                task->cb(task->param, error_code);
                break;
            }
            case TAOSA_TMQ_CONSUMER_CLOSE: {
                consumer_close_task *task = (consumer_close_task *) thread_info->task;
                int32_t error_code = tmq_consumer_close(task->tmq);
                task->cb(task->param, error_code);
                break;
            }
            case TAOSA_TMQ_GET_RAW: {
                tmq_get_raw_task *task = (tmq_get_raw_task *) thread_info->task;
                int32_t error_code = tmq_get_raw(task->res, task->raw);
                task->cb(task->param, error_code);
                break;
            }
            case TAOSA_TMQ_GET_JSON_META: {
                tmq_get_json_meta_task *task = (tmq_get_json_meta_task *) thread_info->task;
                char *meta = tmq_get_json_meta(task->res);
                task->cb(task->param, meta);
                break;
            }
            case TAOSA_TMQ_GET_TOPIC_ASSIGNMENT: {
                tmq_get_topic_assignment_task *task = (tmq_get_topic_assignment_task *) thread_info->task;
                tmq_topic_assignment *pAssign = NULL;
                int32_t numOfAssign = 0;
                int32_t code = tmq_get_topic_assignment(task->tmq, task->topic_name, &pAssign, &numOfAssign);
                if (code != 0) {
                    task->cb(task->param, task->topic_name, code, NULL, 0);
                } else {
                    task->cb(task->param, task->topic_name, 0, pAssign, numOfAssign);
                }
                if (pAssign != NULL) {
                    tmq_free_assignment(pAssign);
                }
                break;
            }
            case TAOSA_TMQ_OFFSET_SEEK: {
                tmq_offset_seek_task *task = (tmq_offset_seek_task *) thread_info->task;
                int32_t error_code = tmq_offset_seek(task->tmq, task->topic_name, task->vgId, task->offset);
                task->cb(task->param, task->topic_name, error_code);
                break;
            }
            case TAOSA_TMQ_COMMIT_OFFSET: {
                tmq_commit_offset_task *task = (tmq_commit_offset_task *) thread_info->task;
                int32_t err_code = tmq_commit_offset_sync(task->tmq, task->topic_name, task->vgId, task->offset);
                task->cb(task->param, task->topic_name, err_code);
                break;
            }
            case TAOSA_TMQ_COMMITTED: {
                tmq_committed_task *task = (tmq_committed_task *)thread_info -> task;
                int64_t code = tmq_committed(task -> tmq, task -> topic_name, task -> vgId);
                task->cb(task->param, task->topic_name, code);
                break;
            }
            case TAOSA_TMQ_POSITION: {
                tmq_position_task *task = (tmq_position_task *)thread_info -> task;
                int64_t code = tmq_position(task -> tmq, task -> topic_name, task -> vgId);
                task->cb(task->param, task->topic_name, code);
                break;
            }
            default:
                need_free = 0;
                break;
        }
        if (need_free) {
            free(thread_info->task);
            thread_info->task = NULL;
        }
        LeaveCriticalSection(&thread_info->lock);
    }
    LeaveCriticalSection(&thread_info->lock);

    _endthreadex(0);
    return 0;
}

tmq_thread *init_tmq_thread() {
    tmq_thread *thread_info = (tmq_thread *) malloc(sizeof(tmq_thread));
    thread_info->event = 0;
    thread_info->shutdown = 0;
    thread_info->task = NULL;
    InitializeCriticalSection(&thread_info->lock);
    InitializeConditionVariable(&thread_info->notify);
    thread_info->thread = (HANDLE) _beginthreadex(NULL, 0, &worker, (void *) thread_info, 0, NULL);
    return thread_info;
}

void destroy_tmq_thread(tmq_thread *thread_info) {
    if (thread_info == NULL || thread_info->shutdown) return;
    thread_info->shutdown = 1;
    WakeConditionVariable(&thread_info->notify);
    WaitForSingleObject(thread_info->thread, INFINITE);
    CloseHandle(thread_info->thread);
    DeleteCriticalSection(&thread_info->lock);
    if (thread_info->task != NULL) free(thread_info->task);
    free(thread_info);
}

void
adapter_tmq_poll_a(tmq_thread *thread_info, tmq_t *tmq, int64_t timeout, adapter_tmq_poll_a_cb cb, uintptr_t param) {
    poll_task *task = (poll_task *) malloc(sizeof(poll_task));
    task->tmq = tmq;
    task->timeout = timeout;
    task->cb = cb;
    task->param = param;
    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_POLL;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void adapter_tmq_free_result_a(tmq_thread *thread_info, TAOS_RES *res, adapter_tmq_free_result_cb cb, uintptr_t param) {
    free_task *task = (free_task *) malloc(sizeof(free_task));
    task->res = res;
    task->cb = cb;
    task->param = param;
    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_FREE;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void
adapter_tmq_commit_a(tmq_thread *thread_info, tmq_t *tmq, TAOS_RES *msg, adapter_tmq_commit_cb cb, uintptr_t param) {
    commit_task *task = (commit_task *) malloc(sizeof(commit_task));
    task->tmq = tmq;
    task->msg = msg;
    task->cb = cb;
    task->param = param;
    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_COMMIT;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void adapter_tmq_fetch_raw_block_a(tmq_thread *thread_info, TAOS_RES *res, adapter_tmq_fetch_raw_block_cb cb,
                                   uintptr_t param) {
    fetch_raw_block_task *task = (fetch_raw_block_task *) malloc(sizeof(fetch_raw_block_task));
    task->res = res;
    task->cb = cb;
    task->param = param;
    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_FETCH_RAW_BLOCK;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void adapter_tmq_new_consumer_a(tmq_thread *thread_info, tmq_conf_t *conf, char *errstr, int32_t errstrLen,
                                adapter_tmq_new_consumer_cb cb, uintptr_t param) {
    new_consumer_task *task = (new_consumer_task *) malloc(sizeof(new_consumer_task));
    task->conf = conf;
    task->errstr = errstr;
    task->errstrLen = errstrLen;
    task->cb = cb;
    task->param = param;

    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_NEW_CONSUMER;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);

}


void
adapter_tmq_subscribe_a(tmq_thread *thread_info, tmq_t *tmq, tmq_list_t *topic_list, adapter_tmq_subscribe_cb cb,
                        uintptr_t param) {
    subscribe_task *task = (subscribe_task *) malloc(sizeof(subscribe_task));
    task->tmq = tmq;
    task->topic_list = topic_list;
    task->cb = cb;
    task->param = param;

    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_SUBSCRIBE;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void adapter_tmq_unsubscribe_a(tmq_thread *thread_info, tmq_t *tmq, adapter_tmq_unsubscribe_cb cb, uintptr_t param) {
    unsubscribe_task *task = (unsubscribe_task *) malloc(sizeof(unsubscribe_task));
    task->tmq = tmq;
    task->cb = cb;
    task->param = param;

    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_UNSUBSCRIBE;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void
adapter_tmq_consumer_close_a(tmq_thread *thread_info, tmq_t *tmq, adapter_tmq_consumer_close_cb cb, uintptr_t param) {
    consumer_close_task *task = (consumer_close_task *) malloc(sizeof(consumer_close_task));
    task->tmq = tmq;
    task->cb = cb;
    task->param = param;

    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_CONSUMER_CLOSE;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void adapter_tmq_get_raw_a(tmq_thread *thread_info, TAOS_RES *res, tmq_raw_data *raw, adapter_tmq_get_raw_cb cb,
                           uintptr_t param) {
    tmq_get_raw_task *task = (tmq_get_raw_task *) malloc(sizeof(tmq_get_raw_task));
    task->res = res;
    task->raw = raw;
    task->cb = cb;
    task->param = param;

    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_GET_RAW;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void
adapter_tmq_get_json_meta_a(tmq_thread *thread_info, TAOS_RES *res, adapter_tmq_get_json_meta_cb cb, uintptr_t param) {
    tmq_get_json_meta_task *task = (tmq_get_json_meta_task *) malloc(sizeof(tmq_get_json_meta_task));
    task->res = res;
    task->cb = cb;
    task->param = param;
    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_GET_JSON_META;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void adapter_tmq_get_topic_assignment_a(tmq_thread *thread_info, tmq_t *tmq, char *topic_name,
                                        adapter_tmq_get_topic_assignment_cb cb, uintptr_t param) {
    tmq_get_topic_assignment_task *task = (tmq_get_topic_assignment_task *) malloc(
            sizeof(tmq_get_topic_assignment_task));
    task->tmq = tmq;
    task->topic_name = topic_name;
    task->cb = cb;
    task->param = param;
    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_GET_TOPIC_ASSIGNMENT;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void
adapter_tmq_offset_seek_a(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId, int64_t offset,
                          adapter_tmq_offset_seek_cb cb, uintptr_t param) {
    tmq_offset_seek_task *task = (tmq_offset_seek_task *) malloc(sizeof(tmq_offset_seek_task));
    task->tmq = tmq;
    task->topic_name = topic_name;
    task->vgId = vgId;
    task->offset = offset;
    task->cb = cb;
    task->param = param;
    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_OFFSET_SEEK;
    thread_info->task = task;
    WakeConditionVariable(&thread_info->notify);
    LeaveCriticalSection(&thread_info->lock);
}

void adapter_tmq_commit_offset(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId, int64_t offset,
                          adapter_tmq_commit_offset_cb cb, uintptr_t param) {
    tmq_commit_offset_task *task = (tmq_commit_offset_task *) malloc(sizeof(tmq_commit_offset_task));
    task -> tmq = tmq;
    task -> topic_name = topic_name;
    task -> vgId = vgId;
    task -> offset = offset;
    task -> cb = cb;
    task -> param = param;

    EnterCriticalSection(&thread_info->lock);
    thread_info->event = TAOSA_TMQ_COMMIT_OFFSET;
    thread_info->task = task;
    WakeConditionVariable(&(thread_info->notify));
    LeaveCriticalSection(&(thread_info->lock));
}

void adapter_tmq_committed(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId,
                        adapter_tmq_committed_cb cb, uintptr_t param) {
    tmq_committed_task *task = (tmq_committed_task *) malloc(sizeof(tmq_committed_task));
    task -> tmq = tmq;
    task -> topic_name = topic_name;
    task -> vgId = vgId;
    task -> cb = cb;
    task -> param = param;

    EnterCriticalSection(&(thread_info->lock));
    thread_info->event = TAOSA_TMQ_COMMITTED;
    thread_info->task = task;
    WakeConditionVariable(&(thread_info->notify));
    LeaveCriticalSection(&(thread_info->lock));
}

void adapter_tmq_position(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId,
                        adapter_tmq_position_cb cb, uintptr_t param) {
    tmq_position_task *task = (tmq_position_task *) malloc(sizeof(tmq_position_task));
    task -> tmq = tmq;
    task -> topic_name = topic_name;
    task -> vgId = vgId;
    task -> cb = cb;
    task -> param = param;

    EnterCriticalSection(&(thread_info->lock));
    thread_info->event = TAOSA_TMQ_POSITION;
    thread_info->task = task;
    WakeConditionVariable(&(thread_info->notify));
    LeaveCriticalSection(&(thread_info->lock));
}

extern void AdapterTMQPollCallback(uintptr_t param, void *res, int32_t code, const char* errstr);

extern void AdapterTMQFreeResultCallback(uintptr_t param);

extern void AdapterTMQCommitCallback(uintptr_t param, int32_t code);

extern void AdapterTMQFetchRawBlockCallback(uintptr_t param, int32_t code, int32_t block_size, void *pData);

extern void AdapterTMQNewConsumerCallback(uintptr_t param, tmq_t *tmq, char *errstr);

extern void AdapterTMQSubscribeCallback(uintptr_t param, int32_t errcode);

extern void AdapterTMQUnsubscribeCallback(uintptr_t param, int32_t errcode);

extern void AdapterTMQConsumerCloseCallback(uintptr_t param, int32_t errcode);

extern void AdapterTMQGetRawCallback(uintptr_t param, int32_t errcode);

extern void AdapterTMQGetJsonMetaCallback(uintptr_t param, char *meta);

extern void AdapterTMQGetTopicAssignmentCallback(uintptr_t param, char *topic_name, int32_t errcode, tmq_topic_assignment *assignment,
                                                 int32_t numOfAssignment);

extern void AdapterTMQOffsetSeekCallback(uintptr_t param, char *topic_name, int32_t errcode);

extern void AdapterTMQCommitOffsetCallback(uintptr_t param, char *topic_name, int32_t errcode);

extern void AdapterTMQCommittedCallback(uintptr_t param, char *topic_name, int64_t code);

extern void AdapterTMQPositionCallback(uintptr_t param, char *topic_name, int64_t code);

void taosa_tmq_poll_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, int64_t timeout, uintptr_t param) {
    return adapter_tmq_poll_a(thread_info, tmq, timeout, AdapterTMQPollCallback, param);
};

void taosa_tmq_free_result_a_wrapper(tmq_thread *thread_info, TAOS_RES *res, uintptr_t param) {
    return adapter_tmq_free_result_a(thread_info, res, AdapterTMQFreeResultCallback, param);
};

void taosa_tmq_commit_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, TAOS_RES *msg, uintptr_t param) {
    return adapter_tmq_commit_a(thread_info, tmq, msg, AdapterTMQCommitCallback, param);
};

void taosa_tmq_fetch_raw_block_a_wrapper(tmq_thread *thread_info, TAOS_RES *res, uintptr_t param) {
    return adapter_tmq_fetch_raw_block_a(thread_info, res, AdapterTMQFetchRawBlockCallback, param);
};

void taosa_tmq_new_consumer_a_wrapper(tmq_thread *thread_info, tmq_conf_t *conf, char *errstr, int32_t errstrLen,
                                      uintptr_t param) {
    return adapter_tmq_new_consumer_a(thread_info, conf, errstr, errstrLen, AdapterTMQNewConsumerCallback, param);
}

void taosa_tmq_subscribe_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, tmq_list_t *topic_list, uintptr_t param) {
    adapter_tmq_subscribe_a(thread_info, tmq, topic_list, AdapterTMQSubscribeCallback, param);
}

void taosa_tmq_unsubscribe_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, uintptr_t param) {
    adapter_tmq_unsubscribe_a(thread_info, tmq, AdapterTMQUnsubscribeCallback, param);
}

void taosa_tmq_consumer_close_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, uintptr_t param) {
    adapter_tmq_consumer_close_a(thread_info, tmq, AdapterTMQConsumerCloseCallback, param);
}

void taosa_tmq_get_raw_a_wrapper(tmq_thread *thread_info, TAOS_RES *res, tmq_raw_data *raw, uintptr_t param) {
    adapter_tmq_get_raw_a(thread_info, res, raw, AdapterTMQGetRawCallback, param);
}

void taosa_tmq_get_json_meta_a_wrapper(tmq_thread *thread_info, TAOS_RES *res, uintptr_t param) {
    adapter_tmq_get_json_meta_a(thread_info, res, AdapterTMQGetJsonMetaCallback, param);
}

void taosa_tmq_get_topic_assignment_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, uintptr_t param) {
    adapter_tmq_get_topic_assignment_a(thread_info, tmq, topic_name, AdapterTMQGetTopicAssignmentCallback, param);
}

void taosa_tmq_offset_seek_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId,
                                     int64_t offset, uintptr_t param) {
    adapter_tmq_offset_seek_a(thread_info, tmq, topic_name, vgId, offset, AdapterTMQOffsetSeekCallback, param);
}

void taosa_tmq_commit_offset_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId,
                                    int64_t offset, uintptr_t param) {
    adapter_tmq_commit_offset(thread_info, tmq, topic_name, vgId, offset, AdapterTMQCommitOffsetCallback, param);
}

void taosa_tmq_committed_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId, uintptr_t param) {
    adapter_tmq_committed(thread_info, tmq, topic_name, vgId, AdapterTMQCommittedCallback, param);
}

void taosa_tmq_position_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId, uintptr_t param) {
    adapter_tmq_position(thread_info, tmq, topic_name, vgId, AdapterTMQPositionCallback, param);
}
*/
import "C"
import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

// InitTMQThread tmq_thread *init_tmq_thread()
func InitTMQThread() unsafe.Pointer {
	return unsafe.Pointer(C.init_tmq_thread())
}

// DestroyTMQThread void destroy_tmq_thread(tmq_thread *thread_info)
func DestroyTMQThread(tmqThread unsafe.Pointer) {
	C.destroy_tmq_thread((*C.tmq_thread)(tmqThread))
}

// TaosaTMQPollA void taosa_tmq_poll_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, int64_t timeout, uintptr_t param)
func TaosaTMQPollA(tmqThread unsafe.Pointer, tmq unsafe.Pointer, timeout int64, caller cgo.Handle) {
	C.taosa_tmq_poll_a_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), (C.int64_t)(timeout), C.uintptr_t(caller))
}

// TaosaTMQFreeResultA taosa_tmq_free_result_a_wrapper(tmq_thread *thread_info, TAOS_RES *res, uintptr_t param)
func TaosaTMQFreeResultA(tmqThread unsafe.Pointer, res unsafe.Pointer, caller cgo.Handle) {
	C.taosa_tmq_free_result_a_wrapper((*C.tmq_thread)(tmqThread), res, C.uintptr_t(caller))
}

// TaosaTMQCommitA taosa_tmq_commit_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, TAOS_RES *msg, uintptr_t param)
func TaosaTMQCommitA(tmqThread unsafe.Pointer, tmq unsafe.Pointer, msg unsafe.Pointer, caller cgo.Handle) {
	C.taosa_tmq_commit_a_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), msg, C.uintptr_t(caller))
}

// TaosaTMQFetchRawBlockA taosa_tmq_fetch_raw_block_a_wrapper(tmq_thread *thread_info, TAOS_RES *res, adapter_tmq_fetch_raw_block_cb cb, uintptr_t para)
func TaosaTMQFetchRawBlockA(tmqThread unsafe.Pointer, res unsafe.Pointer, caller cgo.Handle) {
	C.taosa_tmq_fetch_raw_block_a_wrapper((*C.tmq_thread)(tmqThread), res, C.uintptr_t(caller))
}

// TaosaTMQNewConsumerA void taosa_tmq_new_consumer_a_wrapper(tmq_thread *thread_info, tmq_conf_t *conf, char *errstr, int32_t errstrLen,uintptr_t param)
func TaosaTMQNewConsumerA(tmqThread unsafe.Pointer, conf unsafe.Pointer, caller cgo.Handle) {
	p := (*C.char)(C.calloc(C.size_t(C.uint(1024)), C.size_t(C.uint(1024))))
	C.taosa_tmq_new_consumer_a_wrapper((*C.tmq_thread)(tmqThread), (*C.struct_tmq_conf_t)(conf), p, C.int32_t(1024), C.uintptr_t(caller))
}

// TaosaTMQSubscribeA taosa_tmq_subscribe_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, tmq_list_t *topic_list, uintptr_t param) {
func TaosaTMQSubscribeA(tmqThread unsafe.Pointer, tmq unsafe.Pointer, topicList unsafe.Pointer, caller cgo.Handle) {
	C.taosa_tmq_subscribe_a_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), (*C.tmq_list_t)(topicList), C.uintptr_t(caller))
}

// TaosaTMQUnsubscribeA void taosa_tmq_unsubscribe_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, uintptr_t param) {
func TaosaTMQUnsubscribeA(tmqThread unsafe.Pointer, tmq unsafe.Pointer, caller cgo.Handle) {
	C.taosa_tmq_unsubscribe_a_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), C.uintptr_t(caller))
}

// TaosaTMQConsumerCloseA taosa_tmq_consumer_close_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, uintptr_t param)
func TaosaTMQConsumerCloseA(tmqThread unsafe.Pointer, tmq unsafe.Pointer, caller cgo.Handle) {
	C.taosa_tmq_consumer_close_a_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), C.uintptr_t(caller))
}

func TaosaInitTMQRaw() unsafe.Pointer {
	return unsafe.Pointer(C.malloc(C.sizeof_struct_tmq_raw_data))
}

func TaosaFreeTMQRaw(raw unsafe.Pointer) {
	C.free(raw)
}

// TaosaTMQGetRawA taosa_tmq_get_raw_a_wrapper(tmq_thread *thread_info, TAOS_RES *res, tmq_raw_data *raw, uintptr_t param)
func TaosaTMQGetRawA(tmqThread unsafe.Pointer, res unsafe.Pointer, rawData unsafe.Pointer, caller cgo.Handle) {
	C.taosa_tmq_get_raw_a_wrapper((*C.tmq_thread)(tmqThread), res, (*C.tmq_raw_data)(rawData), C.uintptr_t(caller))
}

// TaosaTMQGetJsonMetaA void taosa_tmq_get_json_meta_a_wrapper(tmq_thread *thread_info, TAOS_RES *res, uintptr_t param) {
func TaosaTMQGetJsonMetaA(tmqThread unsafe.Pointer, res unsafe.Pointer, caller cgo.Handle) {
	C.taosa_tmq_get_json_meta_a_wrapper((*C.tmq_thread)(tmqThread), res, C.uintptr_t(caller))
}

// TaosaTMQGetTopicAssignmentA void taosa_tmq_get_topic_assignment_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, uintptr_t param) {
func TaosaTMQGetTopicAssignmentA(tmqThread, tmq unsafe.Pointer, topic string, caller cgo.Handle) {
	topicName := C.CString(topic)
	C.taosa_tmq_get_topic_assignment_a_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), topicName, C.uintptr_t(caller))
}

// TaosaTMQOffsetSeekA void taosa_tmq_offset_seek_a_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId, int64_t offset, uintptr_t param) {
func TaosaTMQOffsetSeekA(tmqThread, tmq unsafe.Pointer, topic string, vgID int32, offset int64, caller cgo.Handle) {
	topicName := C.CString(topic)
	C.taosa_tmq_offset_seek_a_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), topicName, C.int32_t(vgID), C.int64_t(offset), C.uintptr_t(caller))
}

// TaosaTMQCommitOffset void taosa_tmq_commit_offset_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId, int64_t offset, uintptr_t param)
func TaosaTMQCommitOffset(tmqThread unsafe.Pointer, tmq unsafe.Pointer, topic string, vgID int32, offset int64, caller cgo.Handle) {
	topicName := C.CString(topic)
	C.taosa_tmq_commit_offset_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), topicName, C.int32_t(vgID), C.int64_t(offset), C.uintptr_t(caller))
}

// TaosaTMQCommitted void taosa_tmq_committed_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId, uintptr_t param)
func TaosaTMQCommitted(tmqThread unsafe.Pointer, tmq unsafe.Pointer, topic string, vgID int32, caller cgo.Handle) {
	topicName := C.CString(topic)
	C.taosa_tmq_committed_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), topicName, C.int32_t(vgID), C.uintptr_t(caller))
}

// TaosaTMQPosition void taosa_tmq_position_wrapper(tmq_thread *thread_info, tmq_t *tmq, char *topic_name, int32_t vgId, uintptr_t param)
func TaosaTMQPosition(tmqThread unsafe.Pointer, tmq unsafe.Pointer, topic string, vgID int32, caller cgo.Handle) {
	topicName := C.CString(topic)
	C.taosa_tmq_position_wrapper((*C.tmq_thread)(tmqThread), (*C.tmq_t)(tmq), topicName, C.int32_t(vgID), C.uintptr_t(caller))
}
