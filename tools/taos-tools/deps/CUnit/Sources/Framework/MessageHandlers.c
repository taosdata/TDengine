/*
 * Message handler internal functions
 *
 * These are allocated statically so that abnormal termination of a test does not show
 * spurious memory leaks of intenral CUnit data.
 *
 */
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "CUnit/MessageHandlers.h"

#define MAX_HANDLERS 128

typedef struct _handler_container {
    int used;
    CCU_MessageHandler h;
} _handler_container;

/* handlers that can be dished out */
static _handler_container handler_pool[MAX_HANDLERS];

/* handlsers in use */
static CCU_MessageHandler* handlers[CUMSG_MAX];


static CCU_MessageHandler* alloc_MessageHandler(void) {
    CCU_MessageHandler *new = NULL;
    int i;
    for (i = 0; i < MAX_HANDLERS; i++) {
        if (!handler_pool[i].used) {
            handler_pool[i].used = 1;
            new = &(handler_pool[i].h);
            break;
        }
    }
    assert(new && "CUnit: Exceeded max active message handlers!");
    memset(new, 0, sizeof(*new));
    return new;
}

static void _free_MessageHandler(CCU_MessageHandler* ptr)
{
    int i;
    if (ptr) {
        if (ptr->prev) {
            ptr->prev->next = NULL;
        }
        _free_MessageHandler(ptr->next);
        /* release the handler back to the pool */

        for (i = 0; i < MAX_HANDLERS; i++) {
            if (ptr == &handler_pool[i].h) {
                handler_pool[i].used = 0;
                break;
            }
        }
    }
}

void CCU_MessageHandler_Add(CCU_MessageType type, CCU_MessageHandlerFunction func) {
    CCU_MessageHandler *tail;
    CCU_MessageHandler *new;

    new = alloc_MessageHandler();
    new->func = func;

    if (!handlers[type]) {
        // no list at all, add first entry
        handlers[type] = new;
    } else {
        // append to list for this message
        tail = handlers[type];
        while (tail->next) {
            tail = tail->next;
        }
        tail->next = new;
        new->prev = tail;
    }
}

void CCU_MessageHandler_Clear(CCU_MessageType type)
{
    CCU_MessageHandler *head = handlers[type];
    _free_MessageHandler(head);
    handlers[type] = NULL;
}

void CCU_MessageHandler_Set(CCU_MessageType type, CCU_MessageHandlerFunction func)
{
    CCU_MessageHandler_Clear(type);
    if (func.suite_start) {
        CCU_MessageHandler_Add(type, func);
    }
}

static void _run_MessageHandler(CCU_MessageHandlerFunction func,
                                CCU_MessageType type,
                                CU_pSuite pSuite,
                                CU_pTest pTest,
                                CU_pFailureRecord pFailure)
{
    if (!func.suite_start) return;

    switch (type) {
        case CUMSG_SUITE_STARTED:
            func.suite_start(pSuite);
            break;
        case CUMSG_SUITE_TEARDOWN_FAILED:
            func.suite_teardown_failed(pSuite);
            break;
        case CUMSG_SUITE_SETUP_FAILED:
            func.suite_setup_failed(pSuite);
            break;
        case CUMSG_SUITE_COMPLETED:
            func.suite_completed(pSuite, pFailure);
            break;
        case CUMSG_TEST_STARTED:
            func.test_started(pTest, pSuite);
            break;
        case CUMSG_TEST_SKIPPED:
            func.test_skipped(pTest, pSuite);
            break;
        case CUMSG_TEST_COMPLETED:
            func.test_completed(pTest, pSuite, pFailure);
            break;
        case CUMSG_ALL_COMPLETED:
            func.all_completed(pFailure);
            break;
        case CUMSG_SUITE_SKIPPED:
            func.suite_skipped(pSuite);
            break;
        default:
            break;
    }
}

void CCU_MessageHandler_Run(CCU_MessageType type,
                            CU_pSuite pSuite,
                            CU_pTest pTest,
                            CU_pFailureRecord pFailure)
{
    CCU_MessageHandler *handler = handlers[type];
    while (handler) {
        _run_MessageHandler(handler->func, type, pSuite, pTest, pFailure);
        handler = handler->next;
    }
}

CCU_MessageHandler* CCU_MessageHandler_Get(CCU_MessageType type)
{
    CCU_MessageHandler *handler = handlers[type];
    return handler;
}
