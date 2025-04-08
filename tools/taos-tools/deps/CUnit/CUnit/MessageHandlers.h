/*
 * Headers for CUnit message/event handling types and functions
 *
 */

#ifndef CU_MESSAGEHANDLERS_H
#define CU_MESSAGEHANDLERS_H

#include "CUnit/TestRun.h"

/**
 * Types of MessageHandlers.
 */
typedef enum CU_MessageType
{
    CUMSG_SUITE_STARTED = 0,     /**< suite has started */
    CUMSG_SUITE_COMPLETED,       /**< suite has finished */
    CUMSG_SUITE_SETUP_FAILED,    /**< suite init func failed */
    CUMSG_SUITE_TEARDOWN_FAILED, /**< suite cleanup failed */
    CUMSG_TEST_STARTED,          /**< test has started */
    CUMSG_TEST_COMPLETED,        /**< test has finished */
    CUMSG_TEST_SKIPPED,          /**< test was skipped */
    CUMSG_TEST_SETUP_FAILED,     /**< test setup has failed */
    CUMSG_TEST_TEARDOWN_FAILED,  /**< test teardown has failed */
    CUMSG_ALL_COMPLETED,         /**< all suites finished */
    CUMSG_SUITE_SKIPPED,         /**< test suite was skipped during suite setup */
    CUMSG_MAX
} CCU_MessageType;


typedef union CU_MessageHandlerFunction {
    CU_SuiteStartMessageHandler          suite_start;
    CU_SuiteCompleteMessageHandler       suite_completed;
    CU_SuiteInitFailureMessageHandler    suite_setup_failed;
    CU_SuiteCleanupFailureMessageHandler suite_teardown_failed;
    CU_TestStartMessageHandler           test_started;
    CU_TestCompleteMessageHandler        test_completed;
    CU_TestSkippedMessageHandler         test_skipped;
    CU_AllTestsCompleteMessageHandler    all_completed;
    CU_SuiteSkippedMessageHandler        suite_skipped;
} CCU_MessageHandlerFunction;

/**
 * The handler for a CUnit Event.
 */
typedef struct CCU_MessageHandler {
    CCU_MessageHandlerFunction func;
    struct CCU_MessageHandler *prev;
    struct CCU_MessageHandler *next;
} CCU_MessageHandler;


/**
 * Add a message handler to the test runner.
 * @param type
 * @param handler
 */
void CCU_MessageHandler_Add(CCU_MessageType type, CCU_MessageHandlerFunction func);

/**
 * Clear all the message handlers for the given event type.
 * @param type
 */
void CCU_MessageHandler_Clear(CCU_MessageType type);

/**
 * Remove all pre-existing message handlers and set one.
 * @param type
 * @param handler
 */
void CCU_MessageHandler_Set(CCU_MessageType type, CCU_MessageHandlerFunction func);

/**
 * Run a message handler
 * @param type
 * @param pSuite
 * @param pTest
 * @param pFailure
 */
void CCU_MessageHandler_Run(CCU_MessageType type,
                            CU_pSuite pSuite,
                            CU_pTest pTest,
                            CU_pFailureRecord pFailure);

/**
 * Get the message handler list for the given message type
 * @param type
 * @return
 */
CCU_MessageHandler* CCU_MessageHandler_Get(CCU_MessageType type);


#endif //CU_MESSAGEHANDLERS_H
