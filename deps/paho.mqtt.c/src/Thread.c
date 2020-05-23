/*******************************************************************************
 * Copyright (c) 2009, 2020 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial implementation
 *    Ian Craggs, Allan Stockdill-Mander - async client updates
 *    Ian Craggs - bug #415042 - start Linux thread as disconnected
 *    Ian Craggs - fix for bug #420851
 *    Ian Craggs - change MacOS semaphore implementation
 *    Ian Craggs - fix for clock #284
 *******************************************************************************/

/**
 * @file
 * \brief Threading related functions
 *
 * Used to create platform independent threading functions
 */


#include "Thread.h"
#if defined(THREAD_UNIT_TESTS)
#define NOSTACKTRACE
#endif
#include "Log.h"
#include "StackTrace.h"

#undef malloc
#undef realloc
#undef free

#if !defined(_WIN32) && !defined(_WIN64)
#include <errno.h>
#include <unistd.h>
#include <sys/time.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <limits.h>
#endif
#include <stdlib.h>

#include "OsWrapper.h"

/**
 * Start a new thread
 * @param fn the function to run, must be of the correct signature
 * @param parameter pointer to the function parameter, can be NULL
 * @return the new thread
 */
thread_type Thread_start(thread_fn fn, void* parameter)
{
#if defined(_WIN32) || defined(_WIN64)
	thread_type thread = NULL;
#else
	thread_type thread = 0;
	pthread_attr_t attr;
#endif

	FUNC_ENTRY;
#if defined(_WIN32) || defined(_WIN64)
	thread = CreateThread(NULL, 0, fn, parameter, 0, NULL);
#else
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	if (pthread_create(&thread, &attr, fn, parameter) != 0)
		thread = 0;
	pthread_attr_destroy(&attr);
#endif
	FUNC_EXIT;
	return thread;
}


/**
 * Create a new mutex
 * @return the new mutex
 */
mutex_type Thread_create_mutex(int* rc)
{
	mutex_type mutex = NULL;

	FUNC_ENTRY;
	*rc = -1;
	#if defined(_WIN32) || defined(_WIN64)
		mutex = CreateMutex(NULL, 0, NULL);
		if (mutex == NULL)
			*rc = GetLastError();
	#else
		mutex = malloc(sizeof(pthread_mutex_t));
		if (mutex)
			*rc = pthread_mutex_init(mutex, NULL);
	#endif
	FUNC_EXIT_RC(*rc);
	return mutex;
}


/**
 * Lock a mutex which has alrea
 * @return completion code, 0 is success
 */
int Thread_lock_mutex(mutex_type mutex)
{
	int rc = -1;

	/* don't add entry/exit trace points as the stack log uses mutexes - recursion beckons */
	#if defined(_WIN32) || defined(_WIN64)
		/* WaitForSingleObject returns WAIT_OBJECT_0 (0), on success */
		rc = WaitForSingleObject(mutex, INFINITE);
	#else
		rc = pthread_mutex_lock(mutex);
	#endif

	return rc;
}


/**
 * Unlock a mutex which has already been locked
 * @param mutex the mutex
 * @return completion code, 0 is success
 */
int Thread_unlock_mutex(mutex_type mutex)
{
	int rc = -1;

	/* don't add entry/exit trace points as the stack log uses mutexes - recursion beckons */
	#if defined(_WIN32) || defined(_WIN64)
		/* if ReleaseMutex fails, the return value is 0 */
		if (ReleaseMutex(mutex) == 0)
			rc = GetLastError();
		else
			rc = 0;
	#else
		rc = pthread_mutex_unlock(mutex);
	#endif

	return rc;
}


/**
 * Destroy a mutex which has already been created
 * @param mutex the mutex
 */
int Thread_destroy_mutex(mutex_type mutex)
{
	int rc = 0;

	FUNC_ENTRY;
	#if defined(_WIN32) || defined(_WIN64)
		rc = CloseHandle(mutex);
	#else
		rc = pthread_mutex_destroy(mutex);
		free(mutex);
	#endif
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Get the thread id of the thread from which this function is called
 * @return thread id, type varying according to OS
 */
thread_id_type Thread_getid(void)
{
	#if defined(_WIN32) || defined(_WIN64)
		return GetCurrentThreadId();
	#else
		return pthread_self();
	#endif
}


/**
 * Create a new semaphore
 * @return the new condition variable
 */
sem_type Thread_create_sem(int *rc)
{
	sem_type sem = NULL;

	FUNC_ENTRY;
	*rc = -1;
	#if defined(_WIN32) || defined(_WIN64)
		sem = CreateEvent(
		        NULL,               /* default security attributes */
		        FALSE,              /* manual-reset event? */
		        FALSE,              /* initial state is nonsignaled */
		        NULL                /* object name */
		        );
#if 0
		sem = CreateSemaphore(
				NULL,				/* default security attributes */
				0,       	        /* initial count - non signaled */
				1, 					/* maximum count */
				NULL 				/* unnamed semaphore */
		);
#endif
	#elif defined(OSX)
		sem = dispatch_semaphore_create(0L);
		*rc = (sem == NULL) ? -1 : 0;
	#else
		sem = malloc(sizeof(sem_t));
		if (sem)
			*rc = sem_init(sem, 0, 0);
	#endif
	FUNC_EXIT_RC(*rc);
	return sem;
}


/**
 * Wait for a semaphore to be posted, or timeout.
 * @param sem the semaphore
 * @param timeout the maximum time to wait, in milliseconds
 * @return completion code
 */
int Thread_wait_sem(sem_type sem, int timeout)
{
/* sem_timedwait is the obvious call to use, but seemed not to work on the Viper,
 * so I've used trywait in a loop instead. Ian Craggs 23/7/2010
 */
	int rc = -1;
#if !defined(_WIN32) && !defined(_WIN64) && !defined(OSX)
#define USE_TRYWAIT
#if defined(USE_TRYWAIT)
	int i = 0;
	useconds_t interval = 10000; /* 10000 microseconds: 10 milliseconds */
	int count = (1000 * timeout) / interval; /* how many intervals in timeout period */
#else
	struct timespec ts;
#endif
#endif

	FUNC_ENTRY;
	#if defined(_WIN32) || defined(_WIN64)
		/* returns 0 (WAIT_OBJECT_0) on success, non-zero (WAIT_TIMEOUT) if timeout occurred */
		rc = WaitForSingleObject(sem, timeout < 0 ? 0 : timeout);
		if (rc == WAIT_TIMEOUT)
			rc = ETIMEDOUT;
	#elif defined(OSX)
		/* returns 0 on success, non-zero if timeout occurred */
		rc = (int)dispatch_semaphore_wait(sem, dispatch_time(DISPATCH_TIME_NOW, (int64_t)timeout*1000000L));
		if (rc != 0)
			rc = ETIMEDOUT;
	#elif defined(USE_TRYWAIT)
		while (++i < count && (rc = sem_trywait(sem)) != 0)
		{
			if (rc == -1 && ((rc = errno) != EAGAIN))
			{
				rc = 0;
				break;
			}
			usleep(interval); /* microseconds - .1 of a second */
		}
	#else
		/* We have to use CLOCK_REALTIME rather than MONOTONIC for sem_timedwait interval.
		 * Does this make it susceptible to system clock changes?
		 * The intervals are small enough, and repeated, that I think it's not an issue.
		 */
		if (clock_gettime(CLOCK_REALTIME, &ts) != -1)
		{
			ts.tv_sec += timeout;
			rc = sem_timedwait(sem, &ts);
		}
	#endif

 	FUNC_EXIT_RC(rc);
 	return rc;
}


/**
 * Check to see if a semaphore has been posted, without waiting
 * The semaphore will be unchanged, if the return value is false.
 * The semaphore will have been decremented, if the return value is true.
 * @param sem the semaphore
 * @return 0 (false) or 1 (true)
 */
int Thread_check_sem(sem_type sem)
{
#if defined(_WIN32) || defined(_WIN64)
	/* if the return value is not 0, the semaphore will not have been decremented */
	return WaitForSingleObject(sem, 0) == WAIT_OBJECT_0;
#elif defined(OSX)
	/* if the return value is not 0, the semaphore will not have been decremented */
	return dispatch_semaphore_wait(sem, DISPATCH_TIME_NOW) == 0;
#else
	/* If the call was unsuccessful, the state of the semaphore shall be unchanged,
	 * and the function shall return a value of -1 */
	return sem_trywait(sem) == 0;
#endif
}


/**
 * Post a semaphore
 * @param sem the semaphore
 * @return 0 on success
 */
int Thread_post_sem(sem_type sem)
{
	int rc = 0;

	FUNC_ENTRY;
	#if defined(_WIN32) || defined(_WIN64)
		if (SetEvent(sem) == 0)
			rc = GetLastError();
	#elif defined(OSX)
		rc = (int)dispatch_semaphore_signal(sem);
	#else
		int val;
		int rc1 = sem_getvalue(sem, &val);
		if (rc1 != 0)
			rc = errno;
		else if (val == 0 && sem_post(sem) == -1)
			rc = errno;
	#endif

 	FUNC_EXIT_RC(rc);
 	return rc;
}


/**
 * Destroy a semaphore which has already been created
 * @param sem the semaphore
 */
int Thread_destroy_sem(sem_type sem)
{
	int rc = 0;

	FUNC_ENTRY;
	#if defined(_WIN32) || defined(_WIN64)
		rc = CloseHandle(sem);
	#elif defined(OSX)
	  dispatch_release(sem);
	#else
		rc = sem_destroy(sem);
		free(sem);
	#endif
	FUNC_EXIT_RC(rc);
	return rc;
}


#if !defined(_WIN32) && !defined(_WIN64)

/**
 * Create a new condition variable
 * @return the condition variable struct
 */
cond_type Thread_create_cond(int *rc)
{
	cond_type condvar = NULL;
	pthread_condattr_t attr;

	FUNC_ENTRY;
	*rc = -1;
	pthread_condattr_init(&attr);

#if 0
    /* in theory, a monotonic clock should be able to be used.  However on at least
     * one system reported, even though setclock() reported success, it didn't work.
     */
	if ((rc = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC)) == 0)
		use_clock_monotonic = 1;
	else
		Log(LOG_ERROR, -1, "Error %d calling pthread_condattr_setclock(CLOCK_MONOTONIC)", rc);
#endif

	condvar = malloc(sizeof(cond_type_struct));
	if (condvar)
	{
		*rc = pthread_cond_init(&condvar->cond, &attr);
		*rc = pthread_mutex_init(&condvar->mutex, NULL);
	}

	FUNC_EXIT_RC(*rc);
	return condvar;
}

/**
 * Signal a condition variable
 * @return completion code
 */
int Thread_signal_cond(cond_type condvar)
{
	int rc = 0;

	FUNC_ENTRY;
	pthread_mutex_lock(&condvar->mutex);
	rc = pthread_cond_signal(&condvar->cond);
	pthread_mutex_unlock(&condvar->mutex);

	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * Wait with a timeout (seconds) for condition variable
 * @return 0 for success, ETIMEDOUT otherwise
 */
int Thread_wait_cond(cond_type condvar, int timeout)
{
	int rc = 0;
	struct timespec cond_timeout;

	FUNC_ENTRY;
	clock_gettime(CLOCK_REALTIME, &cond_timeout);

	cond_timeout.tv_sec += timeout;
	pthread_mutex_lock(&condvar->mutex);
	rc = pthread_cond_timedwait(&condvar->cond, &condvar->mutex, &cond_timeout);
	pthread_mutex_unlock(&condvar->mutex);

	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * Destroy a condition variable
 * @return completion code
 */
int Thread_destroy_cond(cond_type condvar)
{
	int rc = 0;

	rc = pthread_mutex_destroy(&condvar->mutex);
	rc = pthread_cond_destroy(&condvar->cond);
	free(condvar);

	return rc;
}
#endif


#if defined(THREAD_UNIT_TESTS)

#if defined(_WIN32) || defined(_WINDOWS)
#define mqsleep(A) Sleep(1000*A)
#define START_TIME_TYPE DWORD
static DWORD start_time = 0;
START_TIME_TYPE start_clock(void)
{
	return GetTickCount();
}
#elif defined(AIX)
#define mqsleep sleep
#define START_TIME_TYPE struct timespec
START_TIME_TYPE start_clock(void)
{
	static struct timespec start;
	clock_gettime(CLOCK_REALTIME, &start);
	return start;
}
#else
#define mqsleep sleep
#define START_TIME_TYPE struct timeval
/* TODO - unused - remove? static struct timeval start_time; */
START_TIME_TYPE start_clock(void)
{
	struct timeval start_time;
	gettimeofday(&start_time, NULL);
	return start_time;
}
#endif


#if defined(_WIN32)
long elapsed(START_TIME_TYPE start_time)
{
	return GetTickCount() - start_time;
}
#elif defined(AIX)
#define assert(a)
long elapsed(struct timespec start)
{
	struct timespec now, res;

	clock_gettime(CLOCK_REALTIME, &now);
	ntimersub(now, start, res);
	return (res.tv_sec)*1000L + (res.tv_nsec)/1000000L;
}
#else
long elapsed(START_TIME_TYPE start_time)
{
	struct timeval now, res;

	gettimeofday(&now, NULL);
	timersub(&now, &start_time, &res);
	return (res.tv_sec)*1000 + (res.tv_usec)/1000;
}
#endif


int tests = 0, failures = 0;

void myassert(char* filename, int lineno, char* description, int value, char* format, ...)
{
	++tests;
	if (!value)
	{
		va_list args;

		++failures;
		printf("Assertion failed, file %s, line %d, description: %s\n", filename, lineno, description);

		va_start(args, format);
		vprintf(format, args);
		va_end(args);

		//cur_output += sprintf(cur_output, "<failure type=\"%s\">file %s, line %d </failure>\n",
        //                description, filename, lineno);
	}
    else
    		printf("Assertion succeeded, file %s, line %d, description: %s\n", filename, lineno, description);
}

#define assert(a, b, c, d) myassert(__FILE__, __LINE__, a, b, c, d)
#define assert1(a, b, c, d, e) myassert(__FILE__, __LINE__, a, b, c, d, e)

#include <stdio.h>

thread_return_type cond_secondary(void* n)
{
	int rc = 0;
	cond_type cond = n;

	printf("This should return immediately as it was posted already\n");
	rc = Thread_wait_cond(cond, 99999);
	assert("rc 1 from wait_cond", rc == 1, "rc was %d", rc);

	printf("This should hang around a few seconds\n");
	rc = Thread_wait_cond(cond, 99999);
	assert("rc 1 from wait_cond", rc == 1, "rc was %d", rc);

	printf("Secondary cond thread ending\n");
	return 0;
}


int cond_test()
{
	int rc = 0;
	cond_type cond = Thread_create_cond();
	thread_type thread;

	printf("Post secondary so it should return immediately\n");
	rc = Thread_signal_cond(cond);
	assert("rc 0 from signal cond", rc == 0, "rc was %d", rc);

	printf("Starting secondary thread\n");
	thread = Thread_start(cond_secondary, (void*)cond);

	sleep(3);

	printf("post secondary\n");
	rc = Thread_signal_cond(cond);
	assert("rc 1 from signal cond", rc == 1, "rc was %d", rc);

	sleep(3);

	printf("Main thread ending\n");

	return failures;
}


thread_return_type sem_secondary(void* n)
{
	int rc = 0;
	sem_type sem = n;

	printf("Secondary semaphore pointer %p\n", sem);

	rc = Thread_check_sem(sem);
	assert("rc 1 from check_sem", rc == 1, "rc was %d", rc);

	printf("Secondary thread about to wait\n");
	rc = Thread_wait_sem(sem, 99999);
	printf("Secondary thread returned from wait %d\n", rc);

	printf("Secondary thread about to wait\n");
	rc = Thread_wait_sem(sem, 99999);
	printf("Secondary thread returned from wait %d\n", rc);
	printf("Secondary check sem %d\n", Thread_check_sem(sem));

	printf("Secondary thread ending\n");
	return 0;
}


int sem_test()
{
	int rc = 0;
	sem_type sem = Thread_create_sem();
	thread_type thread;

	printf("Primary semaphore pointer %p\n", sem);

	rc = Thread_check_sem(sem);
	assert("rc 0 from check_sem", rc == 0, "rc was %d\n", rc);

	printf("post secondary so then check should be 1\n");
	rc = Thread_post_sem(sem);
	assert("rc 0 from post_sem", rc == 0, "rc was %d\n", rc);

	rc = Thread_check_sem(sem);
	assert("rc 1 from check_sem", rc == 1, "rc was %d", rc);

	printf("Starting secondary thread\n");
	thread = Thread_start(sem_secondary, (void*)sem);

	sleep(3);
	rc = Thread_check_sem(sem);
	assert("rc 1 from check_sem", rc == 1, "rc was %d", rc);

	printf("post secondary\n");
	rc = Thread_post_sem(sem);
	assert("rc 1 from post_sem", rc == 1, "rc was %d", rc);

	sleep(3);

	printf("Main thread ending\n");

	return failures;
}


int main(int argc, char *argv[])
{
	sem_test();
	//cond_test();
}

#endif
