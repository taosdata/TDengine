/*******************************************************************************
 * Copyright (c) 2020 IBM Corp.
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
 *******************************************************************************/

#include "MQTTTime.h"
#include "StackTrace.h"

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#else
#include <unistd.h>
#include <sys/time.h>
#endif

void MQTTTime_sleep(long milliseconds)
{
	FUNC_ENTRY;
#if defined(_WIN32) || defined(_WIN64)
	Sleep(milliseconds);
#else
	usleep(milliseconds*1000);
#endif
	FUNC_EXIT;
}

#if defined(_WIN32) || defined(_WIN64)
START_TIME_TYPE MQTTTime_start_clock(void)
{
	return GetTickCount();
}
#elif defined(AIX)
START_TIME_TYPE MQTTTime_start_clock(void)
{
	static struct timespec start;
	clock_gettime(CLOCK_MONOTONIC, &start);
	return start;
}
#else
START_TIME_TYPE MQTTTime_start_clock(void)
{
	static struct timeval start;
	static struct timespec start_ts;

	clock_gettime(CLOCK_MONOTONIC, &start_ts);
	start.tv_sec = start_ts.tv_sec;
	start.tv_usec = start_ts.tv_nsec / 1000;
	return start;
}
#endif
START_TIME_TYPE MQTTTime_now(void)
{
	return MQTTTime_start_clock();
}


#if defined(_WIN32) || defined(_WIN64)
long MQTTTime_elapsed(DWORD milliseconds)
{
	return GetTickCount() - milliseconds;
}
#elif defined(AIX)
#define assert(a)
long MQTTTime_elapsed(struct timespec start)
{
	struct timespec now, res;

	clock_gettime(CLOCK_MONOTONIC, &now);
	ntimersub(now, start, res);
	return (res.tv_sec)*1000L + (res.tv_nsec)/1000000L;
}
#else
long MQTTTime_elapsed(struct timeval start)
{
	struct timeval now, res;
	static struct timespec now_ts;

	clock_gettime(CLOCK_MONOTONIC, &now_ts);
	now.tv_sec = now_ts.tv_sec;
	now.tv_usec = now_ts.tv_nsec / 1000;
	timersub(&now, &start, &res);
	return (res.tv_sec)*1000 + (res.tv_usec)/1000;
}
#endif

#if defined(_WIN32) || defined(_WIN64)
/*
 * @param new most recent time in milliseconds from GetTickCount()
 * @param old older time in milliseconds from GetTickCount()
 * @return difference in milliseconds
 */
long MQTTTime_difftime(DWORD new, DWORD old)
{
	return new - old;
}
#elif defined(AIX)
#define assert(a)
long MQTTTime_difftime(struct timespec new, struct timespec old)
{
	struct timespec result;

	ntimersub(new, old, result);
	return (result.tv_sec)*1000L + (result.tv_nsec)/1000000L; /* convert to milliseconds */
}
#else
long MQTTTime_difftime(struct timeval new, struct timeval old)
{
	struct timeval result;

	timersub(&new, &old, &result);
	return (result.tv_sec)*1000 + (result.tv_usec)/1000; /* convert to milliseconds */
}
#endif
