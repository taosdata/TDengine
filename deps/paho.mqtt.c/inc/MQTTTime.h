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

#if !defined(MQTTTIME_H)
#define MQTTTIME_H

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#define START_TIME_TYPE DWORD
#define START_TIME_ZERO 0
#elif defined(AIX)
#define START_TIME_TYPE struct timespec
#define START_TIME_ZERO {0, 0}
#else
#include <sys/time.h>
#define START_TIME_TYPE struct timeval
#define START_TIME_ZERO {0, 0}
#endif

void MQTTTime_sleep(long milliseconds);
START_TIME_TYPE MQTTTime_start_clock(void);
START_TIME_TYPE MQTTTime_now(void);
long MQTTTime_elapsed(START_TIME_TYPE milliseconds);
long MQTTTime_difftime(START_TIME_TYPE new, START_TIME_TYPE old);

#endif
