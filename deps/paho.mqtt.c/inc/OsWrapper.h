/*******************************************************************************
 * Copyright (c) 2016, 2017 logi.cals GmbH
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
 *    Gunter Raidl - timer support for VxWorks
 *    Rainer Poisel - reusability
 *******************************************************************************/

#if !defined(OSWRAPPER_H)
#define OSWRAPPER_H

#if defined(_WRS_KERNEL)
#include <time.h>

#define lstat stat

typedef unsigned long useconds_t;
void usleep(useconds_t useconds);

#define timersub(a, b, result) \
	do \
	{ \
		(result)->tv_sec = (a)->tv_sec - (b)->tv_sec; \
		(result)->tv_usec = (a)->tv_usec - (b)->tv_usec; \
		if ((result)->tv_usec < 0) \
		{ \
			--(result)->tv_sec; \
			(result)->tv_usec += 1000000L; \
		} \
	} while (0)
#endif /* defined(_WRS_KERNEL) */

#endif /* OSWRAPPER_H */
