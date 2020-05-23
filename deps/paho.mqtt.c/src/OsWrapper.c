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

#include "OsWrapper.h"

#if defined(_WRS_KERNEL)
void usleep(useconds_t useconds)
{
	struct timespec tv;
	tv.tv_sec = useconds / 1000000;
	tv.tv_nsec = (useconds % 1000000) * 1000;
	nanosleep(&tv, NULL);
}
#endif /* defined(_WRS_KERNEL) */
