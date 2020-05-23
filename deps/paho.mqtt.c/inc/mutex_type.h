/*******************************************************************************
 * Copyright (c) 2009, 2018 IBM Corp.
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
 *******************************************************************************/
#if !defined(_MUTEX_TYPE_H_)
#define _MUTEX_TYPE_H_

#if defined(_WIN32) || defined(_WIN64)
	#include <windows.h>
	#define mutex_type HANDLE
#else
	#include <pthread.h>
	#define mutex_type pthread_mutex_t*
#endif

#endif /* _MUTEX_TYPE_H_ */
