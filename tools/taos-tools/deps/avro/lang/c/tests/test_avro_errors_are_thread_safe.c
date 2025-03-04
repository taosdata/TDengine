/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#ifdef _WIN32
#include <Windows.h>
#define THR_HANDLE HANDLE
#define LAST_ERROR() GetLastError()
#define SLEEP_MILLIS(millis) Sleep( millis )
#else
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#define THR_HANDLE pthread_t
#define LAST_ERROR() errno
#define SLEEP_MILLIS(millis) usleep( millis * 1000 )
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <avro.h>

enum { 
	NUMBER_OF_TEST_THREADS = 64,
	THREAD_ERROR_BUFFER_SIZE = 1024,
	MAX_THREAD_SLEEP_MILLIS = 3000,
};

typedef struct _TEST_THREAD_DATA
{
	int index;
	int error_occured;
	char error_message[THREAD_ERROR_BUFFER_SIZE];
	volatile int worker_thread;
	int sleep_interval_millis;
}TEST_THREAD_DATA;

static int get_random_value( int max_value );
#ifdef _WIN32
static DWORD worker_thread( LPVOID lpThreadParameter );
#else
static void *worker_thread( void *p );
#endif
static THR_HANDLE launch_thread( void *thread_context );
static int join_thread( THR_HANDLE thread_handle );

int main()
{
	TEST_THREAD_DATA threads_data[NUMBER_OF_TEST_THREADS];
	THR_HANDLE thread_handle[NUMBER_OF_TEST_THREADS];
	unsigned i;
	int found_error = 0;

	srand( (unsigned)time( NULL ) );  

	memset( threads_data, 0, sizeof(threads_data) );
	for ( i = 0; i < NUMBER_OF_TEST_THREADS; i++ )
	{
		threads_data[i].index = i;
		threads_data[i].sleep_interval_millis = get_random_value( MAX_THREAD_SLEEP_MILLIS );
		thread_handle[i] = launch_thread( &threads_data[i] );
		if ( !thread_handle[i] )
		{
			fprintf( stderr, "failed to launch worker thread, error code is %d\n", LAST_ERROR() );
			return EXIT_FAILURE;
		}
	}

	for ( i = 0; i < NUMBER_OF_TEST_THREADS; i++ )
		if ( join_thread( thread_handle[i] ) != 0 )
		{
			fprintf( stderr, "failed to join thread %d, error code is %d\n", i, LAST_ERROR() );
			return EXIT_FAILURE;
		}

	for ( i = 0; i < NUMBER_OF_TEST_THREADS; i++ )
	{
		if( threads_data[i].error_occured )
		{
			fprintf( stderr, "error occured at thread %d: %s\n", i, threads_data[i].error_message );
			found_error = 1;
		}
	}
	if ( found_error )
		return EXIT_FAILURE;

//	printf( "test ended successfully\n");
	return EXIT_SUCCESS;
}

#ifdef _WIN32
static DWORD worker_thread( LPVOID context )
#else
static void *worker_thread( void *context )
#endif
{
	/* 
	worker thread set an error, request the error stack and validate it contains the error saved.
	later it appends another error to the error stack, and validate it contains the two errors.
	*/
	TEST_THREAD_DATA *thread_context = (TEST_THREAD_DATA *)context;
	char first_error_buffer[1024] = "";
	char second_error_buffer[1024] = "";
	char full_error_buffer[1024] = "";
	const char *error_stack = NULL;
	int index = thread_context->index;
	unsigned sleep_interval_millis = thread_context->sleep_interval_millis;

	//set a thread specific error
	snprintf( first_error_buffer, sizeof(first_error_buffer), "thread %d set an error", index );
	avro_set_error( "%s", first_error_buffer );

	SLEEP_MILLIS( sleep_interval_millis );

	//validate error stack contains the thread specific error
	error_stack = avro_strerror();
	if ( strcmp( error_stack, first_error_buffer ) != 0 )
	{
		thread_context->error_occured = 1;
		snprintf( thread_context->error_message, 
				  sizeof(thread_context->error_message),
				  "invalid error stack found: expected '%s' found '%s'", first_error_buffer, error_stack );
	}

	//set another thread specific error
	SLEEP_MILLIS( sleep_interval_millis );
	snprintf( second_error_buffer, sizeof(second_error_buffer), "thread %d set ANOTHER error...", index );
	avro_prefix_error( "%s", second_error_buffer );
	snprintf( full_error_buffer, sizeof(full_error_buffer), "%s%s", second_error_buffer, first_error_buffer );

	//validate error stack contains the 2 errors as expected
	SLEEP_MILLIS( sleep_interval_millis );
	error_stack = avro_strerror();
	if ( strcmp( error_stack, full_error_buffer ) != 0 )
	{
		thread_context->error_occured = 1;
		snprintf( thread_context->error_message, 
				  sizeof(thread_context->error_message),
				  "invalid error stack found: expected '%s' found '%s'", full_error_buffer, error_stack );
	}

	return 0;

}

static THR_HANDLE launch_thread( void *thread_context )
{
#ifdef _WIN32
	static const LPSECURITY_ATTRIBUTES DEFAULT_SECURITY_ATTIRBUTES = NULL;
	static const SIZE_T DEFAULT_STACK_SIZE = 0;
	static const DWORD DEFAULT_CREATION_FLAGS = 0;
	DWORD thread_id = 0;

	return
		CreateThread( DEFAULT_SECURITY_ATTIRBUTES,
					  DEFAULT_STACK_SIZE,
					  worker_thread,
					  thread_context,
					  DEFAULT_CREATION_FLAGS,
					  &thread_id );
#else
    pthread_attr_t attr = {0};
	pthread_t thread;
    pthread_attr_init( &attr );
	int status = 0;
    status = pthread_create( &thread, &attr, worker_thread, thread_context );
    pthread_attr_destroy(&attr);
	if ( status != 0 )
		return NULL;
	return thread;
#endif
}

static int join_thread( THR_HANDLE thread_handle )
{
#ifdef _WIN32
	return
		( WaitForSingleObject( thread_handle, INFINITE ) == WAIT_OBJECT_0 ) ? 0 : -1;
#else
	return
		pthread_join( thread_handle, NULL );
#endif
}

static int get_random_value( int max_value )
{
	return 
		rand() % max_value;
}
