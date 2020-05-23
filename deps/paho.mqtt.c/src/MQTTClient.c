/*******************************************************************************
 * Copyright (c) 2009, 2020 IBM Corp. and others
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
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *    Ian Craggs - bug 384016 - segv setting will message
 *    Ian Craggs - bug 384053 - v1.0.0.7 - stop MQTTClient_receive on socket error
 *    Ian Craggs, Allan Stockdill-Mander - add ability to connect with SSL
 *    Ian Craggs - multiple server connection support
 *    Ian Craggs - fix for bug 413429 - connectionLost not called
 *    Ian Craggs - fix for bug 421103 - trying to write to same socket, in publish/retries
 *    Ian Craggs - fix for bug 419233 - mutexes not reporting errors
 *    Ian Craggs - fix for bug 420851
 *    Ian Craggs - fix for bug 432903 - queue persistence
 *    Ian Craggs - MQTT 3.1.1 support
 *    Ian Craggs - fix for bug 438176 - MQTT version selection
 *    Rong Xiang, Ian Craggs - C++ compatibility
 *    Ian Craggs - fix for bug 443724 - stack corruption
 *    Ian Craggs - fix for bug 447672 - simultaneous access to socket structure
 *    Ian Craggs - fix for bug 459791 - deadlock in WaitForCompletion for bad client
 *    Ian Craggs - fix for bug 474905 - insufficient synchronization for subscribe, unsubscribe, connect
 *    Ian Craggs - make it clear that yield and receive are not intended for multi-threaded mode (bug 474748)
 *    Ian Craggs - SNI support, message queue unpersist bug
 *    Ian Craggs - binary will message support
 *    Ian Craggs - waitforCompletion fix #240
 *    Ian Craggs - check for NULL SSL options #334
 *    Ian Craggs - allocate username/password buffers #431
 *    Ian Craggs - MQTT 5.0 support
 *******************************************************************************/

/**
 * @file
 * \brief Synchronous API implementation
 *
 */

#include <stdlib.h>
#include <string.h>
#if !defined(_WIN32) && !defined(_WIN64)
	#include <sys/time.h>
#endif

#include "MQTTClient.h"
#if !defined(NO_PERSISTENCE)
#include "MQTTPersistence.h"
#endif

#include "utf-8.h"
#include "MQTTProtocol.h"
#include "MQTTProtocolOut.h"
#include "Thread.h"
#include "SocketBuffer.h"
#include "StackTrace.h"
#include "Heap.h"

#if defined(OPENSSL)
#include <openssl/ssl.h>
#else
#define URI_SSL "ssl://"
#endif

#include "OsWrapper.h"

#define URI_TCP "tcp://"
#define URI_WS "ws://"
#define URI_WSS "wss://"

#include "VersionInfo.h"
#include "WebSocket.h"

const char *client_timestamp_eye = "MQTTClientV3_Timestamp " BUILD_TIMESTAMP;
const char *client_version_eye = "MQTTClientV3_Version " CLIENT_VERSION;

int MQTTClient_init(void);

void MQTTClient_global_init(MQTTClient_init_options* inits)
{
	MQTTClient_init();
#if defined(OPENSSL)
	SSLSocket_handleOpensslInit(inits->do_openssl_init);
#endif
}

static ClientStates ClientState =
{
	CLIENT_VERSION, /* version */
	NULL /* client list */
};

ClientStates* bstate = &ClientState;

MQTTProtocol state;

#if defined(_WIN32) || defined(_WIN64)
static mutex_type mqttclient_mutex = NULL;
static mutex_type socket_mutex = NULL;
static mutex_type subscribe_mutex = NULL;
static mutex_type unsubscribe_mutex = NULL;
static mutex_type connect_mutex = NULL;
#if !defined(NO_HEAP_TRACKING)
extern mutex_type stack_mutex;
extern mutex_type heap_mutex;
#endif
extern mutex_type log_mutex;

int MQTTClient_init(void)
{
	DWORD rc = 0;

	if (mqttclient_mutex == NULL)
	{
		if ((mqttclient_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("mqttclient_mutex error %d\n", rc);
			goto exit;
		}
		if ((subscribe_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("subscribe_mutex error %d\n", rc);
			goto exit;
		}
		if ((unsubscribe_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("unsubscribe_mutex error %d\n", rc);
			goto exit;
		}
		if ((connect_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("connect_mutex error %d\n", rc);
			goto exit;
		}
#if !defined(NO_HEAP_TRACKING)
		if ((stack_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("stack_mutex error %d\n", rc);
			goto exit;
		}
		if ((heap_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("heap_mutex error %d\n", rc);
			goto exit;
		}
#endif
		if ((log_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("log_mutex error %d\n", rc);
			goto exit;
		}
		if ((socket_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("socket_mutex error %d\n", rc);
			goto exit;
		}
	}
exit:
	return rc;
}

void MQTTClient_cleanup(void)
{
	if (connect_mutex)
		CloseHandle(connect_mutex);
	if (subscribe_mutex)
		CloseHandle(subscribe_mutex);
	if (unsubscribe_mutex)
		CloseHandle(unsubscribe_mutex);
#if !defined(NO_HEAP_TRACKING)
	if (stack_mutex)
		CloseHandle(stack_mutex);
	if (heap_mutex)
		CloseHandle(heap_mutex);
#endif
	if (log_mutex)
		CloseHandle(log_mutex);
	if (socket_mutex)
		CloseHandle(socket_mutex);
	if (mqttclient_mutex)
		CloseHandle(mqttclient_mutex);
}

#if defined(PAHO_MQTT_STATIC)
/* Global variable for one-time initialization structure */
static INIT_ONCE g_InitOnce = INIT_ONCE_STATIC_INIT; /* Static initialization */

/* One time initialization function */
BOOL CALLBACK InitOnceFunction (
    PINIT_ONCE InitOnce,        /* Pointer to one-time initialization structure */
    PVOID Parameter,            /* Optional parameter passed by InitOnceExecuteOnce */
    PVOID *lpContext)           /* Receives pointer to event object */
{
	int rc = MQTTClient_init();
    return rc == 0;
}

#else
BOOL APIENTRY DllMain(HANDLE hModule,
					  DWORD  ul_reason_for_call,
					  LPVOID lpReserved)
{
	switch (ul_reason_for_call)
	{
		case DLL_PROCESS_ATTACH:
			Log(TRACE_MAX, -1, "DLL process attach");
			MQTTClient_init();
			break;
		case DLL_THREAD_ATTACH:
			Log(TRACE_MAX, -1, "DLL thread attach");
			break;
		case DLL_THREAD_DETACH:
			Log(TRACE_MAX, -1, "DLL thread detach");
			break;
		case DLL_PROCESS_DETACH:
			Log(TRACE_MAX, -1, "DLL process detach");
			if (lpReserved)
				MQTTClient_cleanup();
			break;
	}
	return TRUE;
}
#endif

#else
static pthread_mutex_t mqttclient_mutex_store = PTHREAD_MUTEX_INITIALIZER;
static mutex_type mqttclient_mutex = &mqttclient_mutex_store;

static pthread_mutex_t socket_mutex_store = PTHREAD_MUTEX_INITIALIZER;
static mutex_type socket_mutex = &socket_mutex_store;

static pthread_mutex_t subscribe_mutex_store = PTHREAD_MUTEX_INITIALIZER;
static mutex_type subscribe_mutex = &subscribe_mutex_store;

static pthread_mutex_t unsubscribe_mutex_store = PTHREAD_MUTEX_INITIALIZER;
static mutex_type unsubscribe_mutex = &unsubscribe_mutex_store;

static pthread_mutex_t connect_mutex_store = PTHREAD_MUTEX_INITIALIZER;
static mutex_type connect_mutex = &connect_mutex_store;

int MQTTClient_init(void)
{
	pthread_mutexattr_t attr;
	int rc;

	pthread_mutexattr_init(&attr);
#if !defined(_WRS_KERNEL)
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#else
	/* #warning "no pthread_mutexattr_settype" */
#endif /* !defined(_WRS_KERNEL) */
	if ((rc = pthread_mutex_init(mqttclient_mutex, &attr)) != 0)
		printf("MQTTClient: error %d initializing client_mutex\n", rc);
	else if ((rc = pthread_mutex_init(socket_mutex, &attr)) != 0)
		printf("MQTTClient: error %d initializing socket_mutex\n", rc);
	else if ((rc = pthread_mutex_init(subscribe_mutex, &attr)) != 0)
		printf("MQTTClient: error %d initializing subscribe_mutex\n", rc);
	else if ((rc = pthread_mutex_init(unsubscribe_mutex, &attr)) != 0)
		printf("MQTTClient: error %d initializing unsubscribe_mutex\n", rc);
	else if ((rc = pthread_mutex_init(connect_mutex, &attr)) != 0)
		printf("MQTTClient: error %d initializing connect_mutex\n", rc);

	return rc;
}

#define WINAPI
#endif

static volatile int library_initialized = 0;
static List* handles = NULL;
static int running = 0;
static int tostop = 0;
static thread_id_type run_id = 0;

typedef struct
{
	MQTTClient_message* msg;
	char* topicName;
	int topicLen;
	unsigned int seqno; /* only used on restore */
} qEntry;


typedef struct
{
	char* serverURI;
	const char* currentServerURI; /* when using HA options, set the currently used serverURI */
#if defined(OPENSSL)
	int ssl;
#endif
	int websocket;
	Clients* c;
	MQTTClient_connectionLost* cl;
	MQTTClient_messageArrived* ma;
	MQTTClient_deliveryComplete* dc;
	void* context;

	MQTTClient_disconnected* disconnected;
	void* disconnected_context; /* the context to be associated with the disconnected callback*/

	MQTTClient_published* published;
	void* published_context; /* the context to be associated with the disconnected callback*/

#if 0
	MQTTClient_authHandle* auth_handle;
	void* auth_handle_context; /* the context to be associated with the authHandle callback*/
#endif

	sem_type connect_sem;
	int rc; /* getsockopt return code in connect */
	sem_type connack_sem;
	sem_type suback_sem;
	sem_type unsuback_sem;
	MQTTPacket* pack;

} MQTTClients;

struct props_rc_parms
{
	MQTTClients* m;
	MQTTProperties* properties;
	enum MQTTReasonCodes reasonCode;
};

static void MQTTClient_terminate(void);
static void MQTTClient_emptyMessageQueue(Clients* client);
static int MQTTClient_deliverMessage(
		int rc, MQTTClients* m,
		char** topicName, int* topicLen,
		MQTTClient_message** message);
static int clientSockCompare(void* a, void* b);
static thread_return_type WINAPI connectionLost_call(void* context);
static thread_return_type WINAPI MQTTClient_run(void* n);
static int MQTTClient_stop(void);
static void MQTTClient_closeSession(Clients* client, enum MQTTReasonCodes reason, MQTTProperties* props);
static int MQTTClient_cleanSession(Clients* client);
static MQTTResponse MQTTClient_connectURIVersion(
	MQTTClient handle, MQTTClient_connectOptions* options,
	const char* serverURI, int MQTTVersion,
	START_TIME_TYPE start, long millisecsTimeout,
	MQTTProperties* connectProperties, MQTTProperties* willProperties);
static MQTTResponse MQTTClient_connectURI(MQTTClient handle, MQTTClient_connectOptions* options, const char* serverURI,
	MQTTProperties* connectProperties, MQTTProperties* willProperties);
static int MQTTClient_disconnect1(MQTTClient handle, int timeout, int internal, int stop, enum MQTTReasonCodes, MQTTProperties*);
static int MQTTClient_disconnect_internal(MQTTClient handle, int timeout);
static void MQTTClient_retry(void);
static MQTTPacket* MQTTClient_cycle(int* sock, unsigned long timeout, int* rc);
static MQTTPacket* MQTTClient_waitfor(MQTTClient handle, int packet_type, int* rc, long timeout);
/*static int pubCompare(void* a, void* b); */
static void MQTTProtocol_checkPendingWrites(void);
static void MQTTClient_writeComplete(int socket, int rc);


int MQTTClient_createWithOptions(MQTTClient* handle, const char* serverURI, const char* clientId,
		int persistence_type, void* persistence_context, MQTTClient_createOptions* options)
{
	int rc = 0;
	MQTTClients *m = NULL;

#if (defined(_WIN32) || defined(_WIN64)) && defined(PAHO_MQTT_STATIC)
	/* intializes mutexes once.  Must come before FUNC_ENTRY */
	BOOL bStatus = InitOnceExecuteOnce(&g_InitOnce, InitOnceFunction, NULL, NULL);
#endif
	FUNC_ENTRY;
	if ((rc = Thread_lock_mutex(mqttclient_mutex)) != 0)
		goto exit;

	if (serverURI == NULL || clientId == NULL)
	{
		rc = MQTTCLIENT_NULL_PARAMETER;
		goto exit;
	}

	if (!UTF8_validateString(clientId))
	{
		rc = MQTTCLIENT_BAD_UTF8_STRING;
		goto exit;
	}

	if (strstr(serverURI, "://") != NULL)
	{
		if (strncmp(URI_TCP, serverURI, strlen(URI_TCP)) != 0
		 && strncmp(URI_WS, serverURI, strlen(URI_WS)) != 0
#if defined(OPENSSL)
            && strncmp(URI_SSL, serverURI, strlen(URI_SSL)) != 0
		 && strncmp(URI_WSS, serverURI, strlen(URI_WSS)) != 0
#endif
			)
		{
			rc = MQTTCLIENT_BAD_PROTOCOL;
			goto exit;
		}
	}

	if (options && (strncmp(options->struct_id, "MQCO", 4) != 0 || options->struct_version != 0))
	{
		rc = MQTTCLIENT_BAD_STRUCTURE;
		goto exit;
	}

	if (!library_initialized)
	{
		#if !defined(NO_HEAP_TRACKING)
			Heap_initialize();
		#endif
		Log_initialize((Log_nameValue*)MQTTClient_getVersionInfo());
		bstate->clients = ListInitialize();
		Socket_outInitialize();
		Socket_setWriteCompleteCallback(MQTTClient_writeComplete);
		handles = ListInitialize();
#if defined(OPENSSL)
		SSLSocket_initialize();
#endif
		library_initialized = 1;
	}

	if ((m = malloc(sizeof(MQTTClients))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	*handle = m;
	memset(m, '\0', sizeof(MQTTClients));
	if (strncmp(URI_TCP, serverURI, strlen(URI_TCP)) == 0)
		serverURI += strlen(URI_TCP);
	else if (strncmp(URI_WS, serverURI, strlen(URI_WS)) == 0)
	{
		serverURI += strlen(URI_WS);
		m->websocket = 1;
	}
	else if (strncmp(URI_SSL, serverURI, strlen(URI_SSL)) == 0)
	{
#if defined(OPENSSL)
		serverURI += strlen(URI_SSL);
		m->ssl = 1;
#else
		rc = MQTTCLIENT_SSL_NOT_SUPPORTED;
		goto exit;
#endif
	}
	else if (strncmp(URI_WSS, serverURI, strlen(URI_WSS)) == 0)
	{
#if defined(OPENSSL)
		serverURI += strlen(URI_WSS);
		m->ssl = 1;
		m->websocket = 1;
#else
		rc = MQTTCLIENT_SSL_NOT_SUPPORTED;
		goto exit;
#endif
	}
	m->serverURI = MQTTStrdup(serverURI);
	ListAppend(handles, m, sizeof(MQTTClients));

	if ((m->c = malloc(sizeof(Clients))) == NULL)
	{
		ListRemove(handles, m);
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	memset(m->c, '\0', sizeof(Clients));
	m->c->context = m;
	m->c->MQTTVersion = (options) ? options->MQTTVersion : MQTTVERSION_DEFAULT;
	m->c->outboundMsgs = ListInitialize();
	m->c->inboundMsgs = ListInitialize();
	m->c->messageQueue = ListInitialize();
	m->c->clientID = MQTTStrdup(clientId);
	m->connect_sem = Thread_create_sem(&rc);
	m->connack_sem = Thread_create_sem(&rc);
	m->suback_sem = Thread_create_sem(&rc);
	m->unsuback_sem = Thread_create_sem(&rc);

#if !defined(NO_PERSISTENCE)
	rc = MQTTPersistence_create(&(m->c->persistence), persistence_type, persistence_context);
	if (rc == 0)
	{
		rc = MQTTPersistence_initialize(m->c, m->serverURI);
		if (rc == 0)
			MQTTPersistence_restoreMessageQueue(m->c);
	}
#endif
	ListAppend(bstate->clients, m->c, sizeof(Clients) + 3*sizeof(List));

exit:
	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTClient_create(MQTTClient* handle, const char* serverURI, const char* clientId,
		int persistence_type, void* persistence_context)
{
	return MQTTClient_createWithOptions(handle, serverURI, clientId, persistence_type,
		persistence_context, NULL);
}


static void MQTTClient_terminate(void)
{
	FUNC_ENTRY;
	MQTTClient_stop();
	if (library_initialized)
	{
		ListFree(bstate->clients);
		ListFree(handles);
		handles = NULL;
		WebSocket_terminate();
		#if !defined(NO_HEAP_TRACKING)
			Heap_terminate();
		#endif
		Log_terminate();
		library_initialized = 0;
	}
	FUNC_EXIT;
}


static void MQTTClient_emptyMessageQueue(Clients* client)
{
	FUNC_ENTRY;
	/* empty message queue */
	if (client->messageQueue->count > 0)
	{
		ListElement* current = NULL;
		while (ListNextElement(client->messageQueue, &current))
		{
			qEntry* qe = (qEntry*)(current->content);
			free(qe->topicName);
			MQTTProperties_free(&qe->msg->properties);
			free(qe->msg->payload);
			free(qe->msg);
		}
		ListEmpty(client->messageQueue);
	}
	FUNC_EXIT;
}


void MQTTClient_destroy(MQTTClient* handle)
{
	MQTTClients* m = *handle;

	FUNC_ENTRY;
	Thread_lock_mutex(connect_mutex);
	Thread_lock_mutex(mqttclient_mutex);

	if (m == NULL)
		goto exit;

	if (m->c)
	{
		int saved_socket = m->c->net.socket;
		char* saved_clientid = MQTTStrdup(m->c->clientID);
#if !defined(NO_PERSISTENCE)
		MQTTPersistence_close(m->c);
#endif
		MQTTClient_emptyMessageQueue(m->c);
		MQTTProtocol_freeClient(m->c);
		if (!ListRemove(bstate->clients, m->c))
			Log(LOG_ERROR, 0, NULL);
		else
			Log(TRACE_MIN, 1, NULL, saved_clientid, saved_socket);
		free(saved_clientid);
	}
	if (m->serverURI)
		free(m->serverURI);
	Thread_destroy_sem(m->connect_sem);
	Thread_destroy_sem(m->connack_sem);
	Thread_destroy_sem(m->suback_sem);
	Thread_destroy_sem(m->unsuback_sem);
	if (!ListRemove(handles, m))
		Log(LOG_ERROR, -1, "free error");
	*handle = NULL;
	if (bstate->clients->count == 0)
		MQTTClient_terminate();

exit:
	Thread_unlock_mutex(mqttclient_mutex);
	Thread_unlock_mutex(connect_mutex);
	FUNC_EXIT;
}


void MQTTClient_freeMessage(MQTTClient_message** message)
{
	FUNC_ENTRY;
	MQTTProperties_free(&(*message)->properties);
	free((*message)->payload);
	free(*message);
	*message = NULL;
	FUNC_EXIT;
}


void MQTTClient_free(void* memory)
{
	FUNC_ENTRY;
	free(memory);
	FUNC_EXIT;
}


void MQTTResponse_free(MQTTResponse response)
{
	FUNC_ENTRY;
	if (response.properties)
	{
		if (response.reasonCodeCount > 0 && response.reasonCodes)
			free(response.reasonCodes);
		MQTTProperties_free(response.properties);
		free(response.properties);
	}
	FUNC_EXIT;
}


static int MQTTClient_deliverMessage(int rc, MQTTClients* m, char** topicName, int* topicLen, MQTTClient_message** message)
{
	qEntry* qe = (qEntry*)(m->c->messageQueue->first->content);

	FUNC_ENTRY;
	*message = qe->msg;
	*topicName = qe->topicName;
	*topicLen = qe->topicLen;
	if (strlen(*topicName) != *topicLen)
		rc = MQTTCLIENT_TOPICNAME_TRUNCATED;
#if !defined(NO_PERSISTENCE)
	if (m->c->persistence)
		MQTTPersistence_unpersistQueueEntry(m->c, (MQTTPersistence_qEntry*)qe);
#endif
	ListRemove(m->c->messageQueue, m->c->messageQueue->first->content);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * List callback function for comparing clients by socket
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
static int clientSockCompare(void* a, void* b)
{
	MQTTClients* m = (MQTTClients*)a;
	return m->c->net.socket == *(int*)b;
}


/**
 * Wrapper function to call connection lost on a separate thread.  A separate thread is needed to allow the
 * connectionLost function to make API calls (e.g. connect)
 * @param context a pointer to the relevant client
 * @return thread_return_type standard thread return value - not used here
 */
static thread_return_type WINAPI connectionLost_call(void* context)
{
	MQTTClients* m = (MQTTClients*)context;

	(*(m->cl))(m->context, NULL);
	return 0;
}


int MQTTClient_setDisconnected(MQTTClient handle, void* context, MQTTClient_disconnected* disconnected)
{
	int rc = MQTTCLIENT_SUCCESS;
	MQTTClients* m = handle;

	FUNC_ENTRY;
	Thread_lock_mutex(mqttclient_mutex);

	if (m == NULL || m->c->connect_state != NOT_IN_PROGRESS)
		rc = MQTTCLIENT_FAILURE;
	else
	{
		m->disconnected_context = context;
		m->disconnected = disconnected;
	}

	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}



/**
 * Wrapper function to call disconnected on a separate thread.  A separate thread is needed to allow the
 * disconnected function to make API calls (e.g. connect)
 * @param context a pointer to the relevant client
 * @return thread_return_type standard thread return value - not used here
 */
static thread_return_type WINAPI call_disconnected(void* context)
{
	struct props_rc_parms* pr = (struct props_rc_parms*)context;

	(*(pr->m->disconnected))(pr->m->disconnected_context, pr->properties, pr->reasonCode);
	MQTTProperties_free(pr->properties);
	free(pr->properties);
	free(pr);
	return 0;
}


int MQTTClient_setPublished(MQTTClient handle, void* context, MQTTClient_published* published)
{
	int rc = MQTTCLIENT_SUCCESS;
	MQTTClients* m = handle;

	FUNC_ENTRY;
	Thread_lock_mutex(mqttclient_mutex);

	if (m == NULL || m->c->connect_state != NOT_IN_PROGRESS)
		rc = MQTTCLIENT_FAILURE;
	else
	{
		m->published_context = context;
		m->published = published;
	}

	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


#if 0
int MQTTClient_setHandleAuth(MQTTClient handle, void* context, MQTTClient_handleAuth* auth_handle)
{
	int rc = MQTTCLIENT_SUCCESS;
	MQTTClients* m = handle;

	FUNC_ENTRY;
	Thread_lock_mutex(mqttclient_mutex);

	if (m == NULL || m->c->connect_state != NOT_IN_PROGRESS)
		rc = MQTTCLIENT_FAILURE;
	else
	{
		m->auth_handle_context = context;
		m->auth_handle = auth_handle;
	}

	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Wrapper function to call authHandle on a separate thread.  A separate thread is needed to allow the
 * disconnected function to make API calls (e.g. MQTTClient_auth)
 * @param context a pointer to the relevant client
 * @return thread_return_type standard thread return value - not used here
 */
static thread_return_type WINAPI call_auth_handle(void* context)
{
	struct props_rc_parms* pr = (struct props_rc_parms*)context;

	(*(pr->m->auth_handle))(pr->m->auth_handle_context, pr->properties, pr->reasonCode);
	MQTTProperties_free(pr->properties);
	free(pr->properties);
	free(pr);
	return 0;
}
#endif


/* This is the thread function that handles the calling of callback functions if set */
static thread_return_type WINAPI MQTTClient_run(void* n)
{
	long timeout = 10L; /* first time in we have a small timeout.  Gets things started more quickly */

	FUNC_ENTRY;
	running = 1;
	run_id = Thread_getid();

	Thread_lock_mutex(mqttclient_mutex);
	while (!tostop)
	{
		int rc = SOCKET_ERROR;
		int sock = -1;
		MQTTClients* m = NULL;
		MQTTPacket* pack = NULL;

		Thread_unlock_mutex(mqttclient_mutex);
		pack = MQTTClient_cycle(&sock, timeout, &rc);
		Thread_lock_mutex(mqttclient_mutex);
		if (tostop)
			break;
		timeout = 1000L;

		/* find client corresponding to socket */
		if (ListFindItem(handles, &sock, clientSockCompare) == NULL)
		{
			/* assert: should not happen */
			continue;
		}
		m = (MQTTClient)(handles->current->content);
		if (m == NULL)
		{
			/* assert: should not happen */
			continue;
		}
		if (rc == SOCKET_ERROR)
		{
			if (m->c->connected)
				MQTTClient_disconnect_internal(m, 0);
			else
			{
				if (m->c->connect_state == SSL_IN_PROGRESS)
				{
					Log(TRACE_MIN, -1, "Posting connect semaphore for client %s", m->c->clientID);
					m->c->connect_state = NOT_IN_PROGRESS;
					Thread_post_sem(m->connect_sem);
				}
				if (m->c->connect_state == WAIT_FOR_CONNACK)
				{
					Log(TRACE_MIN, -1, "Posting connack semaphore for client %s", m->c->clientID);
					m->c->connect_state = NOT_IN_PROGRESS;
					Thread_post_sem(m->connack_sem);
				}
			}
		}
		else
		{
			if (m->c->messageQueue->count > 0)
			{
				qEntry* qe = (qEntry*)(m->c->messageQueue->first->content);
				int topicLen = qe->topicLen;

				if (strlen(qe->topicName) == topicLen)
					topicLen = 0;

				Log(TRACE_MIN, -1, "Calling messageArrived for client %s, queue depth %d",
					m->c->clientID, m->c->messageQueue->count);
				Thread_unlock_mutex(mqttclient_mutex);
				rc = (*(m->ma))(m->context, qe->topicName, topicLen, qe->msg);
				Thread_lock_mutex(mqttclient_mutex);
				/* if 0 (false) is returned by the callback then it failed, so we don't remove the message from
				 * the queue, and it will be retried later.  If 1 is returned then the message data may have been freed,
				 * so we must be careful how we use it.
				 */
				if (rc)
				{
					#if !defined(NO_PERSISTENCE)
					if (m->c->persistence)
						MQTTPersistence_unpersistQueueEntry(m->c, (MQTTPersistence_qEntry*)qe);
					#endif
					ListRemove(m->c->messageQueue, qe);
				}
				else
					Log(TRACE_MIN, -1, "False returned from messageArrived for client %s, message remains on queue",
						m->c->clientID);
			}
			if (pack)
			{
				if (pack->header.bits.type == CONNACK)
				{
					Log(TRACE_MIN, -1, "Posting connack semaphore for client %s", m->c->clientID);
					m->pack = pack;
					Thread_post_sem(m->connack_sem);
				}
				else if (pack->header.bits.type == SUBACK)
				{
					Log(TRACE_MIN, -1, "Posting suback semaphore for client %s", m->c->clientID);
					m->pack = pack;
					Thread_post_sem(m->suback_sem);
				}
				else if (pack->header.bits.type == UNSUBACK)
				{
					Log(TRACE_MIN, -1, "Posting unsuback semaphore for client %s", m->c->clientID);
					m->pack = pack;
					Thread_post_sem(m->unsuback_sem);
				}
				else if (m->c->MQTTVersion >= MQTTVERSION_5)
				{
					if (pack->header.bits.type == DISCONNECT && m->disconnected)
					{
						struct props_rc_parms* dp;
						Ack* disc = (Ack*)pack;

						dp = malloc(sizeof(struct props_rc_parms));
						if (dp)
						{
							dp->m = m;
							dp->reasonCode = disc->rc;
							dp->properties = malloc(sizeof(MQTTProperties));
							if (dp->properties)
							{
								*(dp->properties) = disc->properties;
								MQTTClient_disconnect1(m, 10, 0, 1, MQTTREASONCODE_SUCCESS, NULL);
								Log(TRACE_MIN, -1, "Calling disconnected for client %s", m->c->clientID);
								Thread_start(call_disconnected, dp);
							}
							else
								free(dp);
						}
						free(disc);
					}
#if 0
					if (pack->header.bits.type == AUTH && m->auth_handle)
					{
						struct props_rc_parms dp;
						Ack* disc = (Ack*)pack;

						dp.m = m;
						dp.properties = &disc->properties;
						dp.reasonCode = disc->rc;
						free(pack);
						Log(TRACE_MIN, -1, "Calling auth_handle for client %s", m->c->clientID);
						Thread_start(call_auth_handle, &dp);
					}
#endif
				}
			}
			else if (m->c->connect_state == TCP_IN_PROGRESS)
			{
				int error;
				socklen_t len = sizeof(error);

				if ((m->rc = getsockopt(m->c->net.socket, SOL_SOCKET, SO_ERROR, (char*)&error, &len)) == 0)
					m->rc = error;
				Log(TRACE_MIN, -1, "Posting connect semaphore for client %s rc %d", m->c->clientID, m->rc);
				m->c->connect_state = NOT_IN_PROGRESS;
				Thread_post_sem(m->connect_sem);
			}
#if defined(OPENSSL)
			else if (m->c->connect_state == SSL_IN_PROGRESS)
			{
				rc = m->c->sslopts->struct_version >= 3 ?
					SSLSocket_connect(m->c->net.ssl, m->c->net.socket, m->serverURI,
						m->c->sslopts->verify, m->c->sslopts->ssl_error_cb, m->c->sslopts->ssl_error_context) :
					SSLSocket_connect(m->c->net.ssl, m->c->net.socket, m->serverURI,
						m->c->sslopts->verify, NULL, NULL);
				if (rc == 1 || rc == SSL_FATAL)
				{
					if (rc == 1 && (m->c->cleansession == 0 && m->c->cleanstart == 0) && m->c->session == NULL)
						m->c->session = SSL_get1_session(m->c->net.ssl);
					m->rc = rc;
					Log(TRACE_MIN, -1, "Posting connect semaphore for SSL client %s rc %d", m->c->clientID, m->rc);
					m->c->connect_state = NOT_IN_PROGRESS;
					Thread_post_sem(m->connect_sem);
				}
			}
#endif

			else if (m->c->connect_state == WEBSOCKET_IN_PROGRESS)
			{
				if (rc != TCPSOCKET_INTERRUPTED)
				{
					Log(TRACE_MIN, -1, "Posting websocket handshake for client %s rc %d", m->c->clientID, m->rc);
					m->c->connect_state = WAIT_FOR_CONNACK;
					Thread_post_sem(m->connect_sem);
				}
			}
		}
	}
	run_id = 0;
	running = tostop = 0;
	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT;
#if defined(_WIN32) || defined(_WIN64)
	ExitThread(0);
#endif
	return 0;
}


static int MQTTClient_stop(void)
{
	int rc = 0;

	FUNC_ENTRY;
	if (running == 1 && tostop == 0)
	{
		int conn_count = 0;
		ListElement* current = NULL;

		if (handles != NULL)
		{
			/* find out how many handles are still connected */
			while (ListNextElement(handles, &current))
			{
				if (((MQTTClients*)(current->content))->c->connect_state > NOT_IN_PROGRESS ||
						((MQTTClients*)(current->content))->c->connected)
					++conn_count;
			}
		}
		Log(TRACE_MIN, -1, "Conn_count is %d", conn_count);
		/* stop the background thread, if we are the last one to be using it */
		if (conn_count == 0)
		{
			int count = 0;
			tostop = 1;
			if (Thread_getid() != run_id)
			{
				while (running && ++count < 100)
				{
					Thread_unlock_mutex(mqttclient_mutex);
					Log(TRACE_MIN, -1, "sleeping");
					MQTTTime_sleep(100L);
					Thread_lock_mutex(mqttclient_mutex);
				}
			}
			rc = 1;
		}
	}
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTClient_setCallbacks(MQTTClient handle, void* context, MQTTClient_connectionLost* cl,
														MQTTClient_messageArrived* ma, MQTTClient_deliveryComplete* dc)
{
	int rc = MQTTCLIENT_SUCCESS;
	MQTTClients* m = handle;

	FUNC_ENTRY;
	Thread_lock_mutex(mqttclient_mutex);

	if (m == NULL || ma == NULL || m->c->connect_state != NOT_IN_PROGRESS)
		rc = MQTTCLIENT_FAILURE;
	else
	{
		m->context = context;
		m->cl = cl;
		m->ma = ma;
		m->dc = dc;
	}

	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


static void MQTTClient_closeSession(Clients* client, enum MQTTReasonCodes reason, MQTTProperties* props)
{
	FUNC_ENTRY;
	client->good = 0;
	client->ping_outstanding = 0;
	if (client->net.socket > 0)
	{
		if (client->connected)
			MQTTPacket_send_disconnect(client, reason, props);
		Thread_lock_mutex(socket_mutex);
		WebSocket_close(&client->net, WebSocket_CLOSE_NORMAL, NULL);

#if defined(OPENSSL)
		SSLSocket_close(&client->net);
#endif
		Socket_close(client->net.socket);
		Thread_unlock_mutex(socket_mutex);
		client->net.socket = 0;
#if defined(OPENSSL)
		client->net.ssl = NULL;
#endif
	}
	client->connected = 0;
	client->connect_state = NOT_IN_PROGRESS;

	if (client->MQTTVersion < MQTTVERSION_5 && client->cleansession)
		MQTTClient_cleanSession(client);
	FUNC_EXIT;
}


static int MQTTClient_cleanSession(Clients* client)
{
	int rc = 0;

	FUNC_ENTRY;
#if !defined(NO_PERSISTENCE)
	rc = MQTTPersistence_clear(client);
#endif
	MQTTProtocol_emptyMessageList(client->inboundMsgs);
	MQTTProtocol_emptyMessageList(client->outboundMsgs);
	MQTTClient_emptyMessageQueue(client);
	client->msgID = 0;
	FUNC_EXIT_RC(rc);
	return rc;
}


void Protocol_processPublication(Publish* publish, Clients* client, int allocatePayload)
{
	qEntry* qe = NULL;
	MQTTClient_message* mm = NULL;
	MQTTClient_message initialized = MQTTClient_message_initializer;

	FUNC_ENTRY;
	qe = malloc(sizeof(qEntry));
	if (!qe)
		goto exit;
	mm = malloc(sizeof(MQTTClient_message));
	if (!mm)
	{
		free(qe);
		goto exit;
	}
	memcpy(mm, &initialized, sizeof(MQTTClient_message));

	qe->msg = mm;
	qe->topicName = publish->topic;
	qe->topicLen = publish->topiclen;
	publish->topic = NULL;
	if (allocatePayload)
	{
		mm->payload = malloc(publish->payloadlen);
		if (mm->payload == NULL)
		{
			free(mm);
			free(qe);
			goto exit;
		}
		memcpy(mm->payload, publish->payload, publish->payloadlen);
	}
	else
		mm->payload = publish->payload;
	mm->payloadlen = publish->payloadlen;
	mm->qos = publish->header.bits.qos;
	mm->retained = publish->header.bits.retain;
	if (publish->header.bits.qos == 2)
		mm->dup = 0;  /* ensure that a QoS2 message is not passed to the application with dup = 1 */
	else
		mm->dup = publish->header.bits.dup;
	mm->msgid = publish->msgId;

	if (publish->MQTTVersion >= 5)
		mm->properties = MQTTProperties_copy(&publish->properties);

	ListAppend(client->messageQueue, qe, sizeof(qe) + sizeof(mm) + mm->payloadlen + strlen(qe->topicName)+1);
#if !defined(NO_PERSISTENCE)
	if (client->persistence)
		MQTTPersistence_persistQueueEntry(client, (MQTTPersistence_qEntry*)qe);
#endif
exit:
	FUNC_EXIT;
}


static MQTTResponse MQTTClient_connectURIVersion(MQTTClient handle, MQTTClient_connectOptions* options, const char* serverURI, int MQTTVersion,
	START_TIME_TYPE start, long millisecsTimeout, MQTTProperties* connectProperties, MQTTProperties* willProperties)
{
	MQTTClients* m = handle;
	int rc = SOCKET_ERROR;
	int sessionPresent = 0;
	MQTTResponse resp = MQTTResponse_initializer;

	FUNC_ENTRY;
	resp.reasonCode = SOCKET_ERROR;
	if (m->ma && !running)
	{
		Thread_start(MQTTClient_run, handle);
		if (MQTTTime_elapsed(start) >= millisecsTimeout)
		{
			rc = SOCKET_ERROR;
			goto exit;
		}
		MQTTTime_sleep(100L);
	}

	Log(TRACE_MIN, -1, "Connecting to serverURI %s with MQTT version %d", serverURI, MQTTVersion);
#if defined(OPENSSL)
#if defined(__GNUC__) && defined(__linux__)
	rc = MQTTProtocol_connect(serverURI, m->c, m->ssl, m->websocket, MQTTVersion, connectProperties, willProperties,
			millisecsTimeout - MQTTTime_elapsed(start));
#else
	rc = MQTTProtocol_connect(serverURI, m->c, m->ssl, m->websocket, MQTTVersion, connectProperties, willProperties);
#endif
#else
#if defined(__GNUC__) && defined(__linux__)
	rc = MQTTProtocol_connect(serverURI, m->c, m->websocket, MQTTVersion, connectProperties, willProperties,
			millisecsTimeout - MQTTTime_elapsed(start));
#else
	rc = MQTTProtocol_connect(serverURI, m->c, m->websocket, MQTTVersion, connectProperties, willProperties);
#endif
#endif
	if (rc == SOCKET_ERROR)
		goto exit;

	if (m->c->connect_state == NOT_IN_PROGRESS)
	{
		rc = SOCKET_ERROR;
		goto exit;
	}

	if (m->c->connect_state == TCP_IN_PROGRESS) /* TCP connect started - wait for completion */
	{
		Thread_unlock_mutex(mqttclient_mutex);
		MQTTClient_waitfor(handle, CONNECT, &rc, millisecsTimeout - MQTTTime_elapsed(start));
		Thread_lock_mutex(mqttclient_mutex);
		if (rc != 0)
		{
			rc = SOCKET_ERROR;
			goto exit;
		}
#if defined(OPENSSL)
		if (m->ssl)
		{
			int port;
			size_t hostname_len;
			const char *topic;
			int setSocketForSSLrc = 0;

			if (m->websocket && m->c->net.https_proxy) {
				m->c->connect_state = PROXY_CONNECT_IN_PROGRESS;
				if ((rc = WebSocket_proxy_connect( &m->c->net, 1, serverURI)) == SOCKET_ERROR )
					goto exit;
			}

			hostname_len = MQTTProtocol_addressPort(serverURI, &port, &topic);
			setSocketForSSLrc = SSLSocket_setSocketForSSL(&m->c->net, m->c->sslopts,
				serverURI, hostname_len);

			if (setSocketForSSLrc != MQTTCLIENT_SUCCESS)
			{
				if (m->c->session != NULL)
					if ((rc = SSL_set_session(m->c->net.ssl, m->c->session)) != 1)
						Log(TRACE_MIN, -1, "Failed to set SSL session with stored data, non critical");
				rc = m->c->sslopts->struct_version >= 3 ?
					SSLSocket_connect(m->c->net.ssl, m->c->net.socket, serverURI,
						m->c->sslopts->verify, m->c->sslopts->ssl_error_cb, m->c->sslopts->ssl_error_context) :
					SSLSocket_connect(m->c->net.ssl, m->c->net.socket, serverURI,
						m->c->sslopts->verify, NULL, NULL);
				if (rc == TCPSOCKET_INTERRUPTED)
					m->c->connect_state = SSL_IN_PROGRESS;  /* the connect is still in progress */
				else if (rc == SSL_FATAL)
				{
					rc = SOCKET_ERROR;
					goto exit;
				}
				else if (rc == 1)
				{
					if (m->websocket)
					{
						m->c->connect_state = WEBSOCKET_IN_PROGRESS;
						rc = WebSocket_connect(&m->c->net, serverURI);
						if ( rc == SOCKET_ERROR )
							goto exit;
					}
					else
					{
						rc = MQTTCLIENT_SUCCESS;
						m->c->connect_state = WAIT_FOR_CONNACK;
						if (MQTTPacket_send_connect(m->c, MQTTVersion, connectProperties, willProperties) == SOCKET_ERROR)
						{
							rc = SOCKET_ERROR;
							goto exit;
						}
						if ((m->c->cleansession == 0 && m->c->cleanstart == 0) && m->c->session == NULL)
							m->c->session = SSL_get1_session(m->c->net.ssl);
					}
				}
			}
			else
			{
				rc = SOCKET_ERROR;
				goto exit;
			}
		}
#endif
		else if (m->websocket)
		{
			if (m->c->net.http_proxy) {
				m->c->connect_state = PROXY_CONNECT_IN_PROGRESS;
				if ((rc = WebSocket_proxy_connect( &m->c->net, 0, serverURI)) == SOCKET_ERROR )
					goto exit;
			}

			m->c->connect_state = WEBSOCKET_IN_PROGRESS;
			if ( WebSocket_connect(&m->c->net, serverURI) == SOCKET_ERROR )
			{
				rc = SOCKET_ERROR;
				goto exit;
			}
		}
		else
		{
			m->c->connect_state = WAIT_FOR_CONNACK; /* TCP connect completed, in which case send the MQTT connect packet */
			if (MQTTPacket_send_connect(m->c, MQTTVersion, connectProperties, willProperties) == SOCKET_ERROR)
			{
				rc = SOCKET_ERROR;
				goto exit;
			}
		}
	}

#if defined(OPENSSL)
	if (m->c->connect_state == SSL_IN_PROGRESS) /* SSL connect sent - wait for completion */
	{
		Thread_unlock_mutex(mqttclient_mutex);
		MQTTClient_waitfor(handle, CONNECT, &rc, millisecsTimeout - MQTTTime_elapsed(start));
		Thread_lock_mutex(mqttclient_mutex);
		if (rc != 1)
		{
			rc = SOCKET_ERROR;
			goto exit;
		}
		if((m->c->cleansession == 0 && m->c->cleanstart == 0) && m->c->session == NULL)
			m->c->session = SSL_get1_session(m->c->net.ssl);

		if ( m->websocket )
		{
			/* wait for websocket connect */
			m->c->connect_state = WEBSOCKET_IN_PROGRESS;
			rc = WebSocket_connect( &m->c->net, serverURI );
			if ( rc != 1 )
			{
				rc = SOCKET_ERROR;
				goto exit;
			}
		}
		else
		{
			m->c->connect_state = WAIT_FOR_CONNACK; /* TCP connect completed, in which case send the MQTT connect packet */
			if (MQTTPacket_send_connect(m->c, MQTTVersion, connectProperties, willProperties) == SOCKET_ERROR)
			{
				rc = SOCKET_ERROR;
				goto exit;
			}
		}
	}
#endif

	if (m->c->connect_state == WEBSOCKET_IN_PROGRESS) /* websocket request sent - wait for upgrade */
	{
		Thread_unlock_mutex(mqttclient_mutex);
		MQTTClient_waitfor(handle, CONNECT, &rc, millisecsTimeout - MQTTTime_elapsed(start));
		Thread_lock_mutex(mqttclient_mutex);
		m->c->connect_state = WAIT_FOR_CONNACK; /* websocket upgrade complete */
		if (MQTTPacket_send_connect(m->c, MQTTVersion, connectProperties, willProperties) == SOCKET_ERROR)
		{
			rc = SOCKET_ERROR;
			goto exit;
		}
	}

	if (m->c->connect_state == WAIT_FOR_CONNACK) /* MQTT connect sent - wait for CONNACK */
	{
		MQTTPacket* pack = NULL;
		Thread_unlock_mutex(mqttclient_mutex);
		pack = MQTTClient_waitfor(handle, CONNACK, &rc, millisecsTimeout - MQTTTime_elapsed(start));
		Thread_lock_mutex(mqttclient_mutex);
		if (pack == NULL)
			rc = SOCKET_ERROR;
		else
		{
			Connack* connack = (Connack*)pack;
			Log(TRACE_PROTOCOL, 1, NULL, m->c->net.socket, m->c->clientID, connack->rc);
			if ((rc = connack->rc) == MQTTCLIENT_SUCCESS)
			{
				m->c->connected = 1;
				m->c->good = 1;
				m->c->connect_state = NOT_IN_PROGRESS;
				if (MQTTVersion == 4)
					sessionPresent = connack->flags.bits.sessionPresent;
				if (m->c->cleansession || m->c->cleanstart)
					rc = MQTTClient_cleanSession(m->c);
				if (m->c->outboundMsgs->count > 0)
				{
					ListElement* outcurrent = NULL;
					START_TIME_TYPE zero = START_TIME_ZERO;

					while (ListNextElement(m->c->outboundMsgs, &outcurrent))
					{
						Messages* m = (Messages*)(outcurrent->content);
						memset(&m->lastTouch, '\0', sizeof(m->lastTouch));
					}
					MQTTProtocol_retry(zero, 1, 1);
					if (m->c->connected != 1)
						rc = MQTTCLIENT_DISCONNECTED;
				}
				if (m->c->MQTTVersion == MQTTVERSION_5)
				{
					if ((resp.properties = malloc(sizeof(MQTTProperties))) == NULL)
					{
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
					*resp.properties = MQTTProperties_copy(&connack->properties);
				}
			}
			MQTTPacket_freeConnack(connack);
			m->pack = NULL;
		}
	}
exit:
	if (rc == MQTTCLIENT_SUCCESS)
	{
		if (options->struct_version >= 4) /* means we have to fill out return values */
		{
			options->returned.serverURI = serverURI;
			options->returned.MQTTVersion = MQTTVersion;
			options->returned.sessionPresent = sessionPresent;
		}
	}
	else
		MQTTClient_disconnect1(handle, 0, 0, (MQTTVersion == 3), MQTTREASONCODE_SUCCESS, NULL); /* don't want to call connection lost */

	resp.reasonCode = rc;
	FUNC_EXIT_RC(resp.reasonCode);
	return resp;
}

static int retryLoopInterval = 5;

static void setRetryLoopInterval(int keepalive)
{
	int proposed = keepalive / 10;

	if (proposed < 1)
		proposed = 1;
	else if (proposed > 5)
		proposed = 5;
	if (proposed < retryLoopInterval)
		retryLoopInterval = proposed;
}


static MQTTResponse MQTTClient_connectURI(MQTTClient handle, MQTTClient_connectOptions* options, const char* serverURI,
		MQTTProperties* connectProperties, MQTTProperties* willProperties)
{
	MQTTClients* m = handle;
	START_TIME_TYPE start;
	long millisecsTimeout = 30000L;
	MQTTResponse rc = MQTTResponse_initializer;
	int MQTTVersion = 0;

	FUNC_ENTRY;
	rc.reasonCode = SOCKET_ERROR;
	millisecsTimeout = options->connectTimeout * 1000;
	start = MQTTTime_start_clock();

	m->currentServerURI = serverURI;
	m->c->keepAliveInterval = options->keepAliveInterval;
	m->c->retryInterval = options->retryInterval;
	setRetryLoopInterval(options->keepAliveInterval);
	m->c->MQTTVersion = options->MQTTVersion;
	m->c->cleanstart = m->c->cleansession = 0;
	if (m->c->MQTTVersion >= MQTTVERSION_5)
		m->c->cleanstart = options->cleanstart;
	else
		m->c->cleansession = options->cleansession;
	m->c->maxInflightMessages = (options->reliable) ? 1 : 10;
	if (options->struct_version >= 6)
	{
		if (options->maxInflightMessages > 0)
			m->c->maxInflightMessages = options->maxInflightMessages;
	}

	if (options->struct_version >= 7)
	{
		m->c->net.httpHeaders = options->httpHeaders;
	}

	if (m->c->will)
	{
		free(m->c->will->payload);
		free(m->c->will->topic);
		free(m->c->will);
		m->c->will = NULL;
	}

	if (options->will && (options->will->struct_version == 0 || options->will->struct_version == 1))
	{
		const void* source = NULL;

		if ((m->c->will = malloc(sizeof(willMessages))) == NULL)
		{
			rc.reasonCode = PAHO_MEMORY_ERROR;
			goto exit;
		}
		if (options->will->message || (options->will->struct_version == 1 && options->will->payload.data))
		{
			if (options->will->struct_version == 1 && options->will->payload.data)
			{
				m->c->will->payloadlen = options->will->payload.len;
				source = options->will->payload.data;
			}
			else
			{
				m->c->will->payloadlen = (int)strlen(options->will->message);
				source = (void*)options->will->message;
			}
			if ((m->c->will->payload = malloc(m->c->will->payloadlen)) == NULL)
			{
				free(m->c->will);
				rc.reasonCode = PAHO_MEMORY_ERROR;
				goto exit;
			}
			memcpy(m->c->will->payload, source, m->c->will->payloadlen);
		}
		else
		{
			m->c->will->payload = NULL;
			m->c->will->payloadlen = 0;
		}
		m->c->will->qos = options->will->qos;
		m->c->will->retained = options->will->retained;
		m->c->will->topic = MQTTStrdup(options->will->topicName);
	}

#if defined(OPENSSL)
	if (m->c->sslopts)
	{
		if (m->c->sslopts->trustStore)
			free((void*)m->c->sslopts->trustStore);
		if (m->c->sslopts->keyStore)
			free((void*)m->c->sslopts->keyStore);
		if (m->c->sslopts->privateKey)
			free((void*)m->c->sslopts->privateKey);
		if (m->c->sslopts->privateKeyPassword)
			free((void*)m->c->sslopts->privateKeyPassword);
		if (m->c->sslopts->enabledCipherSuites)
			free((void*)m->c->sslopts->enabledCipherSuites);
		if (m->c->sslopts->struct_version >= 2)
		{
			if (m->c->sslopts->CApath)
				free((void*)m->c->sslopts->CApath);
		}
		free(m->c->sslopts);
		m->c->sslopts = NULL;
	}

	if (options->struct_version != 0 && options->ssl)
	{
		if ((m->c->sslopts = malloc(sizeof(MQTTClient_SSLOptions))) == NULL)
		{
			rc.reasonCode = PAHO_MEMORY_ERROR;
			goto exit;
		}
		memset(m->c->sslopts, '\0', sizeof(MQTTClient_SSLOptions));
		m->c->sslopts->struct_version = options->ssl->struct_version;
		if (options->ssl->trustStore)
			m->c->sslopts->trustStore = MQTTStrdup(options->ssl->trustStore);
		if (options->ssl->keyStore)
			m->c->sslopts->keyStore = MQTTStrdup(options->ssl->keyStore);
		if (options->ssl->privateKey)
			m->c->sslopts->privateKey = MQTTStrdup(options->ssl->privateKey);
		if (options->ssl->privateKeyPassword)
			m->c->sslopts->privateKeyPassword = MQTTStrdup(options->ssl->privateKeyPassword);
		if (options->ssl->enabledCipherSuites)
			m->c->sslopts->enabledCipherSuites = MQTTStrdup(options->ssl->enabledCipherSuites);
		m->c->sslopts->enableServerCertAuth = options->ssl->enableServerCertAuth;
		if (m->c->sslopts->struct_version >= 1)
			m->c->sslopts->sslVersion = options->ssl->sslVersion;
		if (m->c->sslopts->struct_version >= 2)
		{
			m->c->sslopts->verify = options->ssl->verify;
			if (options->ssl->CApath)
				m->c->sslopts->CApath = MQTTStrdup(options->ssl->CApath);
		}
		if (m->c->sslopts->struct_version >= 3)
		{
			m->c->sslopts->ssl_error_cb = options->ssl->ssl_error_cb;
			m->c->sslopts->ssl_error_context = options->ssl->ssl_error_context;
		}
		if (m->c->sslopts->struct_version >= 4)
		{
			m->c->sslopts->ssl_psk_cb = options->ssl->ssl_psk_cb;
			m->c->sslopts->ssl_psk_context = options->ssl->ssl_psk_context;
			m->c->sslopts->disableDefaultTrustStore = options->ssl->disableDefaultTrustStore;
		}
	}
#endif

	if (m->c->username)
		free((void*)m->c->username);
	if (options->username)
		m->c->username = MQTTStrdup(options->username);
	if (m->c->password)
		free((void*)m->c->password);
	if (options->password)
	{
		m->c->password = MQTTStrdup(options->password);
		m->c->passwordlen = (int)strlen(options->password);
	}
	else if (options->struct_version >= 5 && options->binarypwd.data)
	{
		m->c->passwordlen = options->binarypwd.len;
		if ((m->c->password = malloc(m->c->passwordlen)) == NULL)
		{
			rc.reasonCode = PAHO_MEMORY_ERROR;
			goto exit;
		}
		memcpy((void*)m->c->password, options->binarypwd.data, m->c->passwordlen);
	}

	if (options->struct_version >= 3)
		MQTTVersion = options->MQTTVersion;
	else
		MQTTVersion = MQTTVERSION_DEFAULT;

	if (MQTTVersion == MQTTVERSION_DEFAULT)
	{
		rc = MQTTClient_connectURIVersion(handle, options, serverURI, 4, start, millisecsTimeout,
				connectProperties, willProperties);
		if (rc.reasonCode != MQTTCLIENT_SUCCESS)
		{
			rc = MQTTClient_connectURIVersion(handle, options, serverURI, 3, start, millisecsTimeout,
					connectProperties, willProperties);
		}
	}
	else
		rc = MQTTClient_connectURIVersion(handle, options, serverURI, MQTTVersion, start, millisecsTimeout,
				connectProperties, willProperties);

exit:
	FUNC_EXIT_RC(rc.reasonCode);
	return rc;
}

MQTTResponse MQTTClient_connectAll(MQTTClient handle, MQTTClient_connectOptions* options,
		MQTTProperties* connectProperties, MQTTProperties* willProperties);

int MQTTClient_connect(MQTTClient handle, MQTTClient_connectOptions* options)
{
	MQTTClients* m = handle;
	MQTTResponse response;

	if (m->c->MQTTVersion >= MQTTVERSION_5)
		return MQTTCLIENT_WRONG_MQTT_VERSION;

	response = MQTTClient_connectAll(handle, options, NULL, NULL);

	return response.reasonCode;
}


MQTTResponse MQTTClient_connect5(MQTTClient handle, MQTTClient_connectOptions* options,
		MQTTProperties* connectProperties, MQTTProperties* willProperties)
{
	MQTTClients* m = handle;
	MQTTResponse response = MQTTResponse_initializer;

	if (m->c->MQTTVersion < MQTTVERSION_5)
	{
		response.reasonCode = MQTTCLIENT_WRONG_MQTT_VERSION;
		return response;
	}

	return MQTTClient_connectAll(handle, options, connectProperties, willProperties);
}


MQTTResponse MQTTClient_connectAll(MQTTClient handle, MQTTClient_connectOptions* options,
		MQTTProperties* connectProperties, MQTTProperties* willProperties)
{
	MQTTClients* m = handle;
	MQTTResponse rc = MQTTResponse_initializer;

	FUNC_ENTRY;
	Thread_lock_mutex(connect_mutex);
	Thread_lock_mutex(mqttclient_mutex);

	rc.reasonCode = SOCKET_ERROR;
	if (!library_initialized)
	{
		rc.reasonCode = MQTTCLIENT_FAILURE;
		goto exit;
	}

	if (options == NULL)
	{
		rc.reasonCode = MQTTCLIENT_NULL_PARAMETER;
		goto exit;
	}

	if (strncmp(options->struct_id, "MQTC", 4) != 0 || options->struct_version < 0 || options->struct_version > 7)
	{
		rc.reasonCode = MQTTCLIENT_BAD_STRUCTURE;
		goto exit;
	}

#if defined(OPENSSL)
	if (m->ssl && options->ssl == NULL)
	{
		rc.reasonCode = MQTTCLIENT_NULL_PARAMETER;
		goto exit;
	}
#endif

	if (options->will) /* check validity of will options structure */
	{
		if (strncmp(options->will->struct_id, "MQTW", 4) != 0 || (options->will->struct_version != 0 && options->will->struct_version != 1))
		{
			rc.reasonCode = MQTTCLIENT_BAD_STRUCTURE;
			goto exit;
		}
		if (options->will->qos < 0 || options->will->qos > 2)
		{
			rc.reasonCode = MQTTCLIENT_BAD_QOS;
			goto exit;
		}
		if (options->will->topicName == NULL)
		{
			rc.reasonCode = MQTTCLIENT_NULL_PARAMETER;
			goto exit;
		} else if (strlen(options->will->topicName) == 0)
		{
			rc.reasonCode = MQTTCLIENT_0_LEN_WILL_TOPIC;
			goto exit;
		}
	}


#if defined(OPENSSL)
	if (options->struct_version != 0 && options->ssl) /* check validity of SSL options structure */
	{
		if (strncmp(options->ssl->struct_id, "MQTS", 4) != 0 || options->ssl->struct_version < 0 || options->ssl->struct_version > 4)
		{
			rc.reasonCode = MQTTCLIENT_BAD_STRUCTURE;
			goto exit;
		}
	}
#endif

	if ((options->username && !UTF8_validateString(options->username)) ||
		(options->password && !UTF8_validateString(options->password)))
	{
		rc.reasonCode = MQTTCLIENT_BAD_UTF8_STRING;
		goto exit;
	}

	if (options->MQTTVersion != MQTTVERSION_DEFAULT &&
			(options->MQTTVersion < MQTTVERSION_3_1 || options->MQTTVersion > MQTTVERSION_5))
	{
		rc.reasonCode = MQTTCLIENT_BAD_MQTT_VERSION;
		goto exit;
	}

	if (options->MQTTVersion >= MQTTVERSION_5)
	{
		if (options->cleansession != 0)
		{
			rc.reasonCode = MQTTCLIENT_BAD_MQTT_OPTION;
			goto exit;
		}
	}
	else if (options->cleanstart != 0)
	{
		rc.reasonCode = MQTTCLIENT_BAD_MQTT_OPTION;
		goto exit;
	}

	if (options->struct_version < 2 || options->serverURIcount == 0)
	{
		if ( !m )
		{
			rc.reasonCode = MQTTCLIENT_NULL_PARAMETER;
			goto exit;
		}
		rc = MQTTClient_connectURI(handle, options, m->serverURI, connectProperties, willProperties);
	}
	else
	{
		int i;

		for (i = 0; i < options->serverURIcount; ++i)
		{
			char* serverURI = options->serverURIs[i];

			if (strncmp(URI_TCP, serverURI, strlen(URI_TCP)) == 0)
				serverURI += strlen(URI_TCP);
			else if (strncmp(URI_WS, serverURI, strlen(URI_WS)) == 0)
			{
				serverURI += strlen(URI_WS);
				m->websocket = 1;
			}
#if defined(OPENSSL)
			else if (strncmp(URI_SSL, serverURI, strlen(URI_SSL)) == 0)
			{
				serverURI += strlen(URI_SSL);
				m->ssl = 1;
			}
			else if (strncmp(URI_WSS, serverURI, strlen(URI_WSS)) == 0)
			{
				serverURI += strlen(URI_WSS);
				m->ssl = 1;
				m->websocket = 1;
			}
#endif
			rc = MQTTClient_connectURI(handle, options, serverURI, connectProperties, willProperties);
			if (rc.reasonCode == MQTTREASONCODE_SUCCESS)
				break;
		}
	}
	if (rc.reasonCode == MQTTREASONCODE_SUCCESS)
	{
		if (rc.properties && MQTTProperties_hasProperty(rc.properties, MQTTPROPERTY_CODE_RECEIVE_MAXIMUM))
		{
			int recv_max = MQTTProperties_getNumericValue(rc.properties, MQTTPROPERTY_CODE_RECEIVE_MAXIMUM);
			if (m->c->maxInflightMessages > recv_max)
				m->c->maxInflightMessages = recv_max;
		}
	}

exit:
	if (m && m->c && m->c->will)
	{
		if (m->c->will->payload)
			free(m->c->will->payload);
		if (m->c->will->topic)
			free(m->c->will->topic);
		free(m->c->will);
		m->c->will = NULL;
	}
	Thread_unlock_mutex(mqttclient_mutex);
	Thread_unlock_mutex(connect_mutex);
	FUNC_EXIT_RC(rc.reasonCode);
	return rc;
}


/**
 * mqttclient_mutex must be locked when you call this function, if multi threaded
 */
static int MQTTClient_disconnect1(MQTTClient handle, int timeout, int call_connection_lost, int stop,
		enum MQTTReasonCodes reason, MQTTProperties* props)
{
	MQTTClients* m = handle;
	START_TIME_TYPE start;
	int rc = MQTTCLIENT_SUCCESS;
	int was_connected = 0;

	FUNC_ENTRY;
	if (m == NULL || m->c == NULL)
	{
		rc = MQTTCLIENT_FAILURE;
		goto exit;
	}
	was_connected = m->c->connected; /* should be 1 */
	if (m->c->connected != 0)
	{
		start = MQTTTime_start_clock();
		m->c->connect_state = DISCONNECTING; /* indicate disconnecting */
		while (m->c->inboundMsgs->count > 0 || m->c->outboundMsgs->count > 0)
		{ /* wait for all inflight message flows to finish, up to timeout */
			if (MQTTTime_elapsed(start) >= timeout)
				break;
			Thread_unlock_mutex(mqttclient_mutex);
			MQTTClient_yield();
			Thread_lock_mutex(mqttclient_mutex);
		}
	}

	MQTTClient_closeSession(m->c, reason, props);

exit:
	if (stop)
		MQTTClient_stop();
	if (call_connection_lost && m->cl && was_connected)
	{
		Log(TRACE_MIN, -1, "Calling connectionLost for client %s", m->c->clientID);
		Thread_start(connectionLost_call, m);
	}
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * mqttclient_mutex must be locked when you call this function, if multi threaded
 */
static int MQTTClient_disconnect_internal(MQTTClient handle, int timeout)
{
	return MQTTClient_disconnect1(handle, timeout, 1, 1, MQTTREASONCODE_SUCCESS, NULL);
}


/**
 * mqttclient_mutex must be locked when you call this function, if multi threaded
 */
void MQTTProtocol_closeSession(Clients* c, int sendwill)
{
	MQTTClient_disconnect_internal((MQTTClient)c->context, 0);
}


int MQTTClient_disconnect(MQTTClient handle, int timeout)
{
	int rc = 0;

	Thread_lock_mutex(mqttclient_mutex);
	rc = MQTTClient_disconnect1(handle, timeout, 0, 1, MQTTREASONCODE_SUCCESS, NULL);
	Thread_unlock_mutex(mqttclient_mutex);
	return rc;
}


int MQTTClient_disconnect5(MQTTClient handle, int timeout, enum MQTTReasonCodes reason, MQTTProperties* props)
{
	int rc = 0;

	Thread_lock_mutex(mqttclient_mutex);
	rc = MQTTClient_disconnect1(handle, timeout, 0, 1, reason, props);
	Thread_unlock_mutex(mqttclient_mutex);
	return rc;
}


int MQTTClient_isConnected(MQTTClient handle)
{
	MQTTClients* m = handle;
	int rc = 0;

	FUNC_ENTRY;
	Thread_lock_mutex(mqttclient_mutex);
	if (m && m->c)
		rc = m->c->connected;
	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


MQTTResponse MQTTClient_subscribeMany5(MQTTClient handle, int count, char* const* topic,
		int* qos, MQTTSubscribe_options* opts, MQTTProperties* props)
{
	MQTTClients* m = handle;
	List* topics = NULL;
	List* qoss = NULL;
	int i = 0;
	int rc = MQTTCLIENT_FAILURE;
	MQTTResponse resp = MQTTResponse_initializer;
	int msgid = 0;

	FUNC_ENTRY;
	Thread_lock_mutex(subscribe_mutex);
	Thread_lock_mutex(mqttclient_mutex);

	resp.reasonCode = MQTTCLIENT_FAILURE;
	if (m == NULL || m->c == NULL)
	{
		rc = MQTTCLIENT_FAILURE;
		goto exit;
	}
	if (m->c->connected == 0)
	{
		rc = MQTTCLIENT_DISCONNECTED;
		goto exit;
	}
	for (i = 0; i < count; i++)
	{
		if (!UTF8_validateString(topic[i]))
		{
			rc = MQTTCLIENT_BAD_UTF8_STRING;
			goto exit;
		}

		if (qos[i] < 0 || qos[i] > 2)
		{
			rc = MQTTCLIENT_BAD_QOS;
			goto exit;
		}
	}
	if ((msgid = MQTTProtocol_assignMsgId(m->c)) == 0)
	{
		rc = MQTTCLIENT_MAX_MESSAGES_INFLIGHT;
		goto exit;
	}

	topics = ListInitialize();
	qoss = ListInitialize();
	for (i = 0; i < count; i++)
	{
		ListAppend(topics, topic[i], strlen(topic[i]));
		ListAppend(qoss, &qos[i], sizeof(int));
	}

	rc = MQTTProtocol_subscribe(m->c, topics, qoss, msgid, opts, props);
	ListFreeNoContent(topics);
	ListFreeNoContent(qoss);

	if (rc == TCPSOCKET_COMPLETE)
	{
		MQTTPacket* pack = NULL;

		Thread_unlock_mutex(mqttclient_mutex);
		pack = MQTTClient_waitfor(handle, SUBACK, &rc, 10000L);
		Thread_lock_mutex(mqttclient_mutex);
		if (pack != NULL)
		{
			Suback* sub = (Suback*)pack;

			if (m->c->MQTTVersion == MQTTVERSION_5)
			{
				if (sub->properties.count > 0)
				{
					if ((resp.properties = malloc(sizeof(MQTTProperties))) == NULL)
					{
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
					*resp.properties = MQTTProperties_copy(&sub->properties);
				}
				resp.reasonCodeCount = sub->qoss->count;
				resp.reasonCode = *(int*)sub->qoss->first->content;
				if (sub->qoss->count > 1)
				{
					ListElement* current = NULL;
					int rc_count = 0;

					if ((resp.reasonCodes = malloc(sizeof(enum MQTTReasonCodes) * (sub->qoss->count))) == NULL)
					{
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
					while (ListNextElement(sub->qoss, &current))
						(resp.reasonCodes)[rc_count++] = *(enum MQTTReasonCodes*)(current->content);
				}
			}
			else
			{
				ListElement* current = NULL;
				i = 0;
				while (ListNextElement(sub->qoss, &current))
				{
					int* reqqos = (int*)(current->content);
					qos[i++] = *reqqos;
				}
				resp.reasonCode = rc;
			}
			rc = MQTTProtocol_handleSubacks(pack, m->c->net.socket);
			m->pack = NULL;
		}
		else
			rc = SOCKET_ERROR;
	}

	if (rc == SOCKET_ERROR)
		MQTTClient_disconnect_internal(handle, 0);
	else if (rc == TCPSOCKET_COMPLETE)
		rc = MQTTCLIENT_SUCCESS;

exit:
	if (rc < 0)
		resp.reasonCode = rc;
	Thread_unlock_mutex(mqttclient_mutex);
	Thread_unlock_mutex(subscribe_mutex);
	FUNC_EXIT_RC(resp.reasonCode);
	return resp;
}



int MQTTClient_subscribeMany(MQTTClient handle, int count, char* const* topic, int* qos)
{
	MQTTClients* m = handle;
	MQTTResponse response = MQTTResponse_initializer;

	if (m->c->MQTTVersion >= MQTTVERSION_5)
		response.reasonCode = MQTTCLIENT_WRONG_MQTT_VERSION;
	else
		response = MQTTClient_subscribeMany5(handle, count, topic, qos, NULL, NULL);

	return response.reasonCode;
}



MQTTResponse MQTTClient_subscribe5(MQTTClient handle, const char* topic, int qos,
		MQTTSubscribe_options* opts, MQTTProperties* props)
{
	MQTTResponse rc;

	FUNC_ENTRY;
	rc = MQTTClient_subscribeMany5(handle, 1, (char * const *)(&topic), &qos, opts, props);
	if (qos == MQTT_BAD_SUBSCRIBE) /* addition for MQTT 3.1.1 - error code from subscribe */
		rc.reasonCode = MQTT_BAD_SUBSCRIBE;
	FUNC_EXIT_RC(rc.reasonCode);
	return rc;
}


int MQTTClient_subscribe(MQTTClient handle, const char* topic, int qos)
{
	MQTTClients* m = handle;
	MQTTResponse response = MQTTResponse_initializer;

	if (m->c->MQTTVersion >= MQTTVERSION_5)
		response.reasonCode = MQTTCLIENT_WRONG_MQTT_VERSION;
	else
		response = MQTTClient_subscribe5(handle, topic, qos, NULL, NULL);

	return response.reasonCode;
}


MQTTResponse MQTTClient_unsubscribeMany5(MQTTClient handle, int count, char* const* topic, MQTTProperties* props)
{
	MQTTClients* m = handle;
	List* topics = NULL;
	int i = 0;
	int rc = SOCKET_ERROR;
	MQTTResponse resp = MQTTResponse_initializer;
	int msgid = 0;

	FUNC_ENTRY;
	Thread_lock_mutex(unsubscribe_mutex);
	Thread_lock_mutex(mqttclient_mutex);

	resp.reasonCode = MQTTCLIENT_FAILURE;
	if (m == NULL || m->c == NULL)
	{
		rc = MQTTCLIENT_FAILURE;
		goto exit;
	}
	if (m->c->connected == 0)
	{
		rc = MQTTCLIENT_DISCONNECTED;
		goto exit;
	}
	for (i = 0; i < count; i++)
	{
		if (!UTF8_validateString(topic[i]))
		{
			rc = MQTTCLIENT_BAD_UTF8_STRING;
			goto exit;
		}
	}
	if ((msgid = MQTTProtocol_assignMsgId(m->c)) == 0)
	{
		rc = MQTTCLIENT_MAX_MESSAGES_INFLIGHT;
		goto exit;
	}

	topics = ListInitialize();
	for (i = 0; i < count; i++)
		ListAppend(topics, topic[i], strlen(topic[i]));
	rc = MQTTProtocol_unsubscribe(m->c, topics, msgid, props);
	ListFreeNoContent(topics);

	if (rc == TCPSOCKET_COMPLETE)
	{
		MQTTPacket* pack = NULL;

		Thread_unlock_mutex(mqttclient_mutex);
		pack = MQTTClient_waitfor(handle, UNSUBACK, &rc, 10000L);
		Thread_lock_mutex(mqttclient_mutex);
		if (pack != NULL)
		{
			Unsuback* unsub = (Unsuback*)pack;

			if (m->c->MQTTVersion == MQTTVERSION_5)
			{
				if (unsub->properties.count > 0)
				{
					if ((resp.properties = malloc(sizeof(MQTTProperties))) == NULL)
					{
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
					*resp.properties = MQTTProperties_copy(&unsub->properties);
				}
				resp.reasonCodeCount = unsub->reasonCodes->count;
				resp.reasonCode = *(int*)unsub->reasonCodes->first->content;
				if (unsub->reasonCodes->count > 1)
				{
					ListElement* current = NULL;
					int rc_count = 0;

					if ((resp.reasonCodes = malloc(sizeof(enum MQTTReasonCodes) * (unsub->reasonCodes->count))) == NULL)
					{
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
					while (ListNextElement(unsub->reasonCodes, &current))
						(resp.reasonCodes)[rc_count++] = *(enum MQTTReasonCodes*)(current->content);
				}
			}
			else
				resp.reasonCode = rc;
			rc = MQTTProtocol_handleUnsubacks(pack, m->c->net.socket);
			m->pack = NULL;
		}
		else
			rc = SOCKET_ERROR;
	}

	if (rc == SOCKET_ERROR)
		MQTTClient_disconnect_internal(handle, 0);

exit:
	if (rc < 0)
		resp.reasonCode = rc;
	Thread_unlock_mutex(mqttclient_mutex);
	Thread_unlock_mutex(unsubscribe_mutex);
	FUNC_EXIT_RC(resp.reasonCode);
	return resp;
}


int MQTTClient_unsubscribeMany(MQTTClient handle, int count, char* const* topic)
{
	MQTTResponse response = MQTTClient_unsubscribeMany5(handle, count, topic, NULL);

	return response.reasonCode;
}


MQTTResponse MQTTClient_unsubscribe5(MQTTClient handle, const char* topic, MQTTProperties* props)
{
	MQTTResponse rc;

	rc = MQTTClient_unsubscribeMany5(handle, 1, (char * const *)(&topic), props);
	return rc;
}


int MQTTClient_unsubscribe(MQTTClient handle, const char* topic)
{
	MQTTResponse response = MQTTClient_unsubscribe5(handle, topic, NULL);

	return response.reasonCode;
}


MQTTResponse MQTTClient_publish5(MQTTClient handle, const char* topicName, int payloadlen, const void* payload,
		int qos, int retained, MQTTProperties* properties, MQTTClient_deliveryToken* deliveryToken)
{
	int rc = MQTTCLIENT_SUCCESS;
	MQTTClients* m = handle;
	Messages* msg = NULL;
	Publish* p = NULL;
	int blocked = 0;
	int msgid = 0;
	MQTTResponse resp = MQTTResponse_initializer;

	FUNC_ENTRY;
	Thread_lock_mutex(mqttclient_mutex);

	if (m == NULL || m->c == NULL)
		rc = MQTTCLIENT_FAILURE;
	else if (m->c->connected == 0)
		rc = MQTTCLIENT_DISCONNECTED;
	else if (!UTF8_validateString(topicName))
		rc = MQTTCLIENT_BAD_UTF8_STRING;

	if (rc != MQTTCLIENT_SUCCESS)
		goto exit;

	/* If outbound queue is full, block until it is not */
	while (m->c->outboundMsgs->count >= m->c->maxInflightMessages ||
         Socket_noPendingWrites(m->c->net.socket) == 0) /* wait until the socket is free of large packets being written */
	{
		if (blocked == 0)
		{
			blocked = 1;
			Log(TRACE_MIN, -1, "Blocking publish on queue full for client %s", m->c->clientID);
		}
		Thread_unlock_mutex(mqttclient_mutex);
		MQTTClient_yield();
		Thread_lock_mutex(mqttclient_mutex);
		if (m->c->connected == 0)
		{
			rc = MQTTCLIENT_FAILURE;
			goto exit;
		}
	}
	if (blocked == 1)
		Log(TRACE_MIN, -1, "Resuming publish now queue not full for client %s", m->c->clientID);
	if (qos > 0 && (msgid = MQTTProtocol_assignMsgId(m->c)) == 0)
	{	/* this should never happen as we've waited for spaces in the queue */
		rc = MQTTCLIENT_MAX_MESSAGES_INFLIGHT;
		goto exit;
	}

	if ((p = malloc(sizeof(Publish))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit_and_free;
	}
	p->payload = NULL;
	p->payloadlen = payloadlen;
	if (payloadlen > 0)
	{
		if ((p->payload = malloc(payloadlen)) == NULL)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit_and_free;
		}
		memcpy(p->payload, payload, payloadlen);
	}
	if ((p->topic = MQTTStrdup(topicName)) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit_and_free;
	}
	p->msgId = msgid;
	p->MQTTVersion = m->c->MQTTVersion;
	if (m->c->MQTTVersion >= MQTTVERSION_5)
	{
		if (properties)
			p->properties = *properties;
		else
		{
			MQTTProperties props = MQTTProperties_initializer;
			p->properties = props;
		}
	}

	rc = MQTTProtocol_startPublish(m->c, p, qos, retained, &msg);

	/* If the packet was partially written to the socket, wait for it to complete.
	 * However, if the client is disconnected during this time and qos is not 0, still return success, as
	 * the packet has already been written to persistence and assigned a message id so will
	 * be sent when the client next connects.
	 */
	if (rc == TCPSOCKET_INTERRUPTED)
	{
		while (m->c->connected == 1)
		{
			pending_writes* writing = NULL;

			Thread_lock_mutex(socket_mutex);
			writing = SocketBuffer_getWrite(m->c->net.socket);
			Thread_unlock_mutex(socket_mutex);

			if (writing == NULL)
				break;

			Thread_unlock_mutex(mqttclient_mutex);
			MQTTClient_yield();
			Thread_lock_mutex(mqttclient_mutex);
		}
		rc = (qos > 0 || m->c->connected == 1) ? MQTTCLIENT_SUCCESS : MQTTCLIENT_FAILURE;
	}

	if (deliveryToken && qos > 0)
		*deliveryToken = msg->msgid;

exit_and_free:
	if (p)
	{
		if (p->topic)
			free(p->topic);
		if (p->payload)
			free(p->payload);
		free(p);
	}

	if (rc == SOCKET_ERROR)
	{
		MQTTClient_disconnect_internal(handle, 0);
		/* Return success for qos > 0 as the send will be retried automatically */
		rc = (qos > 0) ? MQTTCLIENT_SUCCESS : MQTTCLIENT_FAILURE;
	}

exit:
	Thread_unlock_mutex(mqttclient_mutex);
	resp.reasonCode = rc;
	FUNC_EXIT_RC(resp.reasonCode);
	return resp;
}


int MQTTClient_publish(MQTTClient handle, const char* topicName, int payloadlen, const void* payload,
							 int qos, int retained, MQTTClient_deliveryToken* deliveryToken)
{
	MQTTClients* m = handle;
	MQTTResponse rc = MQTTResponse_initializer;

	if (m->c->MQTTVersion >= MQTTVERSION_5)
		rc.reasonCode = MQTTCLIENT_WRONG_MQTT_VERSION;
	else
		rc = MQTTClient_publish5(handle, topicName, payloadlen, payload, qos, retained, NULL, deliveryToken);
	return rc.reasonCode;
}


MQTTResponse MQTTClient_publishMessage5(MQTTClient handle, const char* topicName, MQTTClient_message* message,
								MQTTClient_deliveryToken* deliveryToken)
{
	MQTTResponse rc = MQTTResponse_initializer;
	MQTTProperties* props = NULL;

	FUNC_ENTRY;
	if (message == NULL)
	{
		rc.reasonCode = MQTTCLIENT_NULL_PARAMETER;
		goto exit;
	}

	if (strncmp(message->struct_id, "MQTM", 4) != 0 ||
			(message->struct_version != 0 && message->struct_version != 1))
	{
		rc.reasonCode = MQTTCLIENT_BAD_STRUCTURE;
		goto exit;
	}

	if (message->struct_version >= 1)
		props = &message->properties;

	rc = MQTTClient_publish5(handle, topicName, message->payloadlen, message->payload,
								message->qos, message->retained, props, deliveryToken);
exit:
	FUNC_EXIT_RC(rc.reasonCode);
	return rc;
}


int MQTTClient_publishMessage(MQTTClient handle, const char* topicName, MQTTClient_message* message,
															 MQTTClient_deliveryToken* deliveryToken)
{
	MQTTClients* m = handle;
	MQTTResponse rc = MQTTResponse_initializer;

	if (strncmp(message->struct_id, "MQTM", 4) != 0 ||
			(message->struct_version != 0 && message->struct_version != 1))
		rc.reasonCode = MQTTCLIENT_BAD_STRUCTURE;
	else if (m->c->MQTTVersion >= MQTTVERSION_5)
		rc.reasonCode = MQTTCLIENT_WRONG_MQTT_VERSION;
	else
		rc = MQTTClient_publishMessage5(handle, topicName, message, deliveryToken);
	return rc.reasonCode;
}


static void MQTTClient_retry(void)
{
	static START_TIME_TYPE last = START_TIME_ZERO;
	START_TIME_TYPE now;

	FUNC_ENTRY;
	now = MQTTTime_now();
	if (MQTTTime_difftime(now, last) > (retryLoopInterval * 1000))
	{
		last = MQTTTime_now();
		MQTTProtocol_keepalive(now);
		MQTTProtocol_retry(now, 1, 0);
	}
	else
		MQTTProtocol_retry(now, 0, 0);
	FUNC_EXIT;
}


static MQTTPacket* MQTTClient_cycle(int* sock, unsigned long timeout, int* rc)
{
	struct timeval tp = {0L, 0L};
	static Ack ack;
	MQTTPacket* pack = NULL;

	FUNC_ENTRY;
	if (timeout > 0L)
	{
		tp.tv_sec = timeout / 1000;
		tp.tv_usec = (timeout % 1000) * 1000; /* this field is microseconds! */
	}

#if defined(OPENSSL)
	if ((*sock = SSLSocket_getPendingRead()) == -1)
	{
		/* 0 from getReadySocket indicates no work to do, -1 == error, but can happen normally */
#endif
		*sock = Socket_getReadySocket(0, &tp, socket_mutex);
#if defined(OPENSSL)
	}
#endif
	Thread_lock_mutex(mqttclient_mutex);
	if (*sock > 0)
	{
		MQTTClients* m = NULL;
		if (ListFindItem(handles, sock, clientSockCompare) != NULL)
			m = (MQTTClient)(handles->current->content);
		if (m != NULL)
		{
			if (m->c->connect_state == TCP_IN_PROGRESS || m->c->connect_state == SSL_IN_PROGRESS)
				*rc = 0;  /* waiting for connect state to clear */
			else if (m->c->connect_state == WEBSOCKET_IN_PROGRESS)
				*rc = WebSocket_upgrade(&m->c->net);
			else
			{
				pack = MQTTPacket_Factory(m->c->MQTTVersion, &m->c->net, rc);
				if (*rc == TCPSOCKET_INTERRUPTED)
					*rc = 0;
			}
		}

		if (pack)
		{
			int freed = 1;

			/* Note that these handle... functions free the packet structure that they are dealing with */
			if (pack->header.bits.type == PUBLISH)
				*rc = MQTTProtocol_handlePublishes(pack, *sock);
			else if (pack->header.bits.type == PUBACK || pack->header.bits.type == PUBCOMP)
			{
				int msgid;

				ack = (pack->header.bits.type == PUBCOMP) ? *(Pubcomp*)pack : *(Puback*)pack;
				msgid = ack.msgId;
				if (m && m->c->MQTTVersion >= MQTTVERSION_5 && m->published)
				{
					Log(TRACE_MIN, -1, "Calling published for client %s, msgid %d", m->c->clientID, msgid);
					(*(m->published))(m->published_context, msgid, pack->header.bits.type, &ack.properties, ack.rc);
				}
				*rc = (pack->header.bits.type == PUBCOMP) ?
					MQTTProtocol_handlePubcomps(pack, *sock) : MQTTProtocol_handlePubacks(pack, *sock);
				if (m && m->dc)
				{
					Log(TRACE_MIN, -1, "Calling deliveryComplete for client %s, msgid %d", m->c->clientID, msgid);
					(*(m->dc))(m->context, msgid);
				}
			}
			else if (pack->header.bits.type == PUBREC)
			{
				Pubrec* pubrec = (Pubrec*)pack;

				if (m && m->c->MQTTVersion >= MQTTVERSION_5 && m->published && pubrec->rc >= MQTTREASONCODE_UNSPECIFIED_ERROR)
				{
					Log(TRACE_MIN, -1, "Calling published for client %s, msgid %d", m->c->clientID, ack.msgId);
					(*(m->published))(m->published_context, pubrec->msgId, pack->header.bits.type,
							&pubrec->properties, pubrec->rc);
				}
				*rc = MQTTProtocol_handlePubrecs(pack, *sock);
			}
			else if (pack->header.bits.type == PUBREL)
				*rc = MQTTProtocol_handlePubrels(pack, *sock);
			else if (pack->header.bits.type == PINGRESP)
				*rc = MQTTProtocol_handlePingresps(pack, *sock);
			else
				freed = 0;
			if (freed)
				pack = NULL;
		}
	}
	MQTTClient_retry();
	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT_RC(*rc);
	return pack;
}


static MQTTPacket* MQTTClient_waitfor(MQTTClient handle, int packet_type, int* rc, long timeout)
{
	MQTTPacket* pack = NULL;
	MQTTClients* m = handle;
	START_TIME_TYPE start = MQTTTime_start_clock();

	FUNC_ENTRY;
	if (((MQTTClients*)handle) == NULL || timeout <= 0L)
	{
		*rc = MQTTCLIENT_FAILURE;
		goto exit;
	}

	if (running)
	{
		if (packet_type == CONNECT)
		{
			if ((*rc = Thread_wait_sem(m->connect_sem, timeout)) == 0)
				*rc = m->rc;
		}
		else if (packet_type == CONNACK)
			*rc = Thread_wait_sem(m->connack_sem, timeout);
		else if (packet_type == SUBACK)
			*rc = Thread_wait_sem(m->suback_sem, timeout);
		else if (packet_type == UNSUBACK)
			*rc = Thread_wait_sem(m->unsuback_sem, timeout);
		if (*rc == 0 && packet_type != CONNECT && m->pack == NULL)
			Log(LOG_ERROR, -1, "waitfor unexpectedly is NULL for client %s, packet_type %d, timeout %ld", m->c->clientID, packet_type, timeout);
		pack = m->pack;
	}
	else
	{
		*rc = TCPSOCKET_COMPLETE;
		while (1)
		{
			int sock = -1;
			pack = MQTTClient_cycle(&sock, 100L, rc);
			if (sock == m->c->net.socket)
			{
				if (*rc == SOCKET_ERROR)
					break;
				if (pack && (pack->header.bits.type == packet_type))
					break;
				if (m->c->connect_state == TCP_IN_PROGRESS)
				{
					int error;
					socklen_t len = sizeof(error);

					if ((*rc = getsockopt(m->c->net.socket, SOL_SOCKET, SO_ERROR, (char*)&error, &len)) == 0)
						*rc = error;
					break;
				}
#if defined(OPENSSL)
				else if (m->c->connect_state == SSL_IN_PROGRESS)
				{

					*rc = m->c->sslopts->struct_version >= 3 ?
						SSLSocket_connect(m->c->net.ssl, sock, m->currentServerURI,
							m->c->sslopts->verify, m->c->sslopts->ssl_error_cb, m->c->sslopts->ssl_error_context) :
						SSLSocket_connect(m->c->net.ssl, sock, m->currentServerURI,
							m->c->sslopts->verify, NULL, NULL);
					if (*rc == SSL_FATAL)
						break;
					else if (*rc == 1) /* rc == 1 means SSL connect has finished and succeeded */
					{
						if ((m->c->cleansession == 0 && m->c->cleanstart == 0) && m->c->session == NULL)
							m->c->session = SSL_get1_session(m->c->net.ssl);
						break;
					}
				}
#endif
				else if (m->c->connect_state == WEBSOCKET_IN_PROGRESS )
				{
					*rc = 1;
					break;
				}
				else if (m->c->connect_state == PROXY_CONNECT_IN_PROGRESS )
				{
					*rc = 1;
					break;
				}
				else if (m->c->connect_state == WAIT_FOR_CONNACK)
				{
					int error;
					socklen_t len = sizeof(error);
					if (getsockopt(m->c->net.socket, SOL_SOCKET, SO_ERROR, (char*)&error, &len) == 0)
					{
						if (error)
						{
							*rc = error;
							break;
						}
					}
				}
			}
			if (MQTTTime_elapsed(start) > timeout)
			{
				pack = NULL;
				break;
			}
		}
	}

exit:
	FUNC_EXIT_RC(*rc);
	return pack;
}


int MQTTClient_receive(MQTTClient handle, char** topicName, int* topicLen, MQTTClient_message** message,
											 unsigned long timeout)
{
	int rc = TCPSOCKET_COMPLETE;
	START_TIME_TYPE start = MQTTTime_start_clock();
	unsigned long elapsed = 0L;
	MQTTClients* m = handle;

	FUNC_ENTRY;
	if (m == NULL || m->c == NULL
			|| running) /* receive is not meant to be called in a multi-thread environment */
	{
		rc = MQTTCLIENT_FAILURE;
		goto exit;
	}
	if (m->c->connected == 0)
	{
		rc = MQTTCLIENT_DISCONNECTED;
		goto exit;
	}

	*topicName = NULL;
	*message = NULL;

	/* if there is already a message waiting, don't hang around but still do some packet handling */
	if (m->c->messageQueue->count > 0)
		timeout = 0L;

	elapsed = MQTTTime_elapsed(start);
	do
	{
		int sock = 0;
		MQTTClient_cycle(&sock, (timeout > elapsed) ? timeout - elapsed : 0L, &rc);

		if (rc == SOCKET_ERROR)
		{
			if (ListFindItem(handles, &sock, clientSockCompare) && 	/* find client corresponding to socket */
			  (MQTTClient)(handles->current->content) == handle)
				break; /* there was an error on the socket we are interested in */
		}
		elapsed = MQTTTime_elapsed(start);
	}
	while (elapsed < timeout && m->c->messageQueue->count == 0);

	if (m->c->messageQueue->count > 0)
		rc = MQTTClient_deliverMessage(rc, m, topicName, topicLen, message);

	if (rc == SOCKET_ERROR)
		MQTTClient_disconnect_internal(handle, 0);

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


void MQTTClient_yield(void)
{
	START_TIME_TYPE start = MQTTTime_start_clock();
	unsigned long elapsed = 0L;
	unsigned long timeout = 100L;
	int rc = 0;

	FUNC_ENTRY;
	if (running) /* yield is not meant to be called in a multi-thread environment */
	{
		MQTTTime_sleep(timeout);
		goto exit;
	}

	elapsed = MQTTTime_elapsed(start);
	do
	{
		int sock = -1;
		MQTTClient_cycle(&sock, (timeout > elapsed) ? timeout - elapsed : 0L, &rc);
		Thread_lock_mutex(mqttclient_mutex);
		if (rc == SOCKET_ERROR && ListFindItem(handles, &sock, clientSockCompare))
		{
			MQTTClients* m = (MQTTClient)(handles->current->content);
			if (m->c->connect_state != DISCONNECTING)
				MQTTClient_disconnect_internal(m, 0);
		}
		Thread_unlock_mutex(mqttclient_mutex);
		elapsed = MQTTTime_elapsed(start);
	}
	while (elapsed < timeout);
exit:
	FUNC_EXIT;
}

/*
static int pubCompare(void* a, void* b)
{
	Messages* msg = (Messages*)a;
	return msg->publish == (Publications*)b;
}*/


int MQTTClient_waitForCompletion(MQTTClient handle, MQTTClient_deliveryToken mdt, unsigned long timeout)
{
	int rc = MQTTCLIENT_FAILURE;
	START_TIME_TYPE start = MQTTTime_start_clock();
	unsigned long elapsed = 0L;
	MQTTClients* m = handle;

	FUNC_ENTRY;
	Thread_lock_mutex(mqttclient_mutex);

	if (m == NULL || m->c == NULL)
	{
		rc = MQTTCLIENT_FAILURE;
		goto exit;
	}

	elapsed = MQTTTime_elapsed(start);
	while (elapsed < timeout)
	{
		if (m->c->connected == 0)
		{
			rc = MQTTCLIENT_DISCONNECTED;
			goto exit;
		}
		if (ListFindItem(m->c->outboundMsgs, &mdt, messageIDCompare) == NULL)
		{
			rc = MQTTCLIENT_SUCCESS; /* well we couldn't find it */
			goto exit;
		}
		Thread_unlock_mutex(mqttclient_mutex);
		MQTTClient_yield();
		Thread_lock_mutex(mqttclient_mutex);
		elapsed = MQTTTime_elapsed(start);
	}

exit:
	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTClient_getPendingDeliveryTokens(MQTTClient handle, MQTTClient_deliveryToken **tokens)
{
	int rc = MQTTCLIENT_SUCCESS;
	MQTTClients* m = handle;
	*tokens = NULL;

	FUNC_ENTRY;
	Thread_lock_mutex(mqttclient_mutex);

	if (m == NULL)
	{
		rc = MQTTCLIENT_FAILURE;
		goto exit;
	}

	if (m->c && m->c->outboundMsgs->count > 0)
	{
		ListElement* current = NULL;
		int count = 0;

		*tokens = malloc(sizeof(MQTTClient_deliveryToken) * (m->c->outboundMsgs->count + 1));
		if (!*tokens)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		while (ListNextElement(m->c->outboundMsgs, &current))
		{
			Messages* m = (Messages*)(current->content);
			(*tokens)[count++] = m->msgid;
		}
		(*tokens)[count] = -1;
	}

exit:
	Thread_unlock_mutex(mqttclient_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


void MQTTClient_setTraceLevel(enum MQTTCLIENT_TRACE_LEVELS level)
{
	Log_setTraceLevel((enum LOG_LEVELS)level);
}


void MQTTClient_setTraceCallback(MQTTClient_traceCallback* callback)
{
	Log_setTraceCallback((Log_traceCallback*)callback);
}


MQTTClient_nameValue* MQTTClient_getVersionInfo(void)
{
	#define MAX_INFO_STRINGS 8
	static MQTTClient_nameValue libinfo[MAX_INFO_STRINGS + 1];
	int i = 0;

	libinfo[i].name = "Product name";
	libinfo[i++].value = "Eclipse Paho Synchronous MQTT C Client Library";

	libinfo[i].name = "Version";
	libinfo[i++].value = CLIENT_VERSION;

	libinfo[i].name = "Build level";
	libinfo[i++].value = BUILD_TIMESTAMP;
#if defined(OPENSSL)
	libinfo[i].name = "OpenSSL version";
	libinfo[i++].value = SSLeay_version(SSLEAY_VERSION);

	libinfo[i].name = "OpenSSL flags";
	libinfo[i++].value = SSLeay_version(SSLEAY_CFLAGS);

	libinfo[i].name = "OpenSSL build timestamp";
	libinfo[i++].value = SSLeay_version(SSLEAY_BUILT_ON);

	libinfo[i].name = "OpenSSL platform";
	libinfo[i++].value = SSLeay_version(SSLEAY_PLATFORM);

	libinfo[i].name = "OpenSSL directory";
	libinfo[i++].value = SSLeay_version(SSLEAY_DIR);
#endif
	libinfo[i].name = NULL;
	libinfo[i].value = NULL;
	return libinfo;
}


const char* MQTTClient_strerror(int code)
{
  static char buf[30];

  switch (code) {
    case MQTTCLIENT_SUCCESS:
      return "Success";
    case MQTTCLIENT_FAILURE:
      return "Failure";
    case MQTTCLIENT_DISCONNECTED:
      return "Disconnected";
    case MQTTCLIENT_MAX_MESSAGES_INFLIGHT:
      return "Maximum in-flight messages amount reached";
    case MQTTCLIENT_BAD_UTF8_STRING:
      return "Invalid UTF8 string";
    case MQTTCLIENT_NULL_PARAMETER:
      return "Invalid (NULL) parameter";
    case MQTTCLIENT_TOPICNAME_TRUNCATED:
      return "Topic containing NULL characters has been truncated";
    case MQTTCLIENT_BAD_STRUCTURE:
      return "Bad structure";
    case MQTTCLIENT_BAD_QOS:
      return "Invalid QoS value";
    case MQTTCLIENT_SSL_NOT_SUPPORTED:
      return "SSL is not supported";
    case MQTTCLIENT_BAD_MQTT_VERSION:
      return "Unrecognized MQTT version";
    case MQTTCLIENT_BAD_PROTOCOL:
      return "Invalid protocol scheme";
    case MQTTCLIENT_BAD_MQTT_OPTION:
      return "Options for wrong MQTT version";
    case MQTTCLIENT_WRONG_MQTT_VERSION:
      return "Client created for another version of MQTT";
    case MQTTCLIENT_0_LEN_WILL_TOPIC:
      return "Zero length will topic on connect";
  }

  sprintf(buf, "Unknown error code %d", code);
  return buf;
}


/**
 * See if any pending writes have been completed, and cleanup if so.
 * Cleaning up means removing any publication data that was stored because the write did
 * not originally complete.
 */
static void MQTTProtocol_checkPendingWrites(void)
{
	FUNC_ENTRY;
	if (state.pending_writes.count > 0)
	{
		ListElement* le = state.pending_writes.first;
		while (le)
		{
			if (Socket_noPendingWrites(((pending_write*)(le->content))->socket))
			{
				MQTTProtocol_removePublication(((pending_write*)(le->content))->p);
				state.pending_writes.current = le;
				ListRemove(&(state.pending_writes), le->content); /* does NextElement itself */
				le = state.pending_writes.current;
			}
			else
				ListNextElement(&(state.pending_writes), &le);
		}
	}
	FUNC_EXIT;
}


static void MQTTClient_writeComplete(int socket, int rc)
{
	ListElement* found = NULL;

	FUNC_ENTRY;
	/* a partial write is now complete for a socket - this will be on a publish*/

	MQTTProtocol_checkPendingWrites();

	/* find the client using this socket */
	if ((found = ListFindItem(handles, &socket, clientSockCompare)) != NULL)
	{
		MQTTClients* m = (MQTTClients*)(found->content);

		m->c->net.lastSent = MQTTTime_now();
	}
	FUNC_EXIT;
}
