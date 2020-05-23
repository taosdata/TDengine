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
 *    Ian Craggs - initial implementation and documentation
 *    Ian Craggs, Allan Stockdill-Mander - SSL support
 *    Ian Craggs - multiple server connection support
 *    Ian Craggs - fix for bug 413429 - connectionLost not called
 *    Ian Craggs - fix for bug 415042 - using already freed structure
 *    Ian Craggs - fix for bug 419233 - mutexes not reporting errors
 *    Ian Craggs - fix for bug 420851
 *    Ian Craggs - fix for bug 432903 - queue persistence
 *    Ian Craggs - MQTT 3.1.1 support
 *    Rong Xiang, Ian Craggs - C++ compatibility
 *    Ian Craggs - fix for bug 442400: reconnecting after network cable unplugged
 *    Ian Craggs - fix for bug 444934 - incorrect free in freeCommand1
 *    Ian Craggs - fix for bug 445891 - assigning msgid is not thread safe
 *    Ian Craggs - fix for bug 465369 - longer latency than expected
 *    Ian Craggs - fix for bug 444103 - success/failure callbacks not invoked
 *    Ian Craggs - fix for bug 484363 - segfault in getReadySocket
 *    Ian Craggs - automatic reconnect and offline buffering (send while disconnected)
 *    Ian Craggs - fix for bug 472250
 *    Ian Craggs - fix for bug 486548
 *    Ian Craggs - SNI support
 *    Ian Craggs - auto reconnect timing fix #218
 *    Ian Craggs - fix for issue #190
 *    Ian Craggs - check for NULL SSL options #334
 *    Ian Craggs - allocate username/password buffers #431
 *    Ian Craggs - MQTT 5.0 support
 *******************************************************************************/

/**
 * @file
 * \brief Asynchronous API implementation
 *
 */

#include <stdlib.h>
#include <string.h>
#if !defined(_WIN32) && !defined(_WIN64)
	#include <sys/time.h>
#endif

#if !defined(NO_PERSISTENCE)
#include "MQTTPersistence.h"
#endif
#include "MQTTAsync.h"
#include "utf-8.h"
#include "MQTTProtocol.h"
#include "MQTTProtocolOut.h"
#include "Thread.h"
#include "SocketBuffer.h"
#include "StackTrace.h"
#include "Heap.h"
#include "OsWrapper.h"
#include "WebSocket.h"

#define URI_TCP "tcp://"
#define URI_WS  "ws://"
#define URI_WSS "wss://"

#include "VersionInfo.h"

const char *client_timestamp_eye = "MQTTAsyncV3_Timestamp " BUILD_TIMESTAMP;
const char *client_version_eye = "MQTTAsyncV3_Version " CLIENT_VERSION;

// global objects init declaration
int MQTTAsync_init(void);

void MQTTAsync_global_init(MQTTAsync_init_options* inits)
{
	MQTTAsync_init();
#if defined(OPENSSL)
	SSLSocket_handleOpensslInit(inits->do_openssl_init);
#endif
}

#if !defined(min)
#define min(a, b) (((a) < (b)) ? (a) : (b))
#endif

#if defined(WIN32) || defined(WIN64)
void MQTTAsync_init_rand(void)
{
	START_TIME_TYPE now = MQTTTime_start_clock();
	srand(now);
}
#elif defined(AIX)
void MQTTAsync_init_rand(void)
{
	START_TIME_TYPE now = MQTTTime_start_clock();
	srand(now.tv_nsec);
}
#else
void MQTTAsync_init_rand(void)
{
	START_TIME_TYPE now = MQTTTime_start_clock();
	srand(now.tv_usec);
}
#endif

static ClientStates ClientState =
{
	CLIENT_VERSION, /* version */
	NULL /* client list */
};

ClientStates* bstate = &ClientState;

MQTTProtocol state;

enum MQTTAsync_threadStates
{
	STOPPED, STARTING, RUNNING, STOPPING
};

enum MQTTAsync_threadStates sendThread_state = STOPPED;
enum MQTTAsync_threadStates receiveThread_state = STOPPED;
static thread_id_type sendThread_id = 0,
					receiveThread_id = 0;

#if defined(_WIN32) || defined(_WIN64)
static mutex_type mqttasync_mutex = NULL;
static mutex_type socket_mutex = NULL;
static mutex_type mqttcommand_mutex = NULL;
static sem_type send_sem = NULL;
#if !defined(NO_HEAP_TRACKING)
extern mutex_type stack_mutex;
extern mutex_type heap_mutex;
#endif
extern mutex_type log_mutex;

int MQTTAsync_init(void)
{
	DWORD rc = 0;

	if (mqttasync_mutex == NULL)
	{
		if ((mqttasync_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("mqttasync_mutex error %d\n", rc);
			goto exit;
		}
		if ((mqttcommand_mutex = CreateMutex(NULL, 0, NULL)) == NULL)
		{
			rc = GetLastError();
			printf("mqttcommand_mutex error %d\n", rc);
			goto exit;
		}
		if ((send_sem = CreateEvent(
				NULL,               /* default security attributes */
				FALSE,              /* manual-reset event? */
				FALSE,              /* initial state is nonsignaled */
				NULL                /* object name */
				)) == NULL)
		{
			rc = GetLastError();
			printf("send_sem error %d\n", rc);
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
	else
	{
		Log(TRACE_MAX, -1, "Library already initialized");
	}
exit:
	return rc;
}

void MQTTAsync_cleanup(void)
{
	if (send_sem)
		CloseHandle(send_sem);
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
	if (mqttasync_mutex)
		CloseHandle(mqttasync_mutex);
}

#if defined(PAHO_MQTT_STATIC)
static INIT_ONCE g_InitOnce = INIT_ONCE_STATIC_INIT; /* Global for one time initialization */

/* This runs at most once */
BOOL CALLBACK InitMutexesOnce (
    PINIT_ONCE InitOnce,        /* Pointer to one-time initialization structure */
    PVOID Parameter,            /* Optional parameter */
    PVOID *lpContext)           /* Return data, if any */
{
	int rc = MQTTAsync_init();
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
			MQTTAsync_init();
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
				MQTTAsync_cleanup();
		break;
	}
	return TRUE;
}
#endif


#else
static pthread_mutex_t mqttasync_mutex_store = PTHREAD_MUTEX_INITIALIZER;
static mutex_type mqttasync_mutex = &mqttasync_mutex_store;

static pthread_mutex_t socket_mutex_store = PTHREAD_MUTEX_INITIALIZER;
static mutex_type socket_mutex = &socket_mutex_store;

static pthread_mutex_t mqttcommand_mutex_store = PTHREAD_MUTEX_INITIALIZER;
static mutex_type mqttcommand_mutex = &mqttcommand_mutex_store;

static cond_type_struct send_cond_store = { PTHREAD_COND_INITIALIZER, PTHREAD_MUTEX_INITIALIZER };
static cond_type send_cond = &send_cond_store;

int MQTTAsync_init(void)
{
	pthread_mutexattr_t attr;
	int rc;

	pthread_mutexattr_init(&attr);
#if !defined(_WRS_KERNEL)
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#else
	/* #warning "no pthread_mutexattr_settype" */
#endif
	if ((rc = pthread_mutex_init(mqttasync_mutex, &attr)) != 0)
		printf("MQTTAsync: error %d initializing async_mutex\n", rc);
	else if ((rc = pthread_mutex_init(mqttcommand_mutex, &attr)) != 0)
		printf("MQTTAsync: error %d initializing command_mutex\n", rc);
	else if ((rc = pthread_mutex_init(socket_mutex, &attr)) != 0)
		printf("MQTTClient: error %d initializing socket_mutex\n", rc);
	else if ((rc = pthread_cond_init(&send_cond->cond, NULL)) != 0)
		printf("MQTTAsync: error %d initializing send_cond cond\n", rc);
	else if ((rc = pthread_mutex_init(&send_cond->mutex, &attr)) != 0)
		printf("MQTTAsync: error %d initializing send_cond mutex\n", rc);

	return rc;
}

#define WINAPI
#endif

static volatile int global_initialized = 0;
static List* handles = NULL;
static int tostop = 0;
static List* commands = NULL;

typedef struct
{
	MQTTAsync_message* msg;
	char* topicName;
	int topicLen;
	unsigned int seqno; /* only used on restore */
} qEntry;

typedef struct
{
	int type;
	MQTTAsync_onSuccess* onSuccess;
	MQTTAsync_onFailure* onFailure;
	MQTTAsync_onSuccess5* onSuccess5;
	MQTTAsync_onFailure5* onFailure5;
	MQTTAsync_token token;
	void* context;
	START_TIME_TYPE start_time;
	MQTTProperties properties;
	union
	{
		struct
		{
			int count;
			char** topics;
			int* qoss;
			MQTTSubscribe_options opts;
			MQTTSubscribe_options* optlist;
		} sub;
		struct
		{
			int count;
			char** topics;
		} unsub;
		struct
		{
			char* destinationName;
			int payloadlen;
			void* payload;
			int qos;
			int retained;
		} pub;
		struct
		{
			int internal;
			int timeout;
			enum MQTTReasonCodes reasonCode;
		} dis;
		struct
		{
			int currentURI;
			int MQTTVersion; /**< current MQTT version being used to connect */
		} conn;
	} details;
} MQTTAsync_command;


typedef struct MQTTAsync_struct
{
	char* serverURI;
	int ssl;
	int websocket;
	Clients* c;

	/* "Global", to the client, callback definitions */
	MQTTAsync_connectionLost* cl;
	MQTTAsync_messageArrived* ma;
	MQTTAsync_deliveryComplete* dc;
	void* clContext; /* the context to be associated with the conn lost callback*/
	void* maContext; /* the context to be associated with the msg arrived callback*/
	void* dcContext; /* the context to be associated with the deliv complete callback*/

	MQTTAsync_connected* connected;
	void* connected_context; /* the context to be associated with the connected callback*/

	MQTTAsync_disconnected* disconnected;
	void* disconnected_context; /* the context to be associated with the disconnected callback*/

	/* Each time connect is called, we store the options that were used.  These are reused in
	   any call to reconnect, or an automatic reconnect attempt */
	MQTTAsync_command connect;		/* Connect operation properties */
	MQTTAsync_command disconnect;		/* Disconnect operation properties */
	MQTTAsync_command* pending_write;       /* Is there a socket write pending? */

	List* responses;
	unsigned int command_seqno;

	MQTTPacket* pack;

	/* added for offline buffering */
	MQTTAsync_createOptions* createOptions;
	int shouldBeConnected;

	/* added for automatic reconnect */
	int automaticReconnect;
	int minRetryInterval;
	int maxRetryInterval;
	int serverURIcount;
	char** serverURIs;
	int connectTimeout;

	int currentInterval;
	int currentIntervalBase;
	START_TIME_TYPE lastConnectionFailedTime;
	int retrying;
	int reconnectNow;

	/* MQTT V5 properties */
	MQTTProperties* connectProps;
	MQTTProperties* willProps;

} MQTTAsyncs;


typedef struct
{
	MQTTAsync_command command;
	MQTTAsyncs* client;
	unsigned int seqno; /* only used on restore */
} MQTTAsync_queuedCommand;


static int clientSockCompare(void* a, void* b);
static void MQTTAsync_lock_mutex(mutex_type amutex);
static void MQTTAsync_unlock_mutex(mutex_type amutex);
static int MQTTAsync_checkConn(MQTTAsync_command* command, MQTTAsyncs* client);
static void MQTTAsync_terminate(void);
#if !defined(NO_PERSISTENCE)
static int MQTTAsync_unpersistCommand(MQTTAsync_queuedCommand* qcmd);
static int MQTTAsync_persistCommand(MQTTAsync_queuedCommand* qcmd);
static MQTTAsync_queuedCommand* MQTTAsync_restoreCommand(char* buffer, int buflen, int MQTTVersion);
/*static void MQTTAsync_insertInOrder(List* list, void* content, int size);*/
static int MQTTAsync_restoreCommands(MQTTAsyncs* client);
#endif
static int MQTTAsync_addCommand(MQTTAsync_queuedCommand* command, int command_size);
static void MQTTAsync_startConnectRetry(MQTTAsyncs* m);
static void MQTTAsync_checkDisconnect(MQTTAsync handle, MQTTAsync_command* command);
static void MQTTProtocol_checkPendingWrites(void);
static void MQTTAsync_freeServerURIs(MQTTAsyncs* m);
static void MQTTAsync_freeCommand1(MQTTAsync_queuedCommand *command);
static void MQTTAsync_freeCommand(MQTTAsync_queuedCommand *command);
static void MQTTAsync_writeComplete(int socket, int rc);
static int MQTTAsync_processCommand(void);
static void MQTTAsync_checkTimeouts(void);
static thread_return_type WINAPI MQTTAsync_sendThread(void* n);
static void MQTTAsync_emptyMessageQueue(Clients* client);
static void MQTTAsync_removeResponsesAndCommands(MQTTAsyncs* m);
static int MQTTAsync_completeConnection(MQTTAsyncs* m, Connack* connack);
static thread_return_type WINAPI MQTTAsync_receiveThread(void* n);
static void MQTTAsync_stop(void);
static void MQTTAsync_closeOnly(Clients* client, enum MQTTReasonCodes reasonCode, MQTTProperties* props);
static void MQTTAsync_closeSession(Clients* client, enum MQTTReasonCodes reasonCode, MQTTProperties* props);
static int clientStructCompare(void* a, void* b);
static int MQTTAsync_cleanSession(Clients* client);
static int MQTTAsync_deliverMessage(MQTTAsyncs* m, char* topicName, size_t topicLen, MQTTAsync_message* mm);
static int MQTTAsync_disconnect1(MQTTAsync handle, const MQTTAsync_disconnectOptions* options, int internal);
static int MQTTAsync_disconnect_internal(MQTTAsync handle, int timeout);
static int cmdMessageIDCompare(void* a, void* b);
static int MQTTAsync_assignMsgId(MQTTAsyncs* m);
static int MQTTAsync_countBufferedMessages(MQTTAsyncs* m);
static void MQTTAsync_retry(void);
static int MQTTAsync_connecting(MQTTAsyncs* m);
static MQTTPacket* MQTTAsync_cycle(int* sock, unsigned long timeout, int* rc);
/*static int pubCompare(void* a, void* b);*/


void MQTTAsync_sleep(long milliseconds)
{
	FUNC_ENTRY;
#if defined(_WIN32) || defined(_WIN64)
	Sleep(milliseconds);
#else
	usleep(milliseconds*1000);
#endif
	FUNC_EXIT;
}


/* Add random amount of jitter for exponential backoff on retry
   Jitter value will be +/- 20% of "base" interval, including max interval
   https://www.awsarchitectureblog.com/2015/03/backoff.html
   http://ee.lbl.gov/papers/sync_94.pdf */
int MQTTAsync_randomJitter(int currentIntervalBase, int minInterval, int maxInterval)
{
	const int max_sleep = (int)(min(maxInterval, currentIntervalBase) * 1.2); // (e.g. 72 if base > 60)
	const int min_sleep = (int)(max(minInterval, currentIntervalBase) / 1.2); // (e.g. 48 if base > 60)

	if (min_sleep >= max_sleep) // shouldn't happen, but just in case
	{
		return min_sleep;
	}

	{
		/* random_between(min_sleep, max_sleep)
		  http://stackoverflow.com/questions/2509679/how-to-generate-a-random-number-from-within-a-range */
		int r;
		int range = max_sleep - min_sleep + 1;
		const int buckets = RAND_MAX / range;
		const int limit = buckets * range;

		/* Create equal size buckets all in a row, then fire randomly towards
		 * the buckets until you land in one of them. All buckets are equally
		 * likely. If you land off the end of the line of buckets, try again. */
		do
		{
			r = rand();
		} while (r >= limit);

		{
			const int randResult = r / buckets;
			return min_sleep + randResult;
		}
	}
}


/**
 * List callback function for comparing clients by socket
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
static int clientSockCompare(void* a, void* b)
{
	MQTTAsyncs* m = (MQTTAsyncs*)a;
	return m->c->net.socket == *(int*)b;
}


static void MQTTAsync_lock_mutex(mutex_type amutex)
{
	int rc = Thread_lock_mutex(amutex);
	if (rc != 0)
		Log(LOG_ERROR, 0, "Error %s locking mutex", strerror(rc));
}


static void MQTTAsync_unlock_mutex(mutex_type amutex)
{
	int rc = Thread_unlock_mutex(amutex);
	if (rc != 0)
		Log(LOG_ERROR, 0, "Error %s unlocking mutex", strerror(rc));
}


/*
  Check whether there are any more connect options.  If not then we are finished
  with connect attempts.
*/
static int MQTTAsync_checkConn(MQTTAsync_command* command, MQTTAsyncs* client)
{
	int rc;

	FUNC_ENTRY;
	rc = command->details.conn.currentURI + 1 < client->serverURIcount ||
		(command->details.conn.MQTTVersion == 4 && client->c->MQTTVersion == MQTTVERSION_DEFAULT);
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTAsync_createWithOptions(MQTTAsync* handle, const char* serverURI, const char* clientId,
		int persistence_type, void* persistence_context,  MQTTAsync_createOptions* options)
{
	int rc = 0;
	MQTTAsyncs *m = NULL;

#if (defined(_WIN32) || defined(_WIN64)) && defined(PAHO_MQTT_STATIC)
	 /* intializes mutexes once.  Must come before FUNC_ENTRY */
	BOOL bStatus = InitOnceExecuteOnce(&g_InitOnce, InitMutexesOnce, NULL, NULL);
#endif
	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (serverURI == NULL || clientId == NULL)
	{
		rc = MQTTASYNC_NULL_PARAMETER;
		goto exit;
	}

	if (!UTF8_validateString(clientId))
	{
		rc = MQTTASYNC_BAD_UTF8_STRING;
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
			rc = MQTTASYNC_BAD_PROTOCOL;
			goto exit;
		}
	}

	if (options && (strncmp(options->struct_id, "MQCO", 4) != 0 ||
					options->struct_version < 0 || options->struct_version > 1))
	{
		rc = MQTTASYNC_BAD_STRUCTURE;
		goto exit;
	}

	if (!global_initialized)
	{
		#if !defined(NO_HEAP_TRACKING)
			Heap_initialize();
		#endif
		Log_initialize((Log_nameValue*)MQTTAsync_getVersionInfo());
		bstate->clients = ListInitialize();
		Socket_outInitialize();
		Socket_setWriteCompleteCallback(MQTTAsync_writeComplete);
		handles = ListInitialize();
		commands = ListInitialize();
#if defined(OPENSSL)
		SSLSocket_initialize();
#endif
		global_initialized = 1;
	}
	if ((m = malloc(sizeof(MQTTAsyncs))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	*handle = m;
	memset(m, '\0', sizeof(MQTTAsyncs));
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
	if ((m->serverURI = MQTTStrdup(serverURI)) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	m->responses = ListInitialize();
	ListAppend(handles, m, sizeof(MQTTAsyncs));

	if ((m->c = malloc(sizeof(Clients))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	memset(m->c, '\0', sizeof(Clients));
	m->c->context = m;
	m->c->outboundMsgs = ListInitialize();
	m->c->inboundMsgs = ListInitialize();
	m->c->messageQueue = ListInitialize();
	m->c->clientID = MQTTStrdup(clientId);
	if (m->c->context == NULL || m->c->outboundMsgs == NULL || m->c->inboundMsgs == NULL ||
			m->c->messageQueue == NULL || m->c->clientID == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	m->c->MQTTVersion = MQTTVERSION_DEFAULT;

	m->shouldBeConnected = 0;
	if (options)
	{
		if ((m->createOptions = malloc(sizeof(MQTTAsync_createOptions))) == NULL)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		memcpy(m->createOptions, options, sizeof(MQTTAsync_createOptions));
		if (options->struct_version > 0)
			m->c->MQTTVersion = options->MQTTVersion;
	}

#if !defined(NO_PERSISTENCE)
	rc = MQTTPersistence_create(&(m->c->persistence), persistence_type, persistence_context);
	if (rc == 0)
	{
		rc = MQTTPersistence_initialize(m->c, m->serverURI);
		if (rc == 0)
		{
			MQTTAsync_restoreCommands(m);
			MQTTPersistence_restoreMessageQueue(m->c);
		}
	}
#endif
	ListAppend(bstate->clients, m->c, sizeof(Clients) + 3*sizeof(List));

exit:
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTAsync_create(MQTTAsync* handle, const char* serverURI, const char* clientId,
		int persistence_type, void* persistence_context)
{
	MQTTAsync_init_rand();

	return MQTTAsync_createWithOptions(handle, serverURI, clientId, persistence_type,
		persistence_context, NULL);
}


static void MQTTAsync_terminate(void)
{
	FUNC_ENTRY;
	MQTTAsync_stop();
	if (global_initialized)
	{
		ListElement* elem = NULL;
		ListFree(bstate->clients);
		ListFree(handles);
		while (ListNextElement(commands, &elem))
			MQTTAsync_freeCommand1((MQTTAsync_queuedCommand*)(elem->content));
		ListFree(commands);
		handles = NULL;
		WebSocket_terminate();
		#if !defined(NO_HEAP_TRACKING)
			Heap_terminate();
		#endif
		Log_terminate();
		global_initialized = 0;
	}
	FUNC_EXIT;
}


#if !defined(NO_PERSISTENCE)
static int MQTTAsync_unpersistCommand(MQTTAsync_queuedCommand* qcmd)
{
	int rc = 0;
	char key[PERSISTENCE_MAX_KEY_LENGTH + 1];

	FUNC_ENTRY;
	if (qcmd->client->c->MQTTVersion >= MQTTVERSION_5)
		sprintf(key, "%s%u", PERSISTENCE_V5_COMMAND_KEY, qcmd->seqno);
	else
		sprintf(key, "%s%u", PERSISTENCE_COMMAND_KEY, qcmd->seqno);
	if ((rc = qcmd->client->c->persistence->premove(qcmd->client->c->phandle, key)) != 0)
		Log(LOG_ERROR, 0, "Error %d removing command from persistence", rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


static int MQTTAsync_persistCommand(MQTTAsync_queuedCommand* qcmd)
{
	int rc = 0;
	MQTTAsyncs* aclient = qcmd->client;
	MQTTAsync_command* command = &qcmd->command;
	int* lens = NULL;
	void** bufs = NULL;
	int bufindex = 0, i, nbufs = 0;
	char key[PERSISTENCE_MAX_KEY_LENGTH + 1];
	int props_allocated = 0;
	int process = 1;

	FUNC_ENTRY;
	switch (command->type)
	{
		case SUBSCRIBE:
			nbufs = ((aclient->c->MQTTVersion >= MQTTVERSION_5) ? 4 : 3) +
				(command->details.sub.count * 2);

			if (((lens = (int*)malloc(nbufs * sizeof(int))) == NULL) ||
					((bufs = malloc(nbufs * sizeof(char *))) == NULL))
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
			bufs[bufindex] = &command->type;
			lens[bufindex++] = sizeof(command->type);

			bufs[bufindex] = &command->token;
			lens[bufindex++] = sizeof(command->token);

			bufs[bufindex] = &command->details.sub.count;
			lens[bufindex++] = sizeof(command->details.sub.count);

			for (i = 0; i < command->details.sub.count; ++i)
			{
				bufs[bufindex] = command->details.sub.topics[i];
				lens[bufindex++] = (int)strlen(command->details.sub.topics[i]) + 1;

				if (aclient->c->MQTTVersion < MQTTVERSION_5)
				{
					bufs[bufindex] = &command->details.sub.qoss[i];
					lens[bufindex++] = sizeof(command->details.sub.qoss[i]);
				}
				else
				{
					if (command->details.sub.count == 1)
					{
						bufs[bufindex] = &command->details.sub.opts;
						lens[bufindex++] = sizeof(command->details.sub.opts);
					}
					else
					{
						bufs[bufindex] = &command->details.sub.optlist[i];
						lens[bufindex++] = sizeof(command->details.sub.optlist[i]);
					}
				}
			}
			break;

		case UNSUBSCRIBE:
			nbufs = ((aclient->c->MQTTVersion >= MQTTVERSION_5) ? 4 : 3) +
					command->details.unsub.count;

			if (((lens = (int*)malloc(nbufs * sizeof(int))) == NULL) ||
					((bufs = malloc(nbufs * sizeof(char *))) == NULL))
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}

			bufs[bufindex] = &command->type;
			lens[bufindex++] = sizeof(command->type);

			bufs[bufindex] = &command->token;
			lens[bufindex++] = sizeof(command->token);

			bufs[bufindex] = &command->details.unsub.count;
			lens[bufindex++] = sizeof(command->details.unsub.count);

			for (i = 0; i < command->details.unsub.count; ++i)
			{
				bufs[bufindex] = command->details.unsub.topics[i];
				lens[bufindex++] = (int)strlen(command->details.unsub.topics[i]) + 1;
			}
			break;

		case PUBLISH:
			nbufs = (aclient->c->MQTTVersion >= MQTTVERSION_5) ? 8 : 7;

			if (((lens = (int*)malloc(nbufs * sizeof(int))) == NULL) ||
					((bufs = malloc(nbufs * sizeof(char *))) == NULL))
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}

			bufs[bufindex] = &command->type;
			lens[bufindex++] = sizeof(command->type);

			bufs[bufindex] = &command->token;
			lens[bufindex++] = sizeof(command->token);

			bufs[bufindex] = command->details.pub.destinationName;
			lens[bufindex++] = (int)strlen(command->details.pub.destinationName) + 1;

			bufs[bufindex] = &command->details.pub.payloadlen;
			lens[bufindex++] = sizeof(command->details.pub.payloadlen);

			bufs[bufindex] = command->details.pub.payload;
			lens[bufindex++] = command->details.pub.payloadlen;

			bufs[bufindex] = &command->details.pub.qos;
			lens[bufindex++] = sizeof(command->details.pub.qos);

			bufs[bufindex] = &command->details.pub.retained;
			lens[bufindex++] = sizeof(command->details.pub.retained);
			break;

		default:
			process = 0;
			break;
	}
	if (aclient->c->MQTTVersion >= MQTTVERSION_5 && process) 	/* persist properties */
	{
		int temp_len = 0;
		char* ptr = NULL;

		temp_len = MQTTProperties_len(&command->properties);
		if ((ptr = bufs[bufindex] = malloc(temp_len)) == NULL)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		props_allocated = bufindex;
		rc = MQTTProperties_write(&ptr, &command->properties);
		lens[bufindex++] = temp_len;
		sprintf(key, "%s%u", PERSISTENCE_V5_COMMAND_KEY, ++aclient->command_seqno);
	}
	else
		sprintf(key, "%s%u", PERSISTENCE_COMMAND_KEY, ++aclient->command_seqno);

	if (nbufs > 0)
	{
		if ((rc = aclient->c->persistence->pput(aclient->c->phandle, key, nbufs, (char**)bufs, lens)) != 0)
			Log(LOG_ERROR, 0, "Error persisting command, rc %d", rc);
		qcmd->seqno = aclient->command_seqno;
	}
exit:
	if (props_allocated > 0)
		free(bufs[props_allocated]);
	if (lens)
		free(lens);
	if (bufs)
		free(bufs);
	FUNC_EXIT_RC(rc);
	return rc;
}


static MQTTAsync_queuedCommand* MQTTAsync_restoreCommand(char* buffer, int buflen, int MQTTVersion)
{
	MQTTAsync_command* command = NULL;
	MQTTAsync_queuedCommand* qcommand = NULL;
	char* ptr = buffer;
	int i;
	size_t data_size;

	FUNC_ENTRY;
	if ((qcommand = malloc(sizeof(MQTTAsync_queuedCommand))) == NULL)
		goto exit;
	memset(qcommand, '\0', sizeof(MQTTAsync_queuedCommand));
	command = &qcommand->command;

	command->type = *(int*)ptr;
	ptr += sizeof(int);

	command->token = *(MQTTAsync_token*)ptr;
	ptr += sizeof(MQTTAsync_token);

	switch (command->type)
	{
		case SUBSCRIBE:
			command->details.sub.count = *(int*)ptr;
			ptr += sizeof(int);

			if (command->details.sub.count > 0)
			{
				if ((command->details.sub.topics = (char **)malloc(sizeof(char *) * command->details.sub.count)) == NULL)
				{
					free(qcommand);
					qcommand = NULL;
					goto exit;
				}
				if (MQTTVersion < MQTTVERSION_5)
				{
					if ((command->details.sub.qoss = (int *)malloc(sizeof(int) * command->details.sub.count)) == NULL)
					{
						free(qcommand);
						qcommand = NULL;
						goto exit;
					}
				}
				else if (command->details.sub.count > 1)
				{
					command->details.sub.optlist = (MQTTSubscribe_options*)malloc(sizeof(MQTTSubscribe_options) * command->details.sub.count);
					if (command->details.sub.optlist == NULL)
					{
						free(qcommand);
						qcommand = NULL;
						goto exit;
					}
				}
			}

			for (i = 0; i < command->details.sub.count; ++i)
			{
				data_size = strlen(ptr) + 1;

				if ((command->details.sub.topics[i] = malloc(data_size)) == NULL)
				{
					free(qcommand);
					qcommand = NULL;
					goto exit;
				}
				strcpy(command->details.sub.topics[i], ptr);
				ptr += data_size;

				if (MQTTVersion < MQTTVERSION_5)
				{
					command->details.sub.qoss[i] = *(int*)ptr;
					ptr += sizeof(int);
				}
				else
				{
					if (command->details.sub.count == 1)
					{
						command->details.sub.opts = *(MQTTSubscribe_options*)ptr;
						ptr += sizeof(MQTTSubscribe_options);
					}
					else
					{
						command->details.sub.optlist[i] = *(MQTTSubscribe_options*)ptr;
						ptr += sizeof(MQTTSubscribe_options);
					}
				}
			}
			break;

		case UNSUBSCRIBE:
			command->details.unsub.count = *(int*)ptr;
			ptr += sizeof(int);

			if (command->details.unsub.count > 0)
			{
				command->details.unsub.topics = (char **)malloc(sizeof(char *) * command->details.unsub.count);
				if (command->details.unsub.topics == NULL)
				{
					free(qcommand);
					qcommand = NULL;
					goto exit;
				}
			}

			for (i = 0; i < command->details.unsub.count; ++i)
			{
				data_size = strlen(ptr) + 1;

				if ((command->details.unsub.topics[i] = malloc(data_size)) == NULL)
				{
					free(qcommand);
					qcommand = NULL;
					goto exit;
				}
				strcpy(command->details.unsub.topics[i], ptr);
				ptr += data_size;
			}
			break;

		case PUBLISH:
			data_size = strlen(ptr) + 1;
			if ((command->details.pub.destinationName = malloc(data_size)) == NULL)
			{
				free(qcommand);
				qcommand = NULL;
				goto exit;
			}
			strcpy(command->details.pub.destinationName, ptr);
			ptr += data_size;

			command->details.pub.payloadlen = *(int*)ptr;
			ptr += sizeof(int);

			data_size = command->details.pub.payloadlen;
			if ((command->details.pub.payload = malloc(data_size)) == NULL)
			{
				free(qcommand);
				qcommand = NULL;
				goto exit;
			}
			memcpy(command->details.pub.payload, ptr, data_size);
			ptr += data_size;

			command->details.pub.qos = *(int*)ptr;
			ptr += sizeof(int);

			command->details.pub.retained = *(int*)ptr;
			ptr += sizeof(int);
			break;

		default:
			free(qcommand);
			qcommand = NULL;

	}
	if (qcommand != NULL && MQTTVersion >= MQTTVERSION_5 &&
			MQTTProperties_read(&command->properties, &ptr, buffer + buflen) != 1)
	{
			Log(LOG_ERROR, -1, "Error restoring properties from persistence");
			free(qcommand);
			qcommand = NULL;
	}
exit:
	FUNC_EXIT;
	return qcommand;
}

/*
static void MQTTAsync_insertInOrder(List* list, void* content, int size)
{
	ListElement* index = NULL;
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(list, &current) != NULL && index == NULL)
	{
		if (((MQTTAsync_queuedCommand*)content)->seqno < ((MQTTAsync_queuedCommand*)current->content)->seqno)
			index = current;
	}

	ListInsert(list, content, size, index);
	FUNC_EXIT;
}*/


static int MQTTAsync_restoreCommands(MQTTAsyncs* client)
{
	int rc = 0;
	char **msgkeys;
	int nkeys;
	int i = 0;
	Clients* c = client->c;
	int commands_restored = 0;

	FUNC_ENTRY;
	if (c->persistence && (rc = c->persistence->pkeys(c->phandle, &msgkeys, &nkeys)) == 0)
	{
		while (rc == 0 && i < nkeys)
		{
			char *buffer = NULL;
			int buflen;

			if (strncmp(msgkeys[i], PERSISTENCE_COMMAND_KEY, strlen(PERSISTENCE_COMMAND_KEY)) != 0 &&
				strncmp(msgkeys[i], PERSISTENCE_V5_COMMAND_KEY, strlen(PERSISTENCE_V5_COMMAND_KEY)) != 0)
			{
				;
			}
			else if ((rc = c->persistence->pget(c->phandle, msgkeys[i], &buffer, &buflen)) == 0)
			{
				int MQTTVersion =
					(strncmp(msgkeys[i], PERSISTENCE_V5_COMMAND_KEY, strlen(PERSISTENCE_V5_COMMAND_KEY)) == 0)
					? MQTTVERSION_5 : MQTTVERSION_3_1_1;
				MQTTAsync_queuedCommand* cmd = MQTTAsync_restoreCommand(buffer, buflen, MQTTVersion);

				if (cmd)
				{
					cmd->client = client;
					cmd->seqno = atoi(strchr(msgkeys[i], '-')+1); /* key format is tag'-'seqno */
					MQTTPersistence_insertInOrder(commands, cmd, sizeof(MQTTAsync_queuedCommand));
					free(buffer);
					client->command_seqno = max(client->command_seqno, cmd->seqno);
					commands_restored++;
				}
			}
			if (msgkeys[i])
				free(msgkeys[i]);
			i++;
		}
		if (msgkeys != NULL)
			free(msgkeys);
	}
	Log(TRACE_MINIMUM, -1, "%d commands restored for client %s", commands_restored, c->clientID);
	FUNC_EXIT_RC(rc);
	return rc;
}
#endif


static int MQTTAsync_addCommand(MQTTAsync_queuedCommand* command, int command_size)
{
	int rc = MQTTASYNC_SUCCESS;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttcommand_mutex);
	/* Don't set start time if the connect command is already in process #218 */
	if ((command->command.type != CONNECT) || (command->client->c->connect_state == NOT_IN_PROGRESS))
		command->command.start_time = MQTTTime_start_clock();
	if (command->command.type == CONNECT ||
		(command->command.type == DISCONNECT && command->command.details.dis.internal))
	{
		MQTTAsync_queuedCommand* head = NULL;

		if (commands->first)
			head = (MQTTAsync_queuedCommand*)(commands->first->content);

		if (head != NULL && head->client == command->client && head->command.type == command->command.type)
			MQTTAsync_freeCommand(command); /* ignore duplicate connect or disconnect command */
		else
			ListInsert(commands, command, command_size, commands->first); /* add to the head of the list */
	}
	else
	{
		ListAppend(commands, command, command_size);
#if !defined(NO_PERSISTENCE)
		if (command->client->c->persistence)
			MQTTAsync_persistCommand(command);
#endif
	}
	MQTTAsync_unlock_mutex(mqttcommand_mutex);
#if !defined(_WIN32) && !defined(_WIN64)
	rc = Thread_signal_cond(send_cond);
	if (rc != 0)
		Log(LOG_ERROR, 0, "Error %d from signal cond", rc);
#else
	rc = Thread_post_sem(send_sem);
#endif
	FUNC_EXIT_RC(rc);
	return rc;
}


static void MQTTAsync_startConnectRetry(MQTTAsyncs* m)
{
	if (m->automaticReconnect && m->shouldBeConnected)
	{
		m->lastConnectionFailedTime = MQTTTime_start_clock();
		if (m->retrying)
		{
			m->currentIntervalBase = min(m->currentIntervalBase * 2, m->maxRetryInterval);
		}
		else
		{
			m->currentIntervalBase = m->minRetryInterval;
			m->retrying = 1;
		}
		m->currentInterval = MQTTAsync_randomJitter(m->currentIntervalBase, m->minRetryInterval, m->maxRetryInterval);
	}
}


int MQTTAsync_reconnect(MQTTAsync handle)
{
	int rc = MQTTASYNC_FAILURE;
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m->automaticReconnect)
	{
		if (m->shouldBeConnected)
		{
			m->reconnectNow = 1;
			if (m->retrying == 0)
			{
				m->currentIntervalBase = m->minRetryInterval;
				m->currentInterval = m->minRetryInterval;
				m->retrying = 1;
			}
			rc = MQTTASYNC_SUCCESS;
		}
	}
	else
	{
		/* to reconnect, put the connect command to the head of the command queue */
		MQTTAsync_queuedCommand* conn = malloc(sizeof(MQTTAsync_queuedCommand));
		if (!conn)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		memset(conn, '\0', sizeof(MQTTAsync_queuedCommand));
		conn->client = m;
		conn->command = m->connect;
		/* make sure that the version attempts are restarted */
		if (m->c->MQTTVersion == MQTTVERSION_DEFAULT)
			conn->command.details.conn.MQTTVersion = 0;
		rc = MQTTAsync_addCommand(conn, sizeof(m->connect));
	}

exit:
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


static void MQTTAsync_checkDisconnect(MQTTAsync handle, MQTTAsync_command* command)
{
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	/* wait for all inflight message flows to finish, up to timeout */;
	if (m->c->outboundMsgs->count == 0 || MQTTTime_elapsed(command->start_time) >= command->details.dis.timeout)
	{
		int was_connected = m->c->connected;
		MQTTAsync_closeSession(m->c, command->details.dis.reasonCode, &command->properties);
		if (command->details.dis.internal)
		{
			if (m->cl && was_connected)
			{
				Log(TRACE_MIN, -1, "Calling connectionLost for client %s", m->c->clientID);
				(*(m->cl))(m->clContext, NULL);
			}
			MQTTAsync_startConnectRetry(m);
		}
		else if (command->onSuccess)
		{
			MQTTAsync_successData data;

			memset(&data, '\0', sizeof(data));
			Log(TRACE_MIN, -1, "Calling disconnect complete for client %s", m->c->clientID);
			(*(command->onSuccess))(command->context, &data);
		}
		else if (command->onSuccess5)
		{
			MQTTAsync_successData5 data = MQTTAsync_successData5_initializer;

			data.reasonCode = MQTTASYNC_SUCCESS;
			Log(TRACE_MIN, -1, "Calling disconnect complete for client %s", m->c->clientID);
			(*(command->onSuccess5))(command->context, &data);
		}
	}
	FUNC_EXIT;
}

/**
 * Call Socket_noPendingWrites(int socket) with protection by socket_mutex, see https://github.com/eclipse/paho.mqtt.c/issues/385
 */
static int MQTTAsync_Socket_noPendingWrites(int socket)
{
    int rc;
    Thread_lock_mutex(socket_mutex);
    rc = Socket_noPendingWrites(socket);
    Thread_unlock_mutex(socket_mutex);
    return rc;
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


static void MQTTAsync_freeServerURIs(MQTTAsyncs* m)
{
	int i;

	for (i = 0; i < m->serverURIcount; ++i)
		free(m->serverURIs[i]);
	m->serverURIcount = 0;
	if (m->serverURIs)
		free(m->serverURIs);
	m->serverURIs = NULL;
}


static void MQTTAsync_freeCommand1(MQTTAsync_queuedCommand *command)
{
	if (command->command.type == SUBSCRIBE)
	{
		int i;

		for (i = 0; i < command->command.details.sub.count; i++)
			free(command->command.details.sub.topics[i]);

		free(command->command.details.sub.topics);
		command->command.details.sub.topics = NULL;
		free(command->command.details.sub.qoss);
		command->command.details.sub.qoss = NULL;
	}
	else if (command->command.type == UNSUBSCRIBE)
	{
		int i;

		for (i = 0; i < command->command.details.unsub.count; i++)
			free(command->command.details.unsub.topics[i]);

		free(command->command.details.unsub.topics);
		command->command.details.unsub.topics = NULL;
	}
	else if (command->command.type == PUBLISH)
	{
		/* qos 1 and 2 topics are freed in the protocol code when the flows are completed */
		if (command->command.details.pub.destinationName)
			free(command->command.details.pub.destinationName);
		command->command.details.pub.destinationName = NULL;
		if (command->command.details.pub.payload)
			free(command->command.details.pub.payload);
		command->command.details.pub.payload = NULL;
	}
	MQTTProperties_free(&command->command.properties);
}

static void MQTTAsync_freeCommand(MQTTAsync_queuedCommand *command)
{
	MQTTAsync_freeCommand1(command);
	free(command);
}


static void MQTTAsync_writeComplete(int socket, int rc)
{
	ListElement* found = NULL;

	FUNC_ENTRY;
	/* a partial write is now complete for a socket - this will be on a publish*/

	MQTTProtocol_checkPendingWrites();

	/* find the client using this socket */
	if ((found = ListFindItem(handles, &socket, clientSockCompare)) != NULL)
	{
		MQTTAsyncs* m = (MQTTAsyncs*)(found->content);

		m->c->net.lastSent = MQTTTime_now();

		/* see if there is a pending write flagged */
		if (m->pending_write)
		{
			ListElement* cur_response = NULL;
			MQTTAsync_command* command = m->pending_write;
			MQTTAsync_queuedCommand* com = NULL;

			cur_response = NULL;
			while (ListNextElement(m->responses, &cur_response))
			{
				com = (MQTTAsync_queuedCommand*)(cur_response->content);
				if (&com->command == m->pending_write)
					break;
			}

			if (cur_response) /* we found a response */
			{
				if (command->type == PUBLISH)
				{
					if (rc == 1 && command->details.pub.qos == 0)
					{
						if (command->onSuccess)
						{
							MQTTAsync_successData data;

							data.token = command->token;
							data.alt.pub.destinationName = command->details.pub.destinationName;
							data.alt.pub.message.payload = command->details.pub.payload;
							data.alt.pub.message.payloadlen = command->details.pub.payloadlen;
							data.alt.pub.message.qos = command->details.pub.qos;
							data.alt.pub.message.retained = command->details.pub.retained;
							Log(TRACE_MIN, -1, "Calling publish success for client %s", m->c->clientID);
							(*(command->onSuccess))(command->context, &data);
						}
						else if (command->onSuccess5)
						{
							MQTTAsync_successData5 data = MQTTAsync_successData5_initializer;

							data.token = command->token;
							data.alt.pub.destinationName = command->details.pub.destinationName;
							data.alt.pub.message.payload = command->details.pub.payload;
							data.alt.pub.message.payloadlen = command->details.pub.payloadlen;
							data.alt.pub.message.qos = command->details.pub.qos;
							data.alt.pub.message.retained = command->details.pub.retained;
							data.properties = command->properties;
							Log(TRACE_MIN, -1, "Calling publish success for client %s", m->c->clientID);
							(*(command->onSuccess5))(command->context, &data);
						}
					}
					else if (rc == -1)
					{
						if (command->onFailure)
						{
							MQTTAsync_failureData data;

							data.token = command->token;
							data.code = rc;
							data.message = NULL;
							Log(TRACE_MIN, -1, "Calling publish failure for client %s", m->c->clientID);
							(*(command->onFailure))(command->context, &data);
						}
						else if (command->onFailure5)
						{
							MQTTAsync_failureData5 data;

							data.token = command->token;
							data.code = rc;
							data.message = NULL;
							data.packet_type = PUBLISH;
							Log(TRACE_MIN, -1, "Calling publish failure for client %s", m->c->clientID);
							(*(command->onFailure5))(command->context, &data);
						}
					}
					else
						com = NULL; /* Don't delete response we haven't acknowledged */
				}
				if (com)
				{
					Log(TRACE_PROTOCOL, -1, "writeComplete: Removing response for msgid %d", com->command.token);
					ListDetach(m->responses, com);
					MQTTAsync_freeCommand(com);
				}
			} /* if cur_response */
			m->pending_write = NULL;
		} /* if pending_write */
	}
	FUNC_EXIT;
}


static int MQTTAsync_processCommand(void)
{
	int rc = 0;
	MQTTAsync_queuedCommand* command = NULL;
	ListElement* cur_command = NULL;
	List* ignored_clients = NULL;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);
	MQTTAsync_lock_mutex(mqttcommand_mutex);

	/* only the first command in the list must be processed for any particular client, so if we skip
	   a command for a client, we must skip all following commands for that client.  Use a list of
	   ignored clients to keep track
	*/
	ignored_clients = ListInitialize();

	/* don't try a command until there isn't a pending write for that client, and we are not connecting */
	while (ListNextElement(commands, &cur_command))
	{
		MQTTAsync_queuedCommand* cmd = (MQTTAsync_queuedCommand*)(cur_command->content);

		if (ListFind(ignored_clients, cmd->client))
			continue;

		if (cmd->command.type == CONNECT || cmd->command.type == DISCONNECT || (cmd->client->c->connected &&
			cmd->client->c->connect_state == NOT_IN_PROGRESS && MQTTAsync_Socket_noPendingWrites(cmd->client->c->net.socket)))
		{
			if ((cmd->command.type == PUBLISH || cmd->command.type == SUBSCRIBE || cmd->command.type == UNSUBSCRIBE) &&
				cmd->client->c->outboundMsgs->count >= MAX_MSG_ID - 1)
			{
				; /* no more message ids available */
			}
			else if (cmd->client->c->MQTTVersion >= MQTTVERSION_5 &&
				((cmd->command.type == PUBLISH && cmd->command.details.pub.qos > 0) ||
						cmd->command.type == SUBSCRIBE || cmd->command.type == UNSUBSCRIBE) &&
				(cmd->client->c->outboundMsgs->count >= cmd->client->c->maxInflightMessages))
			{
				Log(TRACE_MIN, -1, "Blocking on server receive maximum for client %s",
						cmd->client->c->clientID); /* flow control */
			}
			else
			{
				command = cmd;
				break;
			}
		}
		ListAppend(ignored_clients, cmd->client, sizeof(cmd->client));
	}
	ListFreeNoContent(ignored_clients);
	if (command)
	{
		ListDetach(commands, command);
#if !defined(NO_PERSISTENCE)
		if (command->client->c->persistence)
			MQTTAsync_unpersistCommand(command);
#endif
	}
	MQTTAsync_unlock_mutex(mqttcommand_mutex);

	if (!command)
		goto exit; /* nothing to do */

	if (command->command.type == CONNECT)
	{
		if (command->client->c->connect_state != NOT_IN_PROGRESS || command->client->c->connected)
			rc = 0;
		else
		{
			char* serverURI = command->client->serverURI;

			if (command->client->serverURIcount > 0)
			{
				if (command->command.details.conn.currentURI < command->client->serverURIcount)
				{
					serverURI = command->client->serverURIs[command->command.details.conn.currentURI];

					if (strncmp(URI_TCP, serverURI, strlen(URI_TCP)) == 0)
						serverURI += strlen(URI_TCP);
					else if (strncmp(URI_WS, serverURI, strlen(URI_WS)) == 0)
					{
						serverURI += strlen(URI_WS);
						command->client->websocket = 1;
					}
#if defined(OPENSSL)
					else if (strncmp(URI_SSL, serverURI, strlen(URI_SSL)) == 0)
					{
						serverURI += strlen(URI_SSL);
						command->client->ssl = 1;
					}
					else if (strncmp(URI_WSS, serverURI, strlen(URI_WSS)) == 0)
					{
						serverURI += strlen(URI_WSS);
						command->client->ssl = 1;
						command->client->websocket = 1;
					}
#endif
				}
			}

			if (command->client->c->MQTTVersion == MQTTVERSION_DEFAULT)
			{
				if (command->command.details.conn.MQTTVersion == MQTTVERSION_DEFAULT)
					command->command.details.conn.MQTTVersion = MQTTVERSION_3_1_1;
				else if (command->command.details.conn.MQTTVersion == MQTTVERSION_3_1_1)
					command->command.details.conn.MQTTVersion = MQTTVERSION_3_1;
			}
			else
				command->command.details.conn.MQTTVersion = command->client->c->MQTTVersion;

			Log(TRACE_PROTOCOL, -1, "Connecting to serverURI %s with MQTT version %d", serverURI, command->command.details.conn.MQTTVersion);
#if defined(OPENSSL)
#if defined(__GNUC__) && defined(__linux__)
			rc = MQTTProtocol_connect(serverURI, command->client->c, command->client->ssl, command->client->websocket,
					command->command.details.conn.MQTTVersion, command->client->connectProps, command->client->willProps, 100);
#else
			rc = MQTTProtocol_connect(serverURI, command->client->c, command->client->ssl, command->client->websocket,
					command->command.details.conn.MQTTVersion, command->client->connectProps, command->client->willProps);
#endif
#else
#if defined(__GNUC__) && defined(__linux__)
			rc = MQTTProtocol_connect(serverURI, command->client->c, command->client->websocket,
					command->command.details.conn.MQTTVersion, command->client->connectProps, command->client->willProps, 100);
#else
			rc = MQTTProtocol_connect(serverURI, command->client->c, command->client->websocket,
					command->command.details.conn.MQTTVersion, command->client->connectProps, command->client->willProps);
#endif
#endif

			if (command->client->c->connect_state == NOT_IN_PROGRESS)
				rc = SOCKET_ERROR;

			/* if the TCP connect is pending, then we must call select to determine when the connect has completed,
			which is indicated by the socket being ready *either* for reading *or* writing.  The next couple of lines
			make sure we check for writeability as well as readability, otherwise we wait around longer than we need to
			in Socket_getReadySocket() */
			if (rc == EINPROGRESS)
				Socket_addPendingWrite(command->client->c->net.socket);
		}
	}
	else if (command->command.type == SUBSCRIBE)
	{
		List* topics = ListInitialize();
		List* qoss = ListInitialize();
		MQTTProperties* props = NULL;
		MQTTSubscribe_options* subopts = NULL;
		int i;

		for (i = 0; i < command->command.details.sub.count; i++)
		{
			ListAppend(topics, command->command.details.sub.topics[i], strlen(command->command.details.sub.topics[i]));
			ListAppend(qoss, &command->command.details.sub.qoss[i], sizeof(int));
		}
		if (command->client->c->MQTTVersion >= MQTTVERSION_5)
		{
			props = &command->command.properties;
			if (command->command.details.sub.count > 1)
				subopts = command->command.details.sub.optlist;
			else
				subopts = &command->command.details.sub.opts;
		}
		rc = MQTTProtocol_subscribe(command->client->c, topics, qoss, command->command.token, subopts, props);
		ListFreeNoContent(topics);
		ListFreeNoContent(qoss);
		if (command->client->c->MQTTVersion >= MQTTVERSION_5 && command->command.details.sub.count > 1)
			free(command->command.details.sub.optlist);
	}
	else if (command->command.type == UNSUBSCRIBE)
	{
		List* topics = ListInitialize();
		MQTTProperties* props = NULL;
		int i;

		for (i = 0; i < command->command.details.unsub.count; i++)
			ListAppend(topics, command->command.details.unsub.topics[i], strlen(command->command.details.unsub.topics[i]));

		if (command->client->c->MQTTVersion >= MQTTVERSION_5)
			props = &command->command.properties;

		rc = MQTTProtocol_unsubscribe(command->client->c, topics, command->command.token, props);
		ListFreeNoContent(topics);
	}
	else if (command->command.type == PUBLISH)
	{
		Messages* msg = NULL;
		Publish* p = NULL;
		MQTTProperties initialized = MQTTProperties_initializer;

		if ((p = malloc(sizeof(Publish))) == NULL)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}

		p->payload = command->command.details.pub.payload;
		p->payloadlen = command->command.details.pub.payloadlen;
		p->topic = command->command.details.pub.destinationName;
		p->msgId = command->command.token;
		p->MQTTVersion = command->client->c->MQTTVersion;
		p->properties = initialized;
		if (p->MQTTVersion >= MQTTVERSION_5)
			p->properties = command->command.properties;

		rc = MQTTProtocol_startPublish(command->client->c, p, command->command.details.pub.qos, command->command.details.pub.retained, &msg);

		if (command->command.details.pub.qos == 0)
		{
			if (rc == TCPSOCKET_COMPLETE)
			{
				if (command->command.onSuccess)
				{
					MQTTAsync_successData data;

					data.token = command->command.token;
					data.alt.pub.destinationName = command->command.details.pub.destinationName;
					data.alt.pub.message.payload = command->command.details.pub.payload;
					data.alt.pub.message.payloadlen = command->command.details.pub.payloadlen;
					data.alt.pub.message.qos = command->command.details.pub.qos;
					data.alt.pub.message.retained = command->command.details.pub.retained;
					Log(TRACE_MIN, -1, "Calling publish success for client %s", command->client->c->clientID);
					(*(command->command.onSuccess))(command->command.context, &data);
				}
				else if (command->command.onSuccess5)
				{
					MQTTAsync_successData5 data = MQTTAsync_successData5_initializer;

					data.token = command->command.token;
					data.alt.pub.destinationName = command->command.details.pub.destinationName;
					data.alt.pub.message.payload = command->command.details.pub.payload;
					data.alt.pub.message.payloadlen = command->command.details.pub.payloadlen;
					data.alt.pub.message.qos = command->command.details.pub.qos;
					data.alt.pub.message.retained = command->command.details.pub.retained;
					data.properties = command->command.properties;
					Log(TRACE_MIN, -1, "Calling publish success for client %s", command->client->c->clientID);
					(*(command->command.onSuccess5))(command->command.context, &data);
				}
			}
			else
			{
				if (rc != SOCKET_ERROR)
				  command->command.details.pub.destinationName = NULL; /* this will be freed by the protocol code */
				command->client->pending_write = &command->command;
			}
		}
		else
		{
			command->command.details.pub.payload = NULL; /* this will be freed by the protocol code */
			command->command.details.pub.destinationName = NULL; /* this will be freed by the protocol code */
		}
		free(p); /* should this be done if the write isn't complete? */
	}
	else if (command->command.type == DISCONNECT)
	{
		if (command->client->c->connect_state != NOT_IN_PROGRESS || command->client->c->connected != 0)
		{
			if (command->client->c->connect_state != NOT_IN_PROGRESS)
			{
				command->client->c->connect_state = DISCONNECTING;
				if (command->client->connect.onFailure)
				{
					MQTTAsync_failureData data;

					data.token = 0;
					data.code = MQTTASYNC_OPERATION_INCOMPLETE;
					data.message = NULL;
					Log(TRACE_MIN, -1, "Calling connect failure for client %s", command->client->c->clientID);
					(*(command->client->connect.onFailure))(command->client->connect.context, &data);
				}
				else if (command->client->connect.onFailure5)
				{
					MQTTAsync_failureData5 data;

					data.token = 0;
					data.code = MQTTASYNC_OPERATION_INCOMPLETE;
					data.message = NULL;
					Log(TRACE_MIN, -1, "Calling connect failure for client %s", command->client->c->clientID);
					(*(command->client->connect.onFailure5))(command->client->connect.context, &data);
				}
			}
			MQTTAsync_checkDisconnect(command->client, &command->command);
		}
	}

	if (command->command.type == CONNECT && rc != SOCKET_ERROR && rc != MQTTASYNC_PERSISTENCE_ERROR)
	{
		command->client->connect = command->command;
		MQTTAsync_freeCommand(command);
	}
	else if (command->command.type == DISCONNECT)
	{
		command->client->disconnect = command->command;
		MQTTAsync_freeCommand(command);
	}
	else if (command->command.type == PUBLISH && command->command.details.pub.qos == 0 &&
			rc != SOCKET_ERROR && rc != MQTTASYNC_PERSISTENCE_ERROR)
	{
		if (rc == TCPSOCKET_INTERRUPTED)
			ListAppend(command->client->responses, command, sizeof(command));
		else
			MQTTAsync_freeCommand(command);
	}
	else if (rc == SOCKET_ERROR || rc == MQTTASYNC_PERSISTENCE_ERROR)
	{
		if (command->command.type == CONNECT)
		{
			MQTTAsync_disconnectOptions opts = MQTTAsync_disconnectOptions_initializer;
			MQTTAsync_disconnect(command->client, &opts); /* not "internal" because we don't want to call connection lost */
			command->client->shouldBeConnected = 1; /* as above call is not "internal" we need to reset this */
		}
		else
			MQTTAsync_disconnect_internal(command->client, 0);

		if (command->command.type == CONNECT
				&& MQTTAsync_checkConn(&command->command, command->client))
		{
			Log(TRACE_MIN, -1, "Connect failed, more to try");

			if (command->client->c->MQTTVersion == MQTTVERSION_DEFAULT)
			{
				if (command->command.details.conn.MQTTVersion == MQTTVERSION_3_1)
				{
					command->command.details.conn.currentURI++;
					command->command.details.conn.MQTTVersion = 	MQTTVERSION_DEFAULT;
				}
			} else
				command->command.details.conn.currentURI++; /* Here currentURI becomes larger than command->client->serverURIcount. This needs to be handled to avoid segmentation faults! */

			/* put the connect command back to the head of the command queue, using the next serverURI */
			rc = MQTTAsync_addCommand(command,
					sizeof(command->command.details.conn));
		} else
		{
			if (command->command.onFailure)
			{
				MQTTAsync_failureData data;

				data.token = 0;
				data.code = rc;
				data.message = NULL;
				Log(TRACE_MIN, -1, "Calling command failure for client %s", command->client->c->clientID);
				(*(command->command.onFailure))(command->command.context, &data);
			}
			else if (command->command.onFailure5)
			{
				MQTTAsync_failureData5 data = MQTTAsync_failureData5_initializer;

				data.code = rc;
				Log(TRACE_MIN, -1, "Calling command failure for client %s", command->client->c->clientID);
				(*(command->command.onFailure5))(command->command.context, &data);
			}
			if (command->command.type == CONNECT)
			{
				command->client->connect = command->command;
				MQTTAsync_startConnectRetry(command->client);
			}
			MQTTAsync_freeCommand(command);  /* free up the command if necessary */
		}
	}
	else /* put the command into a waiting for response queue for each client, indexed by msgid */
		ListAppend(command->client->responses, command, sizeof(command));

exit:
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	rc = (command != NULL);
	FUNC_EXIT_RC(rc);
	return rc;
}


static void nextOrClose(MQTTAsyncs* m, int rc, char* message)
{
	FUNC_ENTRY;

	if (MQTTAsync_checkConn(&m->connect, m))
	{
		MQTTAsync_queuedCommand* conn;

		MQTTAsync_closeOnly(m->c, MQTTREASONCODE_SUCCESS, NULL);
		/* put the connect command back to the head of the command queue, using the next serverURI */
		if ((conn = malloc(sizeof(MQTTAsync_queuedCommand))) == NULL)
			goto exit;
		memset(conn, '\0', sizeof(MQTTAsync_queuedCommand));
		conn->client = m;
		conn->command = m->connect;
		Log(TRACE_MIN, -1, "Connect failed, more to try");

		if (conn->client->c->MQTTVersion == MQTTVERSION_DEFAULT)
		{
			if (conn->command.details.conn.MQTTVersion == MQTTVERSION_3_1)
			{
				conn->command.details.conn.currentURI++;
				conn->command.details.conn.MQTTVersion = MQTTVERSION_DEFAULT;
			}
		}
		else
			conn->command.details.conn.currentURI++;

		MQTTAsync_addCommand(conn, sizeof(m->connect));
	}
	else
	{
		MQTTAsync_closeSession(m->c, MQTTREASONCODE_SUCCESS, NULL);
		if (m->connect.onFailure)
		{
			MQTTAsync_failureData data;

			data.token = 0;
			data.code = rc;
			data.message = message;
			Log(TRACE_MIN, -1, "Calling connect failure for client %s", m->c->clientID);
			(*(m->connect.onFailure))(m->connect.context, &data);
		}
		else if (m->connect.onFailure5)
		{
			MQTTAsync_failureData5 data = MQTTAsync_failureData5_initializer;

			data.token = 0;
			data.code = rc;
			data.message = message;
			Log(TRACE_MIN, -1, "Calling connect failure for client %s", m->c->clientID);
			(*(m->connect.onFailure5))(m->connect.context, &data);
		}
		MQTTAsync_startConnectRetry(m);
	}
exit:
	FUNC_EXIT;
}


static void MQTTAsync_checkTimeouts(void)
{
	ListElement* current = NULL;
	static START_TIME_TYPE last = START_TIME_ZERO;
	START_TIME_TYPE now;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);
	now = MQTTTime_now();
	if (MQTTTime_difftime(now, last) < 3000)
		goto exit;
	last = now;
	while (ListNextElement(handles, &current))		/* for each client */
	{
		MQTTAsyncs* m = (MQTTAsyncs*)(current->content);

		/* check disconnect timeout */
		if (m->c->connect_state == DISCONNECTING)
			MQTTAsync_checkDisconnect(m, &m->disconnect);

		/* check connect timeout */
		if (m->c->connect_state != NOT_IN_PROGRESS && MQTTTime_elapsed(m->connect.start_time) > (m->connectTimeout * 1000))
		{
			nextOrClose(m, MQTTASYNC_FAILURE, "TCP connect timeout");
			continue;
		}

		/* There was a section here that removed timed-out responses.  But if the command had completed and
		 * there was a response, then we may as well report it, no?
		 *
		 * In any case, that section was disabled when automatic reconnect was implemented.
		 */

		if (m->automaticReconnect && m->retrying)
		{
			if (m->reconnectNow || MQTTTime_elapsed(m->lastConnectionFailedTime) > (m->currentInterval * 1000))
			{
				/* to reconnect put the connect command to the head of the command queue */
				MQTTAsync_queuedCommand* conn = malloc(sizeof(MQTTAsync_queuedCommand));
				if (!conn)
					goto exit;
				memset(conn, '\0', sizeof(MQTTAsync_queuedCommand));
				conn->client = m;
				conn->command = m->connect;
	  			/* make sure that the version attempts are restarted */
				if (m->c->MQTTVersion == MQTTVERSION_DEFAULT)
					conn->command.details.conn.MQTTVersion = 0;
				Log(TRACE_MIN, -1, "Automatically attempting to reconnect");
				MQTTAsync_addCommand(conn, sizeof(m->connect));
				m->reconnectNow = 0;
			}
		}
	}
exit:
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT;
}


static thread_return_type WINAPI MQTTAsync_sendThread(void* n)
{
	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);
	sendThread_state = RUNNING;
	sendThread_id = Thread_getid();
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	while (!tostop)
	{
		int rc;

		while (commands->count > 0)
		{
			if (MQTTAsync_processCommand() == 0)
				break;  /* no commands were processed, so go into a wait */
		}
#if !defined(_WIN32) && !defined(_WIN64)
		if ((rc = Thread_wait_cond(send_cond, 1)) != 0 && rc != ETIMEDOUT)
			Log(LOG_ERROR, -1, "Error %d waiting for condition variable", rc);
#else
		if ((rc = Thread_wait_sem(send_sem, 1000)) != 0 && rc != ETIMEDOUT)
			Log(LOG_ERROR, -1, "Error %d waiting for semaphore", rc);
#endif

		MQTTAsync_checkTimeouts();
	}
	sendThread_state = STOPPING;
	MQTTAsync_lock_mutex(mqttasync_mutex);
	sendThread_state = STOPPED;
	sendThread_id = 0;
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT;
#if defined(_WIN32) || defined(_WIN64)
	ExitThread(0);
#endif
	return 0;
}


static void MQTTAsync_emptyMessageQueue(Clients* client)
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
			free(qe->msg->payload);
			free(qe->msg);
		}
		ListEmpty(client->messageQueue);
	}
	FUNC_EXIT;
}


static void MQTTAsync_removeResponsesAndCommands(MQTTAsyncs* m)
{
	int count = 0;
	ListElement* current = NULL;
	ListElement *next = NULL;

	FUNC_ENTRY;
	if (m->responses)
	{
		ListElement* cur_response = NULL;

		while (ListNextElement(m->responses, &cur_response))
		{
			MQTTAsync_queuedCommand* command = (MQTTAsync_queuedCommand*)(cur_response->content);

			if (command->command.onFailure)
			{
				MQTTAsync_failureData data;

				data.token = command->command.token;
				data.code = MQTTASYNC_OPERATION_INCOMPLETE; /* interrupted return code */
				data.message = NULL;

				Log(TRACE_MIN, -1, "Calling %s failure for client %s",
						MQTTPacket_name(command->command.type), m->c->clientID);
				(*(command->command.onFailure))(command->command.context, &data);
			}
			else if (command->command.onFailure5)
			{
				MQTTAsync_failureData5 data = MQTTAsync_failureData5_initializer;

				data.token = command->command.token;
				data.code = MQTTASYNC_OPERATION_INCOMPLETE; /* interrupted return code */
				data.message = NULL;

				Log(TRACE_MIN, -1, "Calling %s failure for client %s",
						MQTTPacket_name(command->command.type), m->c->clientID);
				(*(command->command.onFailure5))(command->command.context, &data);
			}

			MQTTAsync_freeCommand1(command);
			count++;
		}
		ListEmpty(m->responses);
	}
	Log(TRACE_MINIMUM, -1, "%d responses removed for client %s", count, m->c->clientID);

	/* remove commands in the command queue relating to this client */
	count = 0;
	current = ListNextElement(commands, &next);
	ListNextElement(commands, &next);
	while (current)
	{
		MQTTAsync_queuedCommand* command = (MQTTAsync_queuedCommand*)(current->content);

		if (command->client == m)
		{
			ListDetach(commands, command);

			if (command->command.onFailure)
			{
				MQTTAsync_failureData data;

				data.token = command->command.token;
				data.code = MQTTASYNC_OPERATION_INCOMPLETE; /* interrupted return code */
				data.message = NULL;

				Log(TRACE_MIN, -1, "Calling %s failure for client %s",
							MQTTPacket_name(command->command.type), m->c->clientID);
					(*(command->command.onFailure))(command->command.context, &data);
			}
			else if (command->command.onFailure5)
			{
				MQTTAsync_failureData5 data = MQTTAsync_failureData5_initializer;

				data.token = command->command.token;
				data.code = MQTTASYNC_OPERATION_INCOMPLETE; /* interrupted return code */
				data.message = NULL;

				Log(TRACE_MIN, -1, "Calling %s failure for client %s",
							MQTTPacket_name(command->command.type), m->c->clientID);
					(*(command->command.onFailure5))(command->command.context, &data);
			}

			MQTTAsync_freeCommand(command);
			count++;
		}
		current = next;
		ListNextElement(commands, &next);
	}
	Log(TRACE_MINIMUM, -1, "%d commands removed for client %s", count, m->c->clientID);
	FUNC_EXIT;
}


void MQTTAsync_destroy(MQTTAsync* handle)
{
	MQTTAsyncs* m = *handle;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m == NULL)
		goto exit;

	MQTTAsync_closeSession(m->c, MQTTREASONCODE_SUCCESS, NULL);

	MQTTAsync_removeResponsesAndCommands(m);
	ListFree(m->responses);

	if (m->c)
	{
		int saved_socket = m->c->net.socket;
		char* saved_clientid = MQTTStrdup(m->c->clientID);
#if !defined(NO_PERSISTENCE)
		MQTTPersistence_close(m->c);
#endif
		MQTTAsync_emptyMessageQueue(m->c);
		MQTTProtocol_freeClient(m->c);
		if (!ListRemove(bstate->clients, m->c))
			Log(LOG_ERROR, 0, NULL);
		else
			Log(TRACE_MIN, 1, NULL, saved_clientid, saved_socket);
		free(saved_clientid);
	}

	if (m->serverURI)
		free(m->serverURI);
	if (m->createOptions)
		free(m->createOptions);
	MQTTAsync_freeServerURIs(m);
	if (m->connectProps)
	{
		MQTTProperties_free(m->connectProps);
		free(m->connectProps);
		m->connectProps = NULL;
	}
	if (m->willProps)
	{
		MQTTProperties_free(m->willProps);
		free(m->willProps);
		m->willProps = NULL;
	}
	if (!ListRemove(handles, m))
		Log(LOG_ERROR, -1, "free error");
	*handle = NULL;
	if (bstate->clients->count == 0)
		MQTTAsync_terminate();

exit:
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT;
}


void MQTTAsync_freeMessage(MQTTAsync_message** message)
{
	FUNC_ENTRY;
	MQTTProperties_free(&(*message)->properties);
	free((*message)->payload);
	free(*message);
	*message = NULL;
	FUNC_EXIT;
}


void MQTTAsync_free(void* memory)
{
	FUNC_ENTRY;
	free(memory);
	FUNC_EXIT;
}


static int MQTTAsync_completeConnection(MQTTAsyncs* m, Connack* connack)
{
	int rc = MQTTASYNC_FAILURE;

	FUNC_ENTRY;
	if (m->c->connect_state == WAIT_FOR_CONNACK) /* MQTT connect sent - wait for CONNACK */
	{
		Log(LOG_PROTOCOL, 1, NULL, m->c->net.socket, m->c->clientID, connack->rc);
		if ((rc = connack->rc) == MQTTASYNC_SUCCESS)
		{
			m->retrying = 0;
			m->c->connected = 1;
			m->c->good = 1;
			m->c->connect_state = NOT_IN_PROGRESS;
			if (m->c->cleansession || m->c->cleanstart)
				rc = MQTTAsync_cleanSession(m->c);
			else if (m->c->MQTTVersion >= MQTTVERSION_3_1_1 && connack->flags.bits.sessionPresent == 0)
			{
				Log(LOG_PROTOCOL, -1, "Cleaning session state on connect because sessionPresent is 0");
				rc = MQTTAsync_cleanSession(m->c);
			}
			if (m->c->outboundMsgs->count > 0)
			{
				ListElement* outcurrent = NULL;
				START_TIME_TYPE zero = START_TIME_ZERO;

				while (ListNextElement(m->c->outboundMsgs, &outcurrent))
				{
					Messages* messages = (Messages*)(outcurrent->content);
					memset(&messages->lastTouch, '\0', sizeof(messages->lastTouch));
				}
				MQTTProtocol_retry(zero, 1, 1);
				if (m->c->connected != 1)
					rc = MQTTASYNC_DISCONNECTED;
			}
		}
		m->pack = NULL;
#if !defined(_WIN32) && !defined(_WIN64)
		Thread_signal_cond(send_cond);
#else
		Thread_post_sem(send_sem);
#endif
	}
	FUNC_EXIT_RC(rc);
	return rc;
}


/* This is the thread function that handles the calling of callback functions if set */
static thread_return_type WINAPI MQTTAsync_receiveThread(void* n)
{
	long timeout = 10L; /* first time in we have a small timeout.  Gets things started more quickly */

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);
	receiveThread_state = RUNNING;
	receiveThread_id = Thread_getid();
	while (!tostop)
	{
		int rc = SOCKET_ERROR;
		int sock = -1;
		MQTTAsyncs* m = NULL;
		MQTTPacket* pack = NULL;

		MQTTAsync_unlock_mutex(mqttasync_mutex);
		pack = MQTTAsync_cycle(&sock, timeout, &rc);
		MQTTAsync_lock_mutex(mqttasync_mutex);
		if (tostop)
			break;
		timeout = 1000L;

		if (sock == 0)
			continue;
		/* find client corresponding to socket */
		if (ListFindItem(handles, &sock, clientSockCompare) == NULL)
		{
			Log(TRACE_MINIMUM, -1, "Could not find client corresponding to socket %d", sock);
			/* Socket_close(sock); - removing socket in this case is not necessary (Bug 442400) */
			continue;
		}
		m = (MQTTAsyncs*)(handles->current->content);
		if (m == NULL)
		{
			Log(LOG_ERROR, -1, "Client structure was NULL for socket %d - removing socket", sock);
			Socket_close(sock);
			continue;
		}
		if (rc == SOCKET_ERROR)
		{
			Log(TRACE_MINIMUM, -1, "Error from MQTTAsync_cycle() - removing socket %d", sock);
			if (m->c->connected == 1)
				MQTTAsync_disconnect_internal(m, 0);
			else if (m->c->connect_state != NOT_IN_PROGRESS)
				nextOrClose(m, rc, "socket error");
			else /* calling disconnect_internal won't have any effect if we're already disconnected */
				MQTTAsync_closeOnly(m->c, MQTTREASONCODE_SUCCESS, NULL);
		}
		else
		{
			if (m->c->messageQueue->count > 0)
			{
				qEntry* qe = (qEntry*)(m->c->messageQueue->first->content);
				int topicLen = qe->topicLen;

				if (strlen(qe->topicName) == topicLen)
					topicLen = 0;

				if (m->ma)
					rc = MQTTAsync_deliverMessage(m, qe->topicName, topicLen, qe->msg);
				else
					rc = 1;

				if (rc)
				{
#if !defined(NO_PERSISTENCE)
					if (m->c->persistence)
						MQTTPersistence_unpersistQueueEntry(m->c, (MQTTPersistence_qEntry*)qe);
#endif
					ListRemove(m->c->messageQueue, qe); /* qe is freed here */
				}
				else
					Log(TRACE_MIN, -1, "False returned from messageArrived for client %s, message remains on queue",
						m->c->clientID);
			}
			if (pack)
			{
				if (pack->header.bits.type == CONNACK)
				{
					Connack* connack = (Connack*)pack;
					int sessionPresent = connack->flags.bits.sessionPresent;

					rc = MQTTAsync_completeConnection(m, connack);
					if (rc == MQTTASYNC_SUCCESS)
					{
						int onSuccess = 0;
						if ((m->serverURIcount > 0)
						    && (m->connect.details.conn.currentURI < m->serverURIcount))
						{
							Log(TRACE_MIN, -1, "Connect succeeded to %s",
								m->serverURIs[m->connect.details.conn.currentURI]);
						}
						onSuccess = (m->connect.onSuccess != NULL ||
								m->connect.onSuccess5 != NULL); /* save setting of onSuccess callback */
						if (m->connect.onSuccess)
						{
							MQTTAsync_successData data;
							memset(&data, '\0', sizeof(data));
							Log(TRACE_MIN, -1, "Calling connect success for client %s", m->c->clientID);
							if ((m->serverURIcount > 0)
							    && (m->connect.details.conn.currentURI < m->serverURIcount))
								data.alt.connect.serverURI = m->serverURIs[m->connect.details.conn.currentURI];
							else
								data.alt.connect.serverURI = m->serverURI;
							data.alt.connect.MQTTVersion = m->connect.details.conn.MQTTVersion;
							data.alt.connect.sessionPresent = sessionPresent;
							(*(m->connect.onSuccess))(m->connect.context, &data);
							m->connect.onSuccess = NULL; /* don't accidentally call it again */
						}
						else if (m->connect.onSuccess5)
						{
							MQTTAsync_successData5 data = MQTTAsync_successData5_initializer;
							Log(TRACE_MIN, -1, "Calling connect success for client %s", m->c->clientID);
							if (m->serverURIcount > 0)
								data.alt.connect.serverURI = m->serverURIs[m->connect.details.conn.currentURI];
							else
								data.alt.connect.serverURI = m->serverURI;
							data.alt.connect.MQTTVersion = m->connect.details.conn.MQTTVersion;
							data.alt.connect.sessionPresent = sessionPresent;
							data.properties = connack->properties;
							data.reasonCode = connack->rc;
							(*(m->connect.onSuccess5))(m->connect.context, &data);
							m->connect.onSuccess5 = NULL; /* don't accidentally call it again */
						}
						if (m->connected)
						{
							char* reason = (onSuccess) ? "connect onSuccess called" : "automatic reconnect";
							Log(TRACE_MIN, -1, "Calling connected for client %s", m->c->clientID);
							(*(m->connected))(m->connected_context, reason);
						}
						if (m->c->MQTTVersion >= MQTTVERSION_5)
						{
							if (MQTTProperties_hasProperty(&connack->properties, MQTTPROPERTY_CODE_RECEIVE_MAXIMUM))
							{
								int recv_max = MQTTProperties_getNumericValue(&connack->properties, MQTTPROPERTY_CODE_RECEIVE_MAXIMUM);
								if (m->c->maxInflightMessages > recv_max)
									m->c->maxInflightMessages = recv_max;
							}
						}
					}
					else
					{
					    nextOrClose(m, rc, "CONNACK return code");
					}
					MQTTPacket_freeConnack(connack);
				}
				else if (pack->header.bits.type == SUBACK)
				{
					ListElement* current = NULL;

					/* use the msgid to find the callback to be called */
					while (ListNextElement(m->responses, &current))
					{
						MQTTAsync_queuedCommand* command = (MQTTAsync_queuedCommand*)(current->content);
						if (command->command.token == ((Suback*)pack)->msgId)
						{
							Suback* sub = (Suback*)pack;
							if (!ListDetach(m->responses, command)) /* remove the response from the list */
								Log(LOG_ERROR, -1, "Subscribe command not removed from command list");

							/* Call the failure callback if there is one subscribe in the MQTT packet and
							 * the return code is 0x80 (failure).  If the MQTT packet contains >1 subscription
							 * request, then we call onSuccess with the list of returned QoSs, which inelegantly,
							 * could include some failures, or worse, the whole list could have failed.
							 */
							if (m->c->MQTTVersion >= MQTTVERSION_5)
							{
								if (sub->qoss->count == 1 && *(int*)(sub->qoss->first->content) >= MQTTREASONCODE_UNSPECIFIED_ERROR)
								{
									if (command->command.onFailure5)
									{
										MQTTAsync_failureData5 data = MQTTAsync_failureData5_initializer;

										data.token = command->command.token;
										data.reasonCode = *(int*)(sub->qoss->first->content);
										data.message = NULL;
										data.properties = sub->properties;
										Log(TRACE_MIN, -1, "Calling subscribe failure for client %s", m->c->clientID);
										(*(command->command.onFailure5))(command->command.context, &data);
									}
								}
								else if (command->command.onSuccess5)
								{
									MQTTAsync_successData5 data;
									enum MQTTReasonCodes* array = NULL;

									data.reasonCode = *(int*)(sub->qoss->first->content);
									data.alt.sub.reasonCodeCount = sub->qoss->count;
									if (sub->qoss->count > 1)
									{
										ListElement* cur_qos = NULL;
										enum MQTTReasonCodes* element = array = data.alt.sub.reasonCodes = malloc(sub->qoss->count * sizeof(enum MQTTReasonCodes));
										if (array)
											while (ListNextElement(sub->qoss, &cur_qos))
												*element++ = *(int*)(cur_qos->content);
									}
									data.token = command->command.token;
									data.properties = sub->properties;
									Log(TRACE_MIN, -1, "Calling subscribe success for client %s", m->c->clientID);
									(*(command->command.onSuccess5))(command->command.context, &data);
									if (array)
										free(array);
								}
							}
							else if (sub->qoss->count == 1 && *(int*)(sub->qoss->first->content) == MQTT_BAD_SUBSCRIBE)
							{
								if (command->command.onFailure)
								{
									MQTTAsync_failureData data;

									data.token = command->command.token;
									data.code = *(int*)(sub->qoss->first->content);
									data.message = NULL;
									Log(TRACE_MIN, -1, "Calling subscribe failure for client %s", m->c->clientID);
									(*(command->command.onFailure))(command->command.context, &data);
								}
							}
							else if (command->command.onSuccess)
							{
								MQTTAsync_successData data;
								int* array = NULL;

								if (sub->qoss->count == 1)
									data.alt.qos = *(int*)(sub->qoss->first->content);
								else if (sub->qoss->count > 1)
								{
									ListElement* cur_qos = NULL;
									int* element = array = data.alt.qosList = malloc(sub->qoss->count * sizeof(int));
									if (array)
										while (ListNextElement(sub->qoss, &cur_qos))
											*element++ = *(int*)(cur_qos->content);
								}
								data.token = command->command.token;
								Log(TRACE_MIN, -1, "Calling subscribe success for client %s", m->c->clientID);
								(*(command->command.onSuccess))(command->command.context, &data);
								if (array)
									free(array);
							}
							MQTTAsync_freeCommand(command);
							break;
						}
					}
					rc = MQTTProtocol_handleSubacks(pack, m->c->net.socket);
				}
				else if (pack->header.bits.type == UNSUBACK)
				{
					ListElement* current = NULL;
					Unsuback* unsub = (Unsuback*)pack;

					/* use the msgid to find the callback to be called */
					while (ListNextElement(m->responses, &current))
					{
						MQTTAsync_queuedCommand* command = (MQTTAsync_queuedCommand*)(current->content);
						if (command->command.token == ((Unsuback*)pack)->msgId)
						{
							if (!ListDetach(m->responses, command)) /* remove the response from the list */
								Log(LOG_ERROR, -1, "Unsubscribe command not removed from command list");
							if (command->command.onSuccess || command->command.onSuccess5)
							{
								Log(TRACE_MIN, -1, "Calling unsubscribe success for client %s", m->c->clientID);
								if (command->command.onSuccess)
								{
									MQTTAsync_successData data;

									memset(&data, '\0', sizeof(data));
									data.token = command->command.token;
									(*(command->command.onSuccess))(command->command.context, &data);
								}
								else
								{
									MQTTAsync_successData5 data = MQTTAsync_successData5_initializer;
									enum MQTTReasonCodes* array = NULL;

									data.reasonCode = *(enum MQTTReasonCodes*)(unsub->reasonCodes->first->content);
									data.alt.unsub.reasonCodeCount = unsub->reasonCodes->count;
									if (unsub->reasonCodes->count > 1)
									{
										ListElement* cur_rc = NULL;
										enum MQTTReasonCodes* element = array = data.alt.unsub.reasonCodes = malloc(unsub->reasonCodes->count * sizeof(enum MQTTReasonCodes));
										if (array)
											while (ListNextElement(unsub->reasonCodes, &cur_rc))
												*element++ = *(enum MQTTReasonCodes*)(cur_rc->content);
									}
									data.token = command->command.token;
									data.properties = unsub->properties;
									Log(TRACE_MIN, -1, "Calling unsubscribe success for client %s", m->c->clientID);
									(*(command->command.onSuccess5))(command->command.context, &data);
									if (array)
										free(array);
								}
							}
							MQTTAsync_freeCommand(command);
							break;
						}
					}
					rc = MQTTProtocol_handleUnsubacks(pack, m->c->net.socket);
				}
				else if (pack->header.bits.type == DISCONNECT)
				{
					Ack* disc = (Ack*)pack;

					if (m->disconnected)
					{
						Log(TRACE_MIN, -1, "Calling disconnected for client %s", m->c->clientID);
						(*(m->disconnected))(m->disconnected_context, &disc->properties, disc->rc);
					}
					MQTTPacket_freeAck(disc);
				}
			}
		}
	}
	receiveThread_state = STOPPED;
	receiveThread_id = 0;
	MQTTAsync_unlock_mutex(mqttasync_mutex);
#if !defined(_WIN32) && !defined(_WIN64)
	if (sendThread_state != STOPPED)
		Thread_signal_cond(send_cond);
#else
	if (sendThread_state != STOPPED)
		Thread_post_sem(send_sem);
#endif
	FUNC_EXIT;
#if defined(_WIN32) || defined(_WIN64)
	ExitThread(0);
#endif
	return 0;
}


static void MQTTAsync_stop(void)
{
#if !defined(NOSTACKTRACE)
	int rc = 0;
#endif

	FUNC_ENTRY;
	if (sendThread_state != STOPPED || receiveThread_state != STOPPED)
	{
		int conn_count = 0;
		ListElement* current = NULL;

		if (handles != NULL)
		{
			/* find out how many handles are still connected */
			while (ListNextElement(handles, &current))
			{
				if (((MQTTAsyncs*)(current->content))->c->connect_state > NOT_IN_PROGRESS ||
						((MQTTAsyncs*)(current->content))->c->connected)
					++conn_count;
			}
		}
		Log(TRACE_MIN, -1, "Conn_count is %d", conn_count);
		/* stop the background thread, if we are the last one to be using it */
		if (conn_count == 0)
		{
			int count = 0;
			tostop = 1;
			while ((sendThread_state != STOPPED || receiveThread_state != STOPPED) && ++count < 100)
			{
				MQTTAsync_unlock_mutex(mqttasync_mutex);
				Log(TRACE_MIN, -1, "sleeping");
				MQTTAsync_sleep(100L);
				MQTTAsync_lock_mutex(mqttasync_mutex);
			}
#if !defined(NOSTACKTRACE)
			rc = 1;
#endif
			tostop = 0;
		}
	}
	FUNC_EXIT_RC(rc);
}


int MQTTAsync_setCallbacks(MQTTAsync handle, void* context,
									MQTTAsync_connectionLost* cl,
									MQTTAsync_messageArrived* ma,
									MQTTAsync_deliveryComplete* dc)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m == NULL || ma == NULL || m->c == NULL || m->c->connect_state != NOT_IN_PROGRESS)
		rc = MQTTASYNC_FAILURE;
	else
	{
		m->clContext = m->maContext = m->dcContext = context;
		m->cl = cl;
		m->ma = ma;
		m->dc = dc;
	}

	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTAsync_setConnectionLostCallback(MQTTAsync handle, void* context,
										MQTTAsync_connectionLost* cl)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m == NULL || m->c->connect_state != 0)
		rc = MQTTASYNC_FAILURE;
	else
	{
		m->clContext = context;
		m->cl = cl;
	}

	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTAsync_setMessageArrivedCallback(MQTTAsync handle, void* context,
										MQTTAsync_messageArrived* ma)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m == NULL || ma == NULL || m->c->connect_state != 0)
		rc = MQTTASYNC_FAILURE;
	else
	{
		m->maContext = context;
		m->ma = ma;
	}

	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}

int MQTTAsync_setDeliveryCompleteCallback(MQTTAsync handle, void* context,
										  MQTTAsync_deliveryComplete* dc)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m == NULL || m->c->connect_state != 0)
		rc = MQTTASYNC_FAILURE;
	else
	{
		m->dcContext = context;
		m->dc = dc;
	}

	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}



int MQTTAsync_setDisconnected(MQTTAsync handle, void* context, MQTTAsync_disconnected* disconnected)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m == NULL || m->c->connect_state != NOT_IN_PROGRESS)
		rc = MQTTASYNC_FAILURE;
	else
	{
		m->disconnected_context = context;
		m->disconnected = disconnected;
	}

	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTAsync_setConnected(MQTTAsync handle, void* context, MQTTAsync_connected* connected)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m == NULL || m->c->connect_state != NOT_IN_PROGRESS)
		rc = MQTTASYNC_FAILURE;
	else
	{
		m->connected_context = context;
		m->connected = connected;
	}

	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


static void MQTTAsync_closeOnly(Clients* client, enum MQTTReasonCodes reasonCode, MQTTProperties* props)
{
	FUNC_ENTRY;
	client->good = 0;
	client->ping_outstanding = 0;
	if (client->net.socket > 0)
	{
		MQTTProtocol_checkPendingWrites();
		if (client->connected && Socket_noPendingWrites(client->net.socket))
			MQTTPacket_send_disconnect(client, reasonCode, props);
		Thread_lock_mutex(socket_mutex);
		WebSocket_close(&client->net, WebSocket_CLOSE_NORMAL, NULL);
#if defined(OPENSSL)
		SSLSocket_close(&client->net);
#endif
		Socket_close(client->net.socket);
		client->net.socket = 0;
#if defined(OPENSSL)
		client->net.ssl = NULL;
#endif
		Thread_unlock_mutex(socket_mutex);
	}
	client->connected = 0;
	client->connect_state = NOT_IN_PROGRESS;
	FUNC_EXIT;
}


static void MQTTAsync_closeSession(Clients* client, enum MQTTReasonCodes reasonCode, MQTTProperties* props)
{
	FUNC_ENTRY;
	MQTTAsync_closeOnly(client, reasonCode, props);

	if (client->cleansession ||
			(client->MQTTVersion >= MQTTVERSION_5 && client->sessionExpiry == 0))
		MQTTAsync_cleanSession(client);

	FUNC_EXIT;
}


/**
 * List callback function for comparing clients by client structure
 * @param a Async structure
 * @param b Client structure
 * @return boolean indicating whether a and b are equal
 */
static int clientStructCompare(void* a, void* b)
{
	MQTTAsyncs* m = (MQTTAsyncs*)a;
	return m->c == (Clients*)b;
}


static int MQTTAsync_cleanSession(Clients* client)
{
	int rc = 0;
	ListElement* found = NULL;

	FUNC_ENTRY;
#if !defined(NO_PERSISTENCE)
	rc = MQTTPersistence_clear(client);
#endif
	MQTTProtocol_emptyMessageList(client->inboundMsgs);
	MQTTProtocol_emptyMessageList(client->outboundMsgs);
	MQTTAsync_emptyMessageQueue(client);
	client->msgID = 0;

	if ((found = ListFindItem(handles, client, clientStructCompare)) != NULL)
	{
		MQTTAsyncs* m = (MQTTAsyncs*)(found->content);
		MQTTAsync_removeResponsesAndCommands(m);
	}
	else
		Log(LOG_ERROR, -1, "cleanSession: did not find client structure in handles list");
	FUNC_EXIT_RC(rc);
	return rc;
}


static int MQTTAsync_deliverMessage(MQTTAsyncs* m, char* topicName, size_t topicLen, MQTTAsync_message* mm)
{
	int rc;

	Log(TRACE_MIN, -1, "Calling messageArrived for client %s, queue depth %d",
					m->c->clientID, m->c->messageQueue->count);
	rc = (*(m->ma))(m->maContext, topicName, (int)topicLen, mm);
	/* if 0 (false) is returned by the callback then it failed, so we don't remove the message from
	 * the queue, and it will be retried later.  If 1 is returned then the message data may have been freed,
	 * so we must be careful how we use it.
	 */
	return rc;
}


void Protocol_processPublication(Publish* publish, Clients* client, int allocatePayload)
{
	MQTTAsync_message* mm = NULL;
	MQTTAsync_message initialized = MQTTAsync_message_initializer;
	int rc = 0;

	FUNC_ENTRY;
	if ((mm = malloc(sizeof(MQTTAsync_message))) == NULL)
		goto exit;
	memcpy(mm, &initialized, sizeof(MQTTAsync_message));

	if (allocatePayload)
	{
		if ((mm->payload = malloc(publish->payloadlen)) == NULL)
		{
			free(mm);
			goto exit;
		}
		memcpy(mm->payload, publish->payload, publish->payloadlen);
	} else
		mm->payload = publish->payload;
	mm->payloadlen = publish->payloadlen;
	mm->qos = publish->header.bits.qos;
	mm->retained = publish->header.bits.retain;
	if (publish->header.bits.qos == 2)
		mm->dup = 0;  /* ensure that a QoS2 message is not passed to the application with dup = 1 */
	else
		mm->dup = publish->header.bits.dup;
	mm->msgid = publish->msgId;

	if (publish->MQTTVersion >= MQTTVERSION_5)
		mm->properties = MQTTProperties_copy(&publish->properties);

	if (client->messageQueue->count == 0 && client->connected)
	{
		ListElement* found = NULL;

		if ((found = ListFindItem(handles, client, clientStructCompare)) == NULL)
			Log(LOG_ERROR, -1, "processPublication: did not find client structure in handles list");
		else
		{
			MQTTAsyncs* m = (MQTTAsyncs*)(found->content);

			if (m->ma)
				rc = MQTTAsync_deliverMessage(m, publish->topic, publish->topiclen, mm);
		}
	}

	if (rc == 0) /* if message was not delivered, queue it up */
	{
		qEntry* qe = malloc(sizeof(qEntry));

		if (!qe)
			goto exit;
		qe->msg = mm;
		qe->topicName = publish->topic;
		qe->topicLen = publish->topiclen;
		ListAppend(client->messageQueue, qe, sizeof(qe) + sizeof(mm) + mm->payloadlen + strlen(qe->topicName)+1);
#if !defined(NO_PERSISTENCE)
		if (client->persistence)
			MQTTPersistence_persistQueueEntry(client, (MQTTPersistence_qEntry*)qe);
#endif
	}
exit:
	publish->topic = NULL;
	FUNC_EXIT;
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


int MQTTAsync_connect(MQTTAsync handle, const MQTTAsync_connectOptions* options)
{
	MQTTAsyncs* m = handle;
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsync_queuedCommand* conn;

	FUNC_ENTRY;
	if (options == NULL)
	{
		rc = MQTTASYNC_NULL_PARAMETER;
		goto exit;
	}

	if (strncmp(options->struct_id, "MQTC", 4) != 0 || options->struct_version < 0 || options->struct_version > 7)
	{
		rc = MQTTASYNC_BAD_STRUCTURE;
		goto exit;
	}

#if defined(OPENSSL)
	if (m->ssl && options->ssl == NULL)
	{
		rc = MQTTASYNC_NULL_PARAMETER;
		goto exit;
	}
#endif

	if (options->will) /* check validity of will options structure */
	{
		if (strncmp(options->will->struct_id, "MQTW", 4) != 0 || (options->will->struct_version != 0 && options->will->struct_version != 1))
		{
			rc = MQTTASYNC_BAD_STRUCTURE;
			goto exit;
		}
		if (options->will->qos < 0 || options->will->qos > 2)
		{
			rc = MQTTASYNC_BAD_QOS;
			goto exit;
		}
		if (options->will->topicName == NULL)
		{
			rc = MQTTASYNC_NULL_PARAMETER;
			goto exit;
		} else if (strlen(options->will->topicName) == 0)
		{
			rc = MQTTASYNC_0_LEN_WILL_TOPIC;
			goto exit;
		}
	}
	if (options->struct_version != 0 && options->ssl) /* check validity of SSL options structure */
	{
		if (strncmp(options->ssl->struct_id, "MQTS", 4) != 0 || options->ssl->struct_version < 0 || options->ssl->struct_version > 4)
		{
			rc = MQTTASYNC_BAD_STRUCTURE;
			goto exit;
		}
	}
	if (options->MQTTVersion >= MQTTVERSION_5 && m->c->MQTTVersion < MQTTVERSION_5)
	{
		rc = MQTTASYNC_WRONG_MQTT_VERSION;
		goto exit;
	}
	if ((options->username && !UTF8_validateString(options->username)) ||
		(options->password && !UTF8_validateString(options->password)))
	{
		rc = MQTTASYNC_BAD_UTF8_STRING;
		goto exit;
	}
	if (options->MQTTVersion >= MQTTVERSION_5 && options->struct_version < 6)
	{
		rc = MQTTASYNC_BAD_STRUCTURE;
		goto exit;
	}
	if (options->MQTTVersion >= MQTTVERSION_5 && options->cleansession != 0)
	{
		rc = MQTTASYNC_BAD_MQTT_OPTION;
		goto exit;
	}
	if (options->MQTTVersion < MQTTVERSION_5 && options->struct_version >= 6)
	{
		if (options->cleanstart != 0 || options->onFailure5 || options->onSuccess5 ||
				options->connectProperties || options->willProperties)
		{
			rc = MQTTASYNC_BAD_MQTT_OPTION;
			goto exit;
		}
	}

	m->connect.onSuccess = options->onSuccess;
	m->connect.onFailure = options->onFailure;
	if (options->struct_version >= 6)
	{
		m->connect.onSuccess5 = options->onSuccess5;
		m->connect.onFailure5 = options->onFailure5;
	}
	m->connect.context = options->context;
	m->connectTimeout = options->connectTimeout;

	tostop = 0;
	if (sendThread_state != STARTING && sendThread_state != RUNNING)
	{
		MQTTAsync_lock_mutex(mqttasync_mutex);
		sendThread_state = STARTING;
		Thread_start(MQTTAsync_sendThread, NULL);
		MQTTAsync_unlock_mutex(mqttasync_mutex);
	}
	if (receiveThread_state != STARTING && receiveThread_state != RUNNING)
	{
		MQTTAsync_lock_mutex(mqttasync_mutex);
		receiveThread_state = STARTING;
		Thread_start(MQTTAsync_receiveThread, handle);
		MQTTAsync_unlock_mutex(mqttasync_mutex);
	}

	m->c->keepAliveInterval = options->keepAliveInterval;
	setRetryLoopInterval(options->keepAliveInterval);
	m->c->cleansession = options->cleansession;
	m->c->maxInflightMessages = options->maxInflight;
	if (options->struct_version >= 3)
		m->c->MQTTVersion = options->MQTTVersion;
	else
		m->c->MQTTVersion = MQTTVERSION_DEFAULT;
	if (options->struct_version >= 4)
	{
		m->automaticReconnect = options->automaticReconnect;
		m->minRetryInterval = options->minRetryInterval;
		m->maxRetryInterval = options->maxRetryInterval;
	}
	if (options->struct_version >= 7)
	{
		m->c->net.httpHeaders = (const MQTTClient_nameValue *) options->httpHeaders;
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
			rc = PAHO_MEMORY_ERROR;
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
				rc = PAHO_MEMORY_ERROR;
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
		free((void*)m->c->sslopts);
		m->c->sslopts = NULL;
	}

	if (options->struct_version != 0 && options->ssl)
	{
		if ((m->c->sslopts = malloc(sizeof(MQTTClient_SSLOptions))) == NULL)
		{
			rc = PAHO_MEMORY_ERROR;
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
#else
	if (options->struct_version != 0 && options->ssl)
	{
		rc = MQTTASYNC_SSL_NOT_SUPPORTED;
		goto exit;
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
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		memcpy((void*)m->c->password, options->binarypwd.data, m->c->passwordlen);
	}

	m->c->retryInterval = options->retryInterval;
	m->shouldBeConnected = 1;

	m->connectTimeout = options->connectTimeout;

	MQTTAsync_freeServerURIs(m);
	if (options->struct_version >= 2 && options->serverURIcount > 0)
	{
		int i;

		m->serverURIcount = options->serverURIcount;
		if ((m->serverURIs = malloc(options->serverURIcount * sizeof(char*))) == NULL)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		for (i = 0; i < options->serverURIcount; ++i)
			m->serverURIs[i] = MQTTStrdup(options->serverURIs[i]);
	}

	if (m->connectProps)
	{
		MQTTProperties_free(m->connectProps);
		free(m->connectProps);
		m->connectProps = NULL;
	}
	if (m->willProps)
	{
		MQTTProperties_free(m->willProps);
		free(m->willProps);
		m->willProps = NULL;
	}
	if (options->struct_version >=6)
	{
		if (options->connectProperties)
		{
			MQTTProperties initialized = MQTTProperties_initializer;

			if ((m->connectProps = malloc(sizeof(MQTTProperties))) == NULL)
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
			*m->connectProps = initialized;
			*m->connectProps = MQTTProperties_copy(options->connectProperties);

			if (MQTTProperties_hasProperty(options->connectProperties, MQTTPROPERTY_CODE_SESSION_EXPIRY_INTERVAL))
				m->c->sessionExpiry = MQTTProperties_getNumericValue(options->connectProperties,
						MQTTPROPERTY_CODE_SESSION_EXPIRY_INTERVAL);

		}
		if (options->willProperties)
		{
			MQTTProperties initialized = MQTTProperties_initializer;

			if ((m->willProps = malloc(sizeof(MQTTProperties))) == NULL)
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
			*m->willProps = initialized;
			*m->willProps = MQTTProperties_copy(options->willProperties);
		}
		m->c->cleanstart = options->cleanstart;
	}

	/* Add connect request to operation queue */
	if ((conn = malloc(sizeof(MQTTAsync_queuedCommand))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	memset(conn, '\0', sizeof(MQTTAsync_queuedCommand));
	conn->client = m;
	if (options)
	{
		conn->command.onSuccess = options->onSuccess;
		conn->command.onFailure = options->onFailure;
		conn->command.onSuccess5 = options->onSuccess5;
		conn->command.onFailure5 = options->onFailure5;
		conn->command.context = options->context;
	}
	conn->command.type = CONNECT;
	conn->command.details.conn.currentURI = 0;
	rc = MQTTAsync_addCommand(conn, sizeof(conn));

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


static int MQTTAsync_disconnect1(MQTTAsync handle, const MQTTAsync_disconnectOptions* options, int internal)
{
	MQTTAsyncs* m = handle;
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsync_queuedCommand* dis;

	FUNC_ENTRY;
	if (m == NULL || m->c == NULL)
	{
		rc = MQTTASYNC_FAILURE;
		goto exit;
	}
	if (!internal)
		m->shouldBeConnected = 0;
	if (m->c->connected == 0)
	{
		rc = MQTTASYNC_DISCONNECTED;
		goto exit;
	}

	/* Add disconnect request to operation queue */
	if ((dis = malloc(sizeof(MQTTAsync_queuedCommand))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	memset(dis, '\0', sizeof(MQTTAsync_queuedCommand));
	dis->client = m;
	if (options)
	{
		dis->command.onSuccess = options->onSuccess;
		dis->command.onFailure = options->onFailure;
		dis->command.onSuccess5 = options->onSuccess5;
		dis->command.onFailure5 = options->onFailure5;
		dis->command.context = options->context;
		dis->command.details.dis.timeout = options->timeout;
		if (m->c->MQTTVersion >= MQTTVERSION_5 && options->struct_version >= 1)
		{
			dis->command.properties = MQTTProperties_copy(&options->properties);
			dis->command.details.dis.reasonCode = options->reasonCode;
		}
	}
	dis->command.type = DISCONNECT;
	dis->command.details.dis.internal = internal;
	rc = MQTTAsync_addCommand(dis, sizeof(dis));

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


static int MQTTAsync_disconnect_internal(MQTTAsync handle, int timeout)
{
	MQTTAsync_disconnectOptions options = MQTTAsync_disconnectOptions_initializer;

	options.timeout = timeout;
	return MQTTAsync_disconnect1(handle, &options, 1);
}


void MQTTProtocol_closeSession(Clients* c, int sendwill)
{
	MQTTAsync_disconnect_internal((MQTTAsync)c->context, 0);
}


int MQTTAsync_disconnect(MQTTAsync handle, const MQTTAsync_disconnectOptions* options)
{
	if (options != NULL && (strncmp(options->struct_id, "MQTD", 4) != 0 || options->struct_version < 0 || options->struct_version > 1))
		return MQTTASYNC_BAD_STRUCTURE;
	else
		return MQTTAsync_disconnect1(handle, options, 0);
}


int MQTTAsync_isConnected(MQTTAsync handle)
{
	MQTTAsyncs* m = handle;
	int rc = 0;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);
	if (m && m->c)
		rc = m->c->connected;
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


static int cmdMessageIDCompare(void* a, void* b)
{
	MQTTAsync_queuedCommand* cmd = (MQTTAsync_queuedCommand*)a;
	return cmd->command.token == *(int*)b;
}


/**
 * Assign a new message id for a client.  Make sure it isn't already being used and does
 * not exceed the maximum.
 * @param m a client structure
 * @return the next message id to use, or 0 if none available
 */
static int MQTTAsync_assignMsgId(MQTTAsyncs* m)
{
	int start_msgid;
	int msgid;
	thread_id_type thread_id = 0;
	int locked = 0;

	/* need to check: commands list and response list for a client */
	FUNC_ENTRY;
	/* We might be called in a callback. In which case, this mutex will be already locked. */
	thread_id = Thread_getid();
	if (thread_id != sendThread_id && thread_id != receiveThread_id)
	{
		MQTTAsync_lock_mutex(mqttasync_mutex);
		locked = 1;
	}

	/* Fetch last message ID in locked state */
	start_msgid = m->c->msgID;
	msgid = start_msgid;

	msgid = (msgid == MAX_MSG_ID) ? 1 : msgid + 1;
	while (ListFindItem(commands, &msgid, cmdMessageIDCompare) ||
			ListFindItem(m->c->outboundMsgs, &msgid, messageIDCompare) ||
			ListFindItem(m->responses, &msgid, cmdMessageIDCompare))
	{
		msgid = (msgid == MAX_MSG_ID) ? 1 : msgid + 1;
		if (msgid == start_msgid)
		{ /* we've tried them all - none free */
			msgid = 0;
			break;
		}
	}
	if (msgid != 0)
		m->c->msgID = msgid;
	if (locked)
		MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(msgid);
	return msgid;
}


int MQTTAsync_subscribeMany(MQTTAsync handle, int count, char* const* topic, int* qos, MQTTAsync_responseOptions* response)
{
	MQTTAsyncs* m = handle;
	int i = 0;
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsync_queuedCommand* sub;
	int msgid = 0;

	FUNC_ENTRY;
	if (m == NULL || m->c == NULL)
		rc = MQTTASYNC_FAILURE;
	else if (m->c->connected == 0)
		rc = MQTTASYNC_DISCONNECTED;
	else for (i = 0; i < count; i++)
	{
		if (!UTF8_validateString(topic[i]))
		{
			rc = MQTTASYNC_BAD_UTF8_STRING;
			break;
		}
		if (qos[i] < 0 || qos[i] > 2)
		{
			rc = MQTTASYNC_BAD_QOS;
			break;
		}
	}
	if (rc != MQTTASYNC_SUCCESS)
		; /* don't overwrite a previous error code */
	else if ((msgid = MQTTAsync_assignMsgId(m)) == 0)
		rc = MQTTASYNC_NO_MORE_MSGIDS;
	else if (m->c->MQTTVersion >= MQTTVERSION_5 && count > 1 && (count != response->subscribeOptionsCount
			&& response->subscribeOptionsCount != 0))
		rc = MQTTASYNC_BAD_MQTT_OPTION;
	else if (response)
	{
		if (m->c->MQTTVersion >= MQTTVERSION_5)
		{
			if (response->struct_version == 0 || response->onFailure || response->onSuccess)
				rc = MQTTASYNC_BAD_MQTT_OPTION;
		}
		else if (m->c->MQTTVersion < MQTTVERSION_5)
		{
			if (response->struct_version >= 1 && (response->onFailure5 || response->onSuccess5))
				rc = MQTTASYNC_BAD_MQTT_OPTION;
		}
	}
	if (rc != MQTTASYNC_SUCCESS)
		goto exit;

	/* Add subscribe request to operation queue */
	if ((sub = malloc(sizeof(MQTTAsync_queuedCommand))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	memset(sub, '\0', sizeof(MQTTAsync_queuedCommand));
	sub->client = m;
	sub->command.token = msgid;
	if (response)
	{
		sub->command.onSuccess = response->onSuccess;
		sub->command.onFailure = response->onFailure;
		sub->command.onSuccess5 = response->onSuccess5;
		sub->command.onFailure5 = response->onFailure5;
		sub->command.context = response->context;
		response->token = sub->command.token;
		if (m->c->MQTTVersion >= MQTTVERSION_5)
		{
			sub->command.properties = MQTTProperties_copy(&response->properties);
			sub->command.details.sub.opts = response->subscribeOptions;
			if (count > 1)
			{
				if ((sub->command.details.sub.optlist = malloc(sizeof(MQTTSubscribe_options) * count)) == NULL)
				{
					rc = PAHO_MEMORY_ERROR;
					goto exit;
				}
				if (response->subscribeOptionsCount == 0)
				{
					MQTTSubscribe_options initialized = MQTTSubscribe_options_initializer;
					for (i = 0; i < count; ++i)
						sub->command.details.sub.optlist[i] = initialized;
				}
				else
				{
					for (i = 0; i < count; ++i)
						sub->command.details.sub.optlist[i] = response->subscribeOptionsList[i];
				}
			}
		}
	}
	sub->command.type = SUBSCRIBE;
	sub->command.details.sub.count = count;
	sub->command.details.sub.topics = malloc(sizeof(char*) * count);
	sub->command.details.sub.qoss = malloc(sizeof(int) * count);
	if (sub->command.details.sub.topics && sub->command.details.sub.qoss)
	{
		for (i = 0; i < count; ++i)
		{
			if ((sub->command.details.sub.topics[i] = MQTTStrdup(topic[i])) == NULL)
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
			sub->command.details.sub.qoss[i] = qos[i];
		}
		rc = MQTTAsync_addCommand(sub, sizeof(sub));
	}
	else
		rc = PAHO_MEMORY_ERROR;

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTAsync_subscribe(MQTTAsync handle, const char* topic, int qos, MQTTAsync_responseOptions* response)
{
	int rc = 0;
	FUNC_ENTRY;
	rc = MQTTAsync_subscribeMany(handle, 1, (char * const *)(&topic), &qos, response);
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTAsync_unsubscribeMany(MQTTAsync handle, int count, char* const* topic, MQTTAsync_responseOptions* response)
{
	MQTTAsyncs* m = handle;
	int i = 0;
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsync_queuedCommand* unsub;
	int msgid = 0;

	FUNC_ENTRY;
	if (m == NULL || m->c == NULL)
		rc = MQTTASYNC_FAILURE;
	else if (m->c->connected == 0)
		rc = MQTTASYNC_DISCONNECTED;
	else for (i = 0; i < count; i++)
	{
		if (!UTF8_validateString(topic[i]))
		{
			rc = MQTTASYNC_BAD_UTF8_STRING;
			break;
		}
	}
	if (rc != MQTTASYNC_SUCCESS)
		; /* don't overwrite a previous error code */
	else if ((msgid = MQTTAsync_assignMsgId(m)) == 0)
		rc = MQTTASYNC_NO_MORE_MSGIDS;
	else if (response)
	{
		if (m->c->MQTTVersion >= MQTTVERSION_5)
		{
			if (response->struct_version == 0 || response->onFailure || response->onSuccess)
				rc = MQTTASYNC_BAD_MQTT_OPTION;
		}
		else if (m->c->MQTTVersion < MQTTVERSION_5)
		{
			if (response->struct_version >= 1 && (response->onFailure5 || response->onSuccess5))
				rc = MQTTASYNC_BAD_MQTT_OPTION;
		}
	}
	if (rc != MQTTASYNC_SUCCESS)
		goto exit;

	/* Add unsubscribe request to operation queue */
	if ((unsub = malloc(sizeof(MQTTAsync_queuedCommand))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	memset(unsub, '\0', sizeof(MQTTAsync_queuedCommand));
	unsub->client = m;
	unsub->command.type = UNSUBSCRIBE;
	unsub->command.token = msgid;
	if (response)
	{
		unsub->command.onSuccess = response->onSuccess;
		unsub->command.onFailure = response->onFailure;
		unsub->command.onSuccess5 = response->onSuccess5;
		unsub->command.onFailure5 = response->onFailure5;
		unsub->command.context = response->context;
		response->token = unsub->command.token;
		if (m->c->MQTTVersion >= MQTTVERSION_5)
			unsub->command.properties = MQTTProperties_copy(&response->properties);
	}
	unsub->command.details.unsub.count = count;
	if ((unsub->command.details.unsub.topics = malloc(sizeof(char*) * count)) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	for (i = 0; i < count; ++i)
		unsub->command.details.unsub.topics[i] = MQTTStrdup(topic[i]);
	rc = MQTTAsync_addCommand(unsub, sizeof(unsub));

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTAsync_unsubscribe(MQTTAsync handle, const char* topic, MQTTAsync_responseOptions* response)
{
	int rc = 0;
	FUNC_ENTRY;
	rc = MQTTAsync_unsubscribeMany(handle, 1, (char * const *)(&topic), response);
	FUNC_EXIT_RC(rc);
	return rc;
}


static int MQTTAsync_countBufferedMessages(MQTTAsyncs* m)
{
	ListElement* current = NULL;
	int count = 0;

	MQTTAsync_lock_mutex(mqttcommand_mutex);
	while (ListNextElement(commands, &current))
	{
		MQTTAsync_queuedCommand* cmd = (MQTTAsync_queuedCommand*)(current->content);

		if (cmd->client == m && cmd->command.type == PUBLISH)
			count++;
	}
	MQTTAsync_unlock_mutex(mqttcommand_mutex);
	return count;
}


int MQTTAsync_send(MQTTAsync handle, const char* destinationName, int payloadlen, const void* payload,
							 int qos, int retained, MQTTAsync_responseOptions* response)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;
	MQTTAsync_queuedCommand* pub;
	int msgid = 0;

	FUNC_ENTRY;
	if (m == NULL || m->c == NULL)
		rc = MQTTASYNC_FAILURE;
	else if (m->c->connected == 0 && (m->createOptions == NULL ||
		m->createOptions->sendWhileDisconnected == 0 || m->shouldBeConnected == 0))
		rc = MQTTASYNC_DISCONNECTED;
	else if (!UTF8_validateString(destinationName))
		rc = MQTTASYNC_BAD_UTF8_STRING;
	else if (qos < 0 || qos > 2)
		rc = MQTTASYNC_BAD_QOS;
	else if (qos > 0 && (msgid = MQTTAsync_assignMsgId(m)) == 0)
		rc = MQTTASYNC_NO_MORE_MSGIDS;
	else if (m->createOptions && (MQTTAsync_countBufferedMessages(m) >= m->createOptions->maxBufferedMessages))
		rc = MQTTASYNC_MAX_BUFFERED_MESSAGES;
	else if (response)
	{
		if (m->c->MQTTVersion >= MQTTVERSION_5)
		{
			if (response->struct_version == 0 || response->onFailure || response->onSuccess)
				rc = MQTTASYNC_BAD_MQTT_OPTION;
		}
		else if (m->c->MQTTVersion < MQTTVERSION_5)
		{
			if (response->struct_version >= 1 && (response->onFailure5 || response->onSuccess5))
				rc = MQTTASYNC_BAD_MQTT_OPTION;
		}
	}

	if (rc != MQTTASYNC_SUCCESS)
		goto exit;

	/* Add publish request to operation queue */
	if ((pub = malloc(sizeof(MQTTAsync_queuedCommand))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	memset(pub, '\0', sizeof(MQTTAsync_queuedCommand));
	pub->client = m;
	pub->command.type = PUBLISH;
	pub->command.token = msgid;
	if (response)
	{
		pub->command.onSuccess = response->onSuccess;
		pub->command.onFailure = response->onFailure;
		pub->command.onSuccess5 = response->onSuccess5;
		pub->command.onFailure5 = response->onFailure5;
		pub->command.context = response->context;
		response->token = pub->command.token;
		if (m->c->MQTTVersion >= MQTTVERSION_5)
			pub->command.properties = MQTTProperties_copy(&response->properties);
	}
	if ((pub->command.details.pub.destinationName = MQTTStrdup(destinationName)) == NULL)
	{
		free(pub);
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	pub->command.details.pub.payloadlen = payloadlen;
	if ((pub->command.details.pub.payload = malloc(payloadlen)) == NULL)
	{
		free(pub->command.details.pub.destinationName);
		free(pub);
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	memcpy(pub->command.details.pub.payload, payload, payloadlen);
	pub->command.details.pub.qos = qos;
	pub->command.details.pub.retained = retained;
	rc = MQTTAsync_addCommand(pub, sizeof(pub));

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}



int MQTTAsync_sendMessage(MQTTAsync handle, const char* destinationName, const MQTTAsync_message* message,
													 MQTTAsync_responseOptions* response)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	if (message == NULL)
	{
		rc = MQTTASYNC_NULL_PARAMETER;
		goto exit;
	}
	if (strncmp(message->struct_id, "MQTM", 4) != 0 ||
			(message->struct_version != 0 && message->struct_version != 1))
	{
		rc = MQTTASYNC_BAD_STRUCTURE;
		goto exit;
	}

	if (m->c->MQTTVersion >= MQTTVERSION_5 && response)
		response->properties = message->properties;

	rc = MQTTAsync_send(handle, destinationName, message->payloadlen, message->payload,
								message->qos, message->retained, response);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


static void MQTTAsync_retry(void)
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


static int MQTTAsync_connecting(MQTTAsyncs* m)
{
	int rc = -1;
	char* serverURI = m->serverURI;

	FUNC_ENTRY;
	if (m->serverURIcount > 0)
	{
		serverURI = m->serverURIs[m->connect.details.conn.currentURI];

		/* skip URI scheme */
		if (strncmp(URI_TCP, serverURI, strlen(URI_TCP)) == 0)
			serverURI += strlen(URI_TCP);
		else if (strncmp(URI_WS, serverURI, strlen(URI_WS)) == 0)
			serverURI += strlen(URI_WS);
#if defined(OPENSSL)
		else if (strncmp(URI_SSL, serverURI, strlen(URI_SSL)) == 0)
			serverURI += strlen(URI_SSL);
		else if (strncmp(URI_WSS, serverURI, strlen(URI_WSS)) == 0)
			serverURI += strlen(URI_WSS);
#endif
	}

	if (m->c->connect_state == TCP_IN_PROGRESS) /* TCP connect started - check for completion */
	{
		int error;
		socklen_t len = sizeof(error);

		if ((rc = getsockopt(m->c->net.socket, SOL_SOCKET, SO_ERROR, (char*)&error, &len)) == 0)
			rc = error;

		if (rc != 0)
			goto exit;

		Socket_clearPendingWrite(m->c->net.socket);

#if defined(OPENSSL)
		if (m->ssl)
		{
			int port;
			size_t hostname_len;
			int setSocketForSSLrc = 0;

			if (m->websocket && m->c->net.https_proxy) {
				m->c->connect_state = PROXY_CONNECT_IN_PROGRESS;
				if ((rc = WebSocket_proxy_connect( &m->c->net, 1, serverURI)) == SOCKET_ERROR )
					goto exit;
			}

			hostname_len = MQTTProtocol_addressPort(serverURI, &port, NULL);
			setSocketForSSLrc = SSLSocket_setSocketForSSL(&m->c->net, m->c->sslopts,
					serverURI, hostname_len);

			if (setSocketForSSLrc != MQTTASYNC_SUCCESS)
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
				{
					rc = MQTTCLIENT_SUCCESS; /* the connect is still in progress */
					m->c->connect_state = SSL_IN_PROGRESS;
				}
				else if (rc == SSL_FATAL)
				{
					rc = SOCKET_ERROR;
					goto exit;
				}
				else if (rc == 1)
				{
					if ( m->websocket )
					{
						m->c->connect_state = WEBSOCKET_IN_PROGRESS;
						if ((rc = WebSocket_connect(&m->c->net, serverURI)) == SOCKET_ERROR )
							goto exit;
					}
					else
					{
						rc = MQTTCLIENT_SUCCESS;
						m->c->connect_state = WAIT_FOR_CONNACK;
						if (MQTTPacket_send_connect(m->c, m->connect.details.conn.MQTTVersion,
								m->connectProps, m->willProps) == SOCKET_ERROR)
						{
							rc = SOCKET_ERROR;
							goto exit;
						}
					}
					if (!m->c->cleansession && m->c->session == NULL)
						m->c->session = SSL_get1_session(m->c->net.ssl);
				}
			}
			else
			{
				rc = SOCKET_ERROR;
				goto exit;
			}
		}
		else
		{
#endif
			if ( m->websocket )
			{
				if (m->c->net.http_proxy) {
					m->c->connect_state = PROXY_CONNECT_IN_PROGRESS;
					if ((rc = WebSocket_proxy_connect( &m->c->net, 0, serverURI)) == SOCKET_ERROR )
						goto exit;
				}

				m->c->connect_state = WEBSOCKET_IN_PROGRESS;
				if ((rc = WebSocket_connect(&m->c->net, serverURI)) == SOCKET_ERROR )
					goto exit;
			}
			else
			{
				m->c->connect_state = WAIT_FOR_CONNACK; /* TCP/SSL connect completed, in which case send the MQTT connect packet */
				if ((rc = MQTTPacket_send_connect(m->c, m->connect.details.conn.MQTTVersion,
						m->connectProps, m->willProps)) == SOCKET_ERROR)
					goto exit;
			}
#if defined(OPENSSL)
		}
#endif
	}
#if defined(OPENSSL)
	else if (m->c->connect_state == SSL_IN_PROGRESS) /* SSL connect sent - wait for completion */
	{
		rc = m->c->sslopts->struct_version >= 3 ?
			SSLSocket_connect(m->c->net.ssl, m->c->net.socket, serverURI,
				m->c->sslopts->verify, m->c->sslopts->ssl_error_cb, m->c->sslopts->ssl_error_context) :
			SSLSocket_connect(m->c->net.ssl, m->c->net.socket, serverURI,
				m->c->sslopts->verify, NULL, NULL);
		if (rc != 1)
			goto exit;

		if(!m->c->cleansession && m->c->session == NULL)
			m->c->session = SSL_get1_session(m->c->net.ssl);

		if ( m->websocket )
		{
			m->c->connect_state = WEBSOCKET_IN_PROGRESS;
			if ((rc = WebSocket_connect(&m->c->net, serverURI)) == SOCKET_ERROR )
				goto exit;
		}
		else
		{
			m->c->connect_state = WAIT_FOR_CONNACK; /* SSL connect completed, in which case send the MQTT connect packet */
			if ((rc = MQTTPacket_send_connect(m->c, m->connect.details.conn.MQTTVersion,
					m->connectProps, m->willProps)) == SOCKET_ERROR)
				goto exit;
		}
	}
#endif
	else if (m->c->connect_state == WEBSOCKET_IN_PROGRESS) /* Websocket connect sent - wait for completion */
	{
		if ((rc = WebSocket_upgrade( &m->c->net ) ) == SOCKET_ERROR )
			goto exit;
		else if (rc != TCPSOCKET_INTERRUPTED)
		{
			m->c->connect_state = WAIT_FOR_CONNACK; /* Websocket upgrade completed, in which case send the MQTT connect packet */
			if ((rc = MQTTPacket_send_connect(m->c, m->connect.details.conn.MQTTVersion, m->connectProps, m->willProps)) == SOCKET_ERROR)
				goto exit;
		}
	}

exit:
	if ((rc != 0 && rc != TCPSOCKET_INTERRUPTED && (m->c->connect_state != SSL_IN_PROGRESS && m->c->connect_state != WEBSOCKET_IN_PROGRESS)) || (rc == SSL_FATAL))
		nextOrClose(m, MQTTASYNC_FAILURE, "TCP/TLS connect failure");

	FUNC_EXIT_RC(rc);
	return rc;
}


static MQTTPacket* MQTTAsync_cycle(int* sock, unsigned long timeout, int* rc)
{
	struct timeval tp = {0L, 0L};
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
#endif
		/* 0 from getReadySocket indicates no work to do, -1 == error, but can happen normally */
		*sock = Socket_getReadySocket(0, &tp,socket_mutex);
		if (!tostop && *sock == 0 && (tp.tv_sec > 0L || tp.tv_usec > 0L))
			MQTTAsync_sleep(100L);
#if defined(OPENSSL)
	}
#endif
	MQTTAsync_lock_mutex(mqttasync_mutex);
	if (*sock > 0)
	{
		MQTTAsyncs* m = NULL;
		if (ListFindItem(handles, sock, clientSockCompare) != NULL)
			m = (MQTTAsync)(handles->current->content);
		if (m != NULL)
		{
			Log(TRACE_MINIMUM, -1, "m->c->connect_state = %d", m->c->connect_state);
			if (m->c->connect_state == TCP_IN_PROGRESS || m->c->connect_state == SSL_IN_PROGRESS || m->c->connect_state == WEBSOCKET_IN_PROGRESS)
				*rc = MQTTAsync_connecting(m);
			else
				pack = MQTTPacket_Factory(m->c->MQTTVersion, &m->c->net, rc);
			if (m->c->connect_state == WAIT_FOR_CONNACK && *rc == SOCKET_ERROR)
			{
				Log(TRACE_MINIMUM, -1, "CONNECT sent but MQTTPacket_Factory has returned SOCKET_ERROR");
				nextOrClose(m, MQTTASYNC_FAILURE, "TCP connect completion failure");
			}
		}
		if (pack)
		{
			int freed = 1;

			/* Note that these handle... functions free the packet structure that they are dealing with */
			if (pack->header.bits.type == PUBLISH)
				*rc = MQTTProtocol_handlePublishes(pack, *sock);
			else if (pack->header.bits.type == PUBACK || pack->header.bits.type == PUBCOMP ||
					pack->header.bits.type == PUBREC)
			{
				int msgid = 0,
					msgtype = 0,
					ackrc = 0,
					mqttversion = 0;
				MQTTProperties msgprops = MQTTProperties_initializer;

				/* This block is so that the ack variable is local and isn't accidentally reused */
				{
					static Ack ack;
					ack = *(Ack*)pack;
					/* these values are stored because the packet structure is freed in the handle functions */
					msgid = ack.msgId;
					msgtype = pack->header.bits.type;
					if (ack.MQTTVersion >= MQTTVERSION_5)
					{
						ackrc = ack.rc;
						msgprops = MQTTProperties_copy(&ack.properties);
						mqttversion = ack.MQTTVersion;
					}
				}

				if (pack->header.bits.type == PUBCOMP)
					*rc = MQTTProtocol_handlePubcomps(pack, *sock);
				else if (pack->header.bits.type == PUBREC)
					*rc = MQTTProtocol_handlePubrecs(pack, *sock);
				else if (pack->header.bits.type == PUBACK)
					*rc = MQTTProtocol_handlePubacks(pack, *sock);
				if (!m)
					Log(LOG_ERROR, -1, "PUBCOMP, PUBACK or PUBREC received for no client, msgid %d", msgid);
				if (m && (msgtype != PUBREC || ackrc >= MQTTREASONCODE_UNSPECIFIED_ERROR))
				{
					ListElement* current = NULL;

					if (m->dc)
					{
						Log(TRACE_MIN, -1, "Calling deliveryComplete for client %s, msgid %d", m->c->clientID, msgid);
						(*(m->dc))(m->dcContext, msgid);
					}
					/* use the msgid to find the callback to be called */
					while (ListNextElement(m->responses, &current))
					{
						MQTTAsync_queuedCommand* command = (MQTTAsync_queuedCommand*)(current->content);
						if (command->command.token == msgid)
						{
							if (!ListDetach(m->responses, command)) /* then remove the response from the list */
								Log(LOG_ERROR, -1, "Publish command not removed from command list");
							if (command->command.onSuccess)
							{
								MQTTAsync_successData data;

								data.token = command->command.token;
								data.alt.pub.destinationName = command->command.details.pub.destinationName;
								data.alt.pub.message.payload = command->command.details.pub.payload;
								data.alt.pub.message.payloadlen = command->command.details.pub.payloadlen;
								data.alt.pub.message.qos = command->command.details.pub.qos;
								data.alt.pub.message.retained = command->command.details.pub.retained;
								Log(TRACE_MIN, -1, "Calling publish success for client %s", m->c->clientID);
								(*(command->command.onSuccess))(command->command.context, &data);
							}
							else if (command->command.onSuccess5 && ackrc < MQTTREASONCODE_UNSPECIFIED_ERROR)
							{
								MQTTAsync_successData5 data = MQTTAsync_successData5_initializer;

								data.token = command->command.token;
								data.alt.pub.destinationName = command->command.details.pub.destinationName;
								data.alt.pub.message.payload = command->command.details.pub.payload;
								data.alt.pub.message.payloadlen = command->command.details.pub.payloadlen;
								data.alt.pub.message.qos = command->command.details.pub.qos;
								data.alt.pub.message.retained = command->command.details.pub.retained;
								data.properties = command->command.properties;
								Log(TRACE_MIN, -1, "Calling publish success for client %s", m->c->clientID);
								(*(command->command.onSuccess5))(command->command.context, &data);
							}
							else if (command->command.onFailure5 && ackrc >= MQTTREASONCODE_UNSPECIFIED_ERROR)
							{
								MQTTAsync_failureData5 data = MQTTAsync_failureData5_initializer;

								data.token = command->command.token;
								data.reasonCode = ackrc;
								data.properties = msgprops;
								data.packet_type = pack->header.bits.type;
								Log(TRACE_MIN, -1, "Calling publish failure for client %s", m->c->clientID);
								(*(command->command.onFailure5))(command->command.context, &data);
							}
							MQTTAsync_freeCommand(command);
							break;
						}
					}
					if (mqttversion >= MQTTVERSION_5)
						MQTTProperties_free(&msgprops);
				}
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
	MQTTAsync_retry();
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(*rc);
	return pack;
}

/*
static int pubCompare(void* a, void* b)
{
	Messages* msg = (Messages*)a;
	return msg->publish == (Publications*)b;
}*/


int MQTTAsync_getPendingTokens(MQTTAsync handle, MQTTAsync_token **tokens)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;
	ListElement* current = NULL;
	int count = 0;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);
	*tokens = NULL;

	if (m == NULL)
	{
		rc = MQTTASYNC_FAILURE;
		goto exit;
	}

	/* calculate the number of pending tokens - commands plus inflight */
	while (ListNextElement(commands, &current))
	{
		MQTTAsync_queuedCommand* cmd = (MQTTAsync_queuedCommand*)(current->content);

		if (cmd->client == m)
			count++;
	}
	if (m->c)
		count += m->c->outboundMsgs->count;
	if (count == 0)
		goto exit; /* no tokens to return */
	*tokens = malloc(sizeof(MQTTAsync_token) * (count + 1));  /* add space for sentinel at end of list */
	if (!*tokens)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}

	/* First add the unprocessed commands to the pending tokens */
	current = NULL;
	count = 0;
	while (ListNextElement(commands, &current))
	{
		MQTTAsync_queuedCommand* cmd = (MQTTAsync_queuedCommand*)(current->content);

		if (cmd->client == m)
			(*tokens)[count++] = cmd->command.token;
	}

	/* Now add the inflight messages */
	if (m->c && m->c->outboundMsgs->count > 0)
	{
		current = NULL;
		while (ListNextElement(m->c->outboundMsgs, &current))
		{
			Messages* m = (Messages*)(current->content);
			(*tokens)[count++] = m->msgid;
		}
	}
	(*tokens)[count] = -1; /* indicate end of list */

exit:
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTAsync_isComplete(MQTTAsync handle, MQTTAsync_token dt)
{
	int rc = MQTTASYNC_SUCCESS;
	MQTTAsyncs* m = handle;
	ListElement* current = NULL;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m == NULL)
	{
		rc = MQTTASYNC_FAILURE;
		goto exit;
	}

	/* First check unprocessed commands */
	current = NULL;
	while (ListNextElement(commands, &current))
	{
		MQTTAsync_queuedCommand* cmd = (MQTTAsync_queuedCommand*)(current->content);

		if (cmd->client == m && cmd->command.token == dt)
			goto exit;
	}

	/* Now check the inflight messages */
	if (m->c && m->c->outboundMsgs->count > 0)
	{
		current = NULL;
		while (ListNextElement(m->c->outboundMsgs, &current))
		{
			Messages* m = (Messages*)(current->content);
			if (m->msgid == dt)
				goto exit;
		}
	}
	rc = MQTTASYNC_TRUE; /* Can't find it, so it must be complete */

exit:
	MQTTAsync_unlock_mutex(mqttasync_mutex);
	FUNC_EXIT_RC(rc);
	return rc;
}


int MQTTAsync_waitForCompletion(MQTTAsync handle, MQTTAsync_token dt, unsigned long timeout)
{
	int rc = MQTTASYNC_FAILURE;
	START_TIME_TYPE start = MQTTTime_start_clock();
	unsigned long elapsed = 0L;
	MQTTAsyncs* m = handle;

	FUNC_ENTRY;
	MQTTAsync_lock_mutex(mqttasync_mutex);

	if (m == NULL || m->c == NULL)
	{
		MQTTAsync_unlock_mutex(mqttasync_mutex);
		rc = MQTTASYNC_FAILURE;
		goto exit;
	}
	if (m->c->connected == 0)
	{
		MQTTAsync_unlock_mutex(mqttasync_mutex);
		rc = MQTTASYNC_DISCONNECTED;
		goto exit;
	}
	MQTTAsync_unlock_mutex(mqttasync_mutex);

	if (MQTTAsync_isComplete(handle, dt) == 1)
	{
		rc = MQTTASYNC_SUCCESS; /* well we couldn't find it */
		goto exit;
	}

	elapsed = MQTTTime_elapsed(start);
	while (elapsed < timeout)
	{
		MQTTTime_sleep(100);
		if (MQTTAsync_isComplete(handle, dt) == 1)
		{
			rc = MQTTASYNC_SUCCESS; /* well we couldn't find it */
			goto exit;
		}
		elapsed = MQTTTime_elapsed(start);
	}
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}



void MQTTAsync_setTraceLevel(enum MQTTASYNC_TRACE_LEVELS level)
{
	Log_setTraceLevel((enum LOG_LEVELS)level);
}


void MQTTAsync_setTraceCallback(MQTTAsync_traceCallback* callback)
{
	Log_setTraceCallback((Log_traceCallback*)callback);
}


MQTTAsync_nameValue* MQTTAsync_getVersionInfo(void)
{
	#define MAX_INFO_STRINGS 8
	static MQTTAsync_nameValue libinfo[MAX_INFO_STRINGS + 1];
	int i = 0;

	libinfo[i].name = "Product name";
	libinfo[i++].value = "Eclipse Paho Asynchronous MQTT C Client Library";

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

const char* MQTTAsync_strerror(int code)
{
  static char buf[30];

  switch (code) {
    case MQTTASYNC_SUCCESS:
      return "Success";
    case MQTTASYNC_FAILURE:
      return "Failure";
    case MQTTASYNC_PERSISTENCE_ERROR:
      return "Persistence error";
    case MQTTASYNC_DISCONNECTED:
      return "Disconnected";
    case MQTTASYNC_MAX_MESSAGES_INFLIGHT:
      return "Maximum in-flight messages amount reached";
    case MQTTASYNC_BAD_UTF8_STRING:
      return "Invalid UTF8 string";
    case MQTTASYNC_NULL_PARAMETER:
      return "Invalid (NULL) parameter";
    case MQTTASYNC_TOPICNAME_TRUNCATED:
      return "Topic containing NULL characters has been truncated";
    case MQTTASYNC_BAD_STRUCTURE:
      return "Bad structure";
    case MQTTASYNC_BAD_QOS:
      return "Invalid QoS value";
    case MQTTASYNC_NO_MORE_MSGIDS:
      return "Too many pending commands";
    case MQTTASYNC_OPERATION_INCOMPLETE:
      return "Operation discarded before completion";
    case MQTTASYNC_MAX_BUFFERED_MESSAGES:
      return "No more messages can be buffered";
    case MQTTASYNC_SSL_NOT_SUPPORTED:
      return "SSL is not supported";
    case MQTTASYNC_BAD_PROTOCOL:
      return "Invalid protocol scheme";
    case MQTTASYNC_BAD_MQTT_OPTION:
      return "Options for wrong MQTT version";
    case MQTTASYNC_WRONG_MQTT_VERSION:
      return "Client created for another version of MQTT";
    case MQTTASYNC_0_LEN_WILL_TOPIC:
      return "Zero length will topic on connect";
  }

  sprintf(buf, "Unknown error code %d", code);
  return buf;
}
