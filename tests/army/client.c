#include "cJSON.h"
#include <arpa/inet.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_IP "127.0.0.1"
#define DEFAULT_PORT 12345
#define DEFAULT_COUNT 10
#define DEFAULT_ROUND 10000

#define ERROR(...)                                                             \
  do {                                                                         \
    fprintf(stderr, __VA_ARGS__);                                              \
    exit(EXIT_FAILURE);                                                        \
  } while (0)

#define CHECK(cond, ...)                                                       \
  if (!(cond))                                                                 \
  ERROR(__VA_ARGS__)

typedef enum { WINDOW_OPEN, WINDOW_CLOSE } EventType;

static const char *getEventTypeStr(EventType e) {
  switch (e) {
  case WINDOW_OPEN:
    return "WINDOW_OPEN";
  case WINDOW_CLOSE:
    return "WINDOW_CLOSE";
  }
}

typedef enum {
  TIME_WINDOW,
  STATE_WINDOW,
  SESSION_WINDOW,
  EVENT_WINDOW,
  COUNT_WINDOW
} WindowType;

static const char *getWindowTypeStr(WindowType w) {
  switch (w) {
  case TIME_WINDOW:
    return "Time Window";
  case STATE_WINDOW:
    return "State Window";
  case SESSION_WINDOW:
    return "Session Window";
  case EVENT_WINDOW:
    return "Event Window";
  case COUNT_WINDOW:
    return "Count Window";
  }
}

typedef struct {
  const char *eventId;
  EventType eventType;
  time_t eventTime;
  WindowType windowType;
  time_t windowStart;
  time_t windowEnd;
  int32_t rowCount;
} Event;

typedef struct {
  const char *streamName;
  Event *events;
} Stream;

typedef struct {
  const char *messageId;
  time_t timestamp;
  Stream *streams;
} Message;

static int64_t getTimeNs() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static void parseArgs(int32_t argc, char *argv[], char **serverIp,
                      int32_t *serverPort, int32_t *eventCount,
                      int32_t *iterRound) {
  int32_t opt;
  while ((opt = getopt(argc, argv, "s:p:e:n:")) != -1) {
    switch (opt) {
    case 's':
      *serverIp = optarg;
      break;
    case 'p':
      *serverPort = atoi(optarg);
      CHECK(*serverPort > 0, "Invalid server port\n");
      break;
    case 'e':
      *eventCount = atoi(optarg);
      CHECK(*eventCount > 0, "Invalid event number\n");
      break;
    case 'n':
      *iterRound = atoi(optarg);
      CHECK(*iterRound >= 0, "Invalid iteration round\n");
      break;
    default:
      ERROR("Usage: %s [-s server_ip] [-p port] [-e event_count] [-n "
            "iterations]\n",
            argv[0]);
    }
  }
}

Message *genRandMessage(int32_t eventCount) {
  Event mocEvent = {.eventId = "A_Mock_Event_Id",
                    .eventType = WINDOW_OPEN,
                    .eventTime = time(NULL),
                    .windowType = TIME_WINDOW,
                    .windowStart = time(NULL),
                    .windowEnd = time(NULL),
                    .rowCount = 1024};

  Stream *stream = calloc(1, sizeof(Stream));
  stream->streamName = "A_Mock_Stream_Name";
  stream->events = calloc(eventCount, sizeof(Event));
  for (int32_t i = 0; i < eventCount; ++i) {
    memcpy(&stream->events[i], &mocEvent, sizeof(Event));
  }

  Message *msg = calloc(1, sizeof(Message));
  msg->messageId = "A_Mock_Message_Id";
  msg->timestamp = time(NULL);
  msg->streams = stream;
  return msg;
}

void freeMessage(Message *msg) {
  free(msg->streams->events);
  free(msg->streams);
  free(msg);
}

static char sendBuf[2 << 20];

char *convert2Json(const Message *msg, int32_t eventCount) {
  const Stream *s = msg->streams;
  const Event *e = s->events;

  cJSON *events = cJSON_CreateArray();
  for (int32_t i = 0; i < eventCount; ++i) {
    cJSON *event = cJSON_CreateObject();
    cJSON_AddItemToObjectCS(event, "event_id",
                          cJSON_CreateString(e[i].eventId));
    cJSON_AddItemToObjectCS(
        event, "event_type",
        cJSON_CreateString(getEventTypeStr(e[i].eventType)));
    cJSON_AddItemToObjectCS(event, "event_time",
                          cJSON_CreateNumber(e[i].eventTime));
    cJSON_AddItemToObjectCS(
        event, "window_type",
        cJSON_CreateString(getWindowTypeStr(e[i].windowType)));
    cJSON_AddItemToObjectCS(event, "window_start",
                          cJSON_CreateNumber(e[i].eventTime));
    cJSON_AddItemToObjectCS(event, "window_end",
                          cJSON_CreateNumber(e[i].eventTime));
    cJSON_AddItemToObjectCS(event, "row_count",
                          cJSON_CreateNumber(e[i].rowCount));
    cJSON_AddItemToArray(events, event);
  }

  cJSON *stream = cJSON_CreateObject();
  cJSON_AddItemToObjectCS(stream, "stream_name",
                        cJSON_CreateString(msg->streams->streamName));
  cJSON_AddItemToObjectCS(stream, "events", events);

  cJSON *json = cJSON_CreateObject();
  cJSON_AddItemToObjectCS(json, "message_id",
                        cJSON_CreateString(msg->messageId));
  cJSON_AddItemToObjectCS(json, "timestamp", cJSON_CreateNumber(msg->timestamp));
  cJSON_AddItemToObjectCS(json, "streams", stream);

  cJSON_PrintPreallocated(json, sendBuf, sizeof(sendBuf), 0);
  cJSON_Delete(json);
  return sendBuf;
}

int32_t main(int32_t argc, char *argv[]) {
  char *serverIp = DEFAULT_IP;
  int32_t serverPort = DEFAULT_PORT;
  int32_t eventCount = DEFAULT_COUNT;
  int32_t iterRound = DEFAULT_ROUND;
  parseArgs(argc, argv, &serverIp, &serverPort, &eventCount, &iterRound);

  struct sockaddr_in serverAddr = {.sin_family = AF_INET,
                                   .sin_port = htons(serverPort)};
  CHECK(inet_pton(AF_INET, serverIp, &serverAddr.sin_addr) > 0,
        "Address is invalid or not supported\n");
  int32_t fd = socket(AF_INET, SOCK_STREAM, 0);
  CHECK(fd != -1, "Failed to create socket\n");
  CHECK(connect(fd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) != -1,
        "Failed to connect\n");

  printf("Connect to server (%s:%d).\n", serverIp, serverPort);

  Message *msg = genRandMessage(eventCount);
  int64_t startTime = getTimeNs();
  for (int32_t i = 0; i < iterRound; ++i) {
    char *str = convert2Json(msg, eventCount);
    CHECK(send(fd, str, strlen(str), 0) != -1, "Failed to send\n");
  }
  int64_t endTime = getTimeNs();
  printf("Iteration %d with %d events completed. Time elapsed: %.3f ms\n",
         iterRound, eventCount, (endTime - startTime) / (1.0 * 1e6));
  freeMessage(msg);
  close(fd);
  return 0;
}
