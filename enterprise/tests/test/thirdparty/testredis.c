// pre-condition
// ~/Downloads/redis-3.2.8/src/redis-server
// ~/Downloads/redis-3.2.8/src/redis-cli

// gcc -o ../../../build/bin/testredis testredis.c -L/usr/local/lib -lhiredis -I/usr/local/include/hiredis
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <hiredis.h>

int main(int argc, char **argv) {
  struct timeval systemTime;
  long   key, st, et, skey, ekey;
  char   qstr[128];
  int    points = 50000;
  long   numOfRows=0, i;
  redisContext *c;
  redisReply *reply;

  if (argc >= 2 ) points = atoi(argv[1]);

  struct timeval timeout = { 1, 500000 }; // 1.5 seconds
  c = redisConnectWithTimeout("127.0.0.1", 6379, timeout);
  if (c == NULL || c->err) {
      if (c) {
          printf("Connection error: %s\n", c->errstr);
          redisFree(c);
      } else {
          printf("Connection error: can't allocate redis context\n");
      }
      exit(1);
  }

  reply = redisCommand(c,"DEL meter");
  freeReplyObject(reply);

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000 + systemTime.tv_usec/1000;
  key = st;
  skey = key;

  for (i=0; i<points; ++i) {
    reply = redisCommand(c,"zadd meter %ld %ld", key++, i);
    freeReplyObject(reply);
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000 + systemTime.tv_usec/1000;
  printf("%ld mseconds to insert %ld data points\n", et-st, i);

  /* Let's check what we have inside the list */
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  reply = redisCommand(c,"zrange meter 0 -1");
  if (reply->type == REDIS_REPLY_ARRAY) {
    for (i = 0; i < reply->elements; i++) {
      numOfRows++;
    }
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("%.3f mseconds to retrieve %ld data points\n", (et-st)/1000.0, numOfRows);

  freeReplyObject(reply);

  /* Disconnects and frees the context */
  redisFree(c);

  return 0;
}
