// ~/apache-cassandra-3.10/bin/cassandra 
// gcc -o ../../../build/testcass testcass.c -L ~/Downloads/cpp-driver/build -lcassandra -I ~/Downloads/cpp-driver/include


#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#include "cassandra.h"

struct Basic_ {
  cass_int64_t timestamp;
  cass_int64_t value;
};

typedef struct Basic_ Basic;

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
    exit(1);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError insert_into_basic(CassSession* session, const char* key, const Basic* basic) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "INSERT INTO METER (timestamp, value) VALUES (?, ?);";

  statement = cass_statement_new(query, 2);

  cass_statement_bind_int64(statement, 0, basic->timestamp);
  cass_statement_bind_int64(statement, 1, basic->value);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError select_from_basic(CassSession* session, const char* key, Basic* basic) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT * FROM meter";

  statement = cass_statement_new(query, 1);

  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    if (cass_iterator_next(iterator)) {
      const CassRow* row = cass_iterator_get_row(iterator);
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main(int argc, char* argv[]) {

  struct timeval systemTime;
  long   key, st, et, skey, ekey;
  char   qstr[128];
  int    points = 50000;
  long   i, numOfRows = 0;
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  CassFuture*  close_future = NULL;
  char* hosts = "127.0.0.1";

  if (argc >= 2 ) points = atoi(argv[1]);

  cluster = create_cluster(hosts);

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

//  execure_query(session, "create keyspace dev with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1 };");

  execute_query(session, "use dev;");
  execute_query(session, "CREATE TABLE meter (timestamp bigint, value bigint, PRIMARY KEY (timestamp));");

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000 + systemTime.tv_usec/1000;
  key = st;
  skey = key;

  for (i=0; i<points; ++i) {
    sprintf(qstr, "insert into meter (timestamp, value) VALUES (%ld, %ld);", key++, i);
    execute_query(session, qstr);
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000 + systemTime.tv_usec/1000;
  printf("%ld mseconds to insert %ld data points\n", et-st, i);

  sprintf(qstr, "SELECT * FROM meter;");
//  sprintf(qstr, "SELECT * FROM meter where timestamp >= %ld ALLOW FILTERING;", skey);

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(qstr, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
    exit(1);
  }

  const CassResult* result = cass_future_get_result(future);
  CassIterator* iterator = cass_iterator_from_result(result);
  const CassRow* row;

  while (cass_iterator_next(iterator)) {
    row = cass_iterator_get_row(iterator);
    numOfRows++;
  }

  cass_iterator_free(iterator);
  cass_future_free(future);
  cass_statement_free(statement);

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("%.3f mseconds to retrieve %ld data points\n", (et-st)/1000.0, numOfRows);

  execute_query(session, "DROP TABLE meter;");

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}

