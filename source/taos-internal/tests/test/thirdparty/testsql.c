// gcc -o ../../../build/bin/testsql testsql.c `mysql_config --cflags --libs`

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

//#include <my_global.h>
#include <mysql.h>

void sql_error(MYSQL *con)
{
  fprintf(stderr, "%s\n", mysql_error(con));
  mysql_close(con);
  exit(1);        
}

int main(int argc, char **argv)
{  
  MYSQL *con = mysql_init(NULL);
  struct timeval systemTime;
  long   key, st, et, skey, ekey;
  char   qstr[128];
  int    points = 50000;

  if ( argc <= 1 ) {
    printf("usage: %s numOfPoints\n", argv[0]);
    return getchar();
  }

  if (argc >= 2 ) points = atoi(argv[1]);

  if (con == NULL) 
  {
      fprintf(stderr, "%s\n", mysql_error(con));
      return getchar();
  }

  if (mysql_real_connect(con, "localhost", "root", "", "tsdb", 0, NULL, 0) == NULL) {
      printf("connect error: %s\n", mysql_error(con));
      return getchar();
  }

  mysql_autocommit(con, 0);

  if (mysql_query(con, "CREATE TABLE tsdb.meter(timestamp BIGINT PRIMARY KEY, value BIGINT)"))  {
      printf("create error: %s\n", mysql_error(con));
      return getchar();
  }

  long i;

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000 + systemTime.tv_usec/1000;
  key = st; 
  skey = key;

  for (i=0; i<points; ++i) {
    sprintf(qstr, "insert into tsdb.meter values(%ld, %ld)", key++, i*10);
    if (mysql_query(con, qstr)) {
      printf("insert error: %s\n", mysql_error(con));
      return getchar();
    }
  }

  mysql_commit(con);

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000 + systemTime.tv_usec/1000;
  printf("%ld mseconds to insert %ld data points\n", et-st, i);
  
//  sprintf(qstr, "SELECT * FROM meter where timestamp > %ld && timestamp < %ld order by timestamp desc", skey-1, skey+points + 1);
//  sprintf(qstr, "SELECT * FROM meter where timestamp > %ld && timestamp < %ld", skey-1, skey+points + 1);

  sprintf(qstr, "SELECT * FROM tsdb.meter");

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  if (mysql_query(con, qstr)) 
      sql_error(con);

  MYSQL_RES *result = mysql_use_result(con);
  
  if (result == NULL) 
      sql_error(con);

  int num_fields = mysql_num_fields(result);

  MYSQL_ROW row;
  
  int numOfRows = 0;
  while ((row = mysql_fetch_row(result))) 
  { 
/*
      for(int i = 0; i < num_fields; i++) 
      { 
          printf("%s ", row[i] ? row[i] : "NULL"); 
      } 

      printf("\n"); 
*/
    numOfRows++;
  }
  
  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("%.3f mseconds to retrieve %d data points\n", (et-st)/1000.0, numOfRows);
  
  mysql_free_result(result);

  if (mysql_query(con, "drop TABLE meter")) 
      sql_error(con);

  mysql_close(con);      

  return getchar();
}


