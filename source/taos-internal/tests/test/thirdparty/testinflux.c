// /usr/bin/influxd &
// gcc -o ../../../build/bin/testinflux testinflux.c -L/usr/lib/x86_64-linux-gnu -lcurl

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include <sys/time.h>
 
uint write_cb(char *in, uint size, uint nmemb, void *p)
{
  return 0;
}
   
int main(int argc, char *argv[])
{
  struct timeval systemTime;
  long   key, st, et, skey, ekey;
  char   qstr[128];
  int    points = 50000;
  CURL   *curl;
  CURLcode res;
  char   *pencode;
  long    i, numOfRows = 0;

  if (argc >= 2 ) points = atoi(argv[1]);

  curl_global_init(CURL_GLOBAL_ALL);
 
  curl = curl_easy_init();
  if ( curl == NULL ) {
    printf("failed to init curl\n");
    return -1;
  }

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000 + systemTime.tv_usec/1000;
  key = st;
  skey = key;

  for (i=0; i<points; ++i) {
    curl_easy_setopt(curl, CURLOPT_URL, "http://localhost:8086/write?db=tsdb");
    sprintf(qstr, "meter5 value=%ld", i);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, qstr);
    res = curl_easy_perform(curl);
    if(res != CURLE_OK) {
      fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
      goto _exit;
    }
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000 + systemTime.tv_usec/1000;
  printf("%ld mseconds to insert %ld data points\n", et-st, i);


  curl_easy_setopt(curl, CURLOPT_URL, "http://localhost:8086/query?pretty=true&db=tsdb&q=SELECT\%20\%2A\%20FROM\%20meter5");
  curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);

//  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
  FILE *fp; 
  fp = fopen("/dev/null", "w");
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  res = curl_easy_perform(curl);
  if(res != CURLE_OK) {
    fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
    goto _exit;
  }
  
  for (i=0; i< points; ++i) {
    numOfRows ++;
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
  printf("%ld useconds to retrieve %ld data points\n", et-st, numOfRows);
  
_exit:
  curl_global_cleanup();
  return 0;
}

