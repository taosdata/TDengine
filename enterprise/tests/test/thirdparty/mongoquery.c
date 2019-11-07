// sudo service mongod start
// gcc -o ../../../build/bin/mongoquery mongoquery.c -I/usr/local/include/libbson-1.0 -I/usr/local/include/libmongoc-1.0 -lmongoc-1.0 -lbson-1.0

#include <bson.h>
#include <bcon.h>
#include <mongoc.h>

int
main (int   argc,
      char *argv[])
{
   mongoc_client_t      *client;
   mongoc_database_t    *database;
   mongoc_collection_t  *collection;
   bson_t               *command,
                         reply,
                        *insert;
   bson_error_t          error;
   char                 *str;
   bool                  retval;
   mongoc_cursor_t      *cursor;
   const                 bson_t *doc;
   bson_t               *query;
/*
   bson_t keys;
   mongoc_index_opt_t opt;
   mongoc_index_opt_geo_t geo_opt;
*/
  struct timeval systemTime;
  long   key, st, et, skey, ekey, i;
  char   qstr[128];
  int    points = 50000;
  int    numOfRows = 0;

  if (argc >= 2 ) points = atoi(argv[1]);

   /*
    * Required to initialize libmongoc's internals
    */
   mongoc_init ();

   /*
    * Create a new client instance
    */
   client = mongoc_client_new ("mongodb://localhost:27017");

   /*
    * Register the application name so we can track it in the profile logs
    * on the server. This can also be done from the URI (see other examples).
    */
   mongoc_client_set_appname (client, "connect-example");

   /*
    * Get a handle on the database "db_name" and collection "coll_name"
    */
   database = mongoc_client_get_database (client, "db_name");
   collection = mongoc_client_get_collection (client, "db_name", "coll_name");

/*
   mongoc_index_opt_init (&opt);
   bson_init (&keys);
   BSON_APPEND_INT64 (&keys, "timestamp", 1);
   if (mongoc_collection_create_index (collection, &keys, &opt, &error)) {
     fprintf (stderr, "%s\n", error.message);  
   }
*/

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

   query = bson_new ();
   cursor = mongoc_collection_find_with_opts (collection, query, NULL, NULL);

   while (mongoc_cursor_next (cursor, &doc)) {

     numOfRows++;     
   }

   gettimeofday(&systemTime, NULL);
   et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
   printf("%.3f mseconds to retrieve %d data points\n", (et-st)/1000.0, numOfRows);

   bson_destroy (query);
   mongoc_cursor_destroy (cursor);

   query = bson_new ();
   mongoc_collection_remove(collection, 0, query, NULL, &error);
   bson_destroy (query);

   /*
    * Release our handles and clean up libmongoc
    */
   mongoc_collection_destroy (collection);
   mongoc_database_destroy (database);
   mongoc_client_destroy (client);
   mongoc_cleanup ();

   return 0;
}

