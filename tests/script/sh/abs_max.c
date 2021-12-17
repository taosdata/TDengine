#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h> 

typedef struct SUdfInit{
 int maybe_null;       /* 1 if function can return NULL */
 int decimals;     /* for real functions */
 int64_t length;       /* For string functions */
 char  *ptr;            /* free pointer for function data */
 int const_item;       /* 0 if result is independent of arguments */
} SUdfInit;


#define TSDB_DATA_INT_NULL              0x80000000L
#define TSDB_DATA_BIGINT_NULL           0x8000000000000000L

void abs_max(char* data, short itype, short ibytes, int numOfRows, int64_t* ts, char* dataOutput, char* interBuf, char* tsOutput,
                        int* numOfOutput, short otype, short obytes, SUdfInit* buf) {
   int i;
   int64_t r = 0;
   // printf("abs_max input data:%p, type:%d, rows:%d, ts:%p, %" PRId64 ", dataoutput:%p, tsOutput:%p, numOfOutput:%p, buf:%p\n", data, itype, numOfRows, ts, *ts, dataOutput, tsOutput, numOfOutput, buf);
   if (itype == 5) {
     r=*(int64_t *)dataOutput;
     *numOfOutput=0;

     for(i=0;i<numOfRows;++i) {
       if (*((int64_t *)data + i) == TSDB_DATA_BIGINT_NULL) {
         continue;
       }

       *numOfOutput=1;
       //int64_t v = abs(*((int64_t *)data + i));
       int64_t v = *((int64_t *)data + i);
       if (v < 0) {
          v = 0 - v;
       }
       
       if (v > r) {
          r = v;
       }
     }

     *(int64_t *)dataOutput=r;

   //   printf("abs_max out, dataoutput:%" PRId64", numOfOutput:%d\n", *(int64_t *)dataOutput, *numOfOutput);
   }else {
     *numOfOutput=0;
   }
}



void abs_max_finalize(char* dataOutput, char* interBuf, int* numOfOutput, SUdfInit* buf) {
   int i;
   //int64_t r = 0;
   // printf("abs_max_finalize dataoutput:%p:%d, numOfOutput:%d, buf:%p\n", dataOutput, *dataOutput, *numOfOutput, buf);
   // *numOfOutput=1;
   // printf("abs_max finalize, dataoutput:%" PRId64", numOfOutput:%d\n", *(int64_t *)dataOutput, *numOfOutput);
}

void abs_max_merge(char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput, SUdfInit* buf) {
   int64_t r = 0;
   
   if (numOfRows > 0) {
      r = *((int64_t *)data);
   }
   // printf("abs_max_merge numOfRows:%d, dataoutput:%p, buf:%p\n", numOfRows, dataOutput, buf);
   for (int i = 1; i < numOfRows; ++i) {
   //   printf("abs_max_merge %d - %" PRId64"\n", i, *((int64_t *)data + i));
     if (*((int64_t*)data + i) > r) {
        r= *((int64_t*)data + i);
     }
   }
   
   *(int64_t*)dataOutput=r;
   if (numOfRows > 0) {
     *numOfOutput=1;
   } else {
     *numOfOutput=0;
   }
   
   // printf("abs_max_merge, dataoutput:%" PRId64", numOfOutput:%d\n", *(int64_t *)dataOutput, *numOfOutput);
}


int abs_max_init(SUdfInit* buf) {
   // printf("abs_max init\n");
   return 0;
}


void abs_max_destroy(SUdfInit* buf) {
   // printf("abs_max destroy\n");
}