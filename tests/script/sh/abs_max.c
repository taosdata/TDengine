#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct SUdfInit{
 int maybe_null;       /* 1 if function can return NULL */
 int decimals;     /* for real functions */
 long long length;       /* For string functions */
 char  *ptr;            /* free pointer for function data */
 int const_item;       /* 0 if result is independent of arguments */
} SUdfInit;


#define TSDB_DATA_INT_NULL              0x80000000L
#define TSDB_DATA_BIGINT_NULL           0x8000000000000000L

void abs_max(char* data, short itype, short ibytes, int numOfRows, long long* ts, char* dataOutput, char* interBuf, char* tsOutput,
                        int* numOfOutput, short otype, short obytes, SUdfInit* buf) {
   int i;
   int r = 0;
   printf("abs_max input data:%p, type:%d, rows:%d, ts:%p,%lld, dataoutput:%p, tsOutput:%p, numOfOutput:%p, buf:%p\n", data, itype, numOfRows, ts, *ts, dataOutput, tsOutput, numOfOutput, buf);
   if (itype == 5) {
     r=*(long *)dataOutput;
     *numOfOutput=0;

     for(i=0;i<numOfRows;++i) {
       if (*((long *)data + i) == TSDB_DATA_BIGINT_NULL) {
         continue;
       }

       *numOfOutput=1;
       long v = abs(*((long *)data + i));
       if (v > r) {
          r = v;
       }
     }

     *(long *)dataOutput=r;

     printf("abs_max out, dataoutput:%ld, numOfOutput:%d\n", *(long *)dataOutput, *numOfOutput);
   }
}



void abs_max_finalize(char* dataOutput, char* interBuf, int* numOfOutput, SUdfInit* buf) {
   int i;
   int r = 0;
   printf("abs_max_finalize dataoutput:%p:%d, numOfOutput:%d, buf:%p\n", dataOutput, *dataOutput, *numOfOutput, buf);
   *numOfOutput=1;
   printf("abs_max finalize, dataoutput:%ld, numOfOutput:%d\n", *(long *)dataOutput, *numOfOutput);
}

void abs_max_merge(char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput, SUdfInit* buf) {
   int r = 0;
   
   if (numOfRows > 0) {
      r = *((long *)data);
   }
   printf("abs_max_merge numOfRows:%d, dataoutput:%p, buf:%p\n", numOfRows, dataOutput, buf);
   for (int i = 1; i < numOfRows; ++i) {
     printf("abs_max_merge %d - %ld\n", i, *((long *)data + i));
     if (*((long*)data + i) > r) {
        r= *((long*)data + i);
     }
   }
   
   *(long*)dataOutput=r;
   if (numOfRows > 0) {
     *numOfOutput=1;
   } else {
     *numOfOutput=0;
   }
   
   printf("abs_max_merge, dataoutput:%ld, numOfOutput:%d\n", *(long *)dataOutput, *numOfOutput);
}


int abs_max_init(SUdfInit* buf) {
   printf("abs_max init\n");
   return 0;
}


void abs_max_destroy(SUdfInit* buf) {
   printf("abs_max destroy\n");
}

