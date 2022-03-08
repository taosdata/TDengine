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


void sum_double(char* data, short itype, short ibytes, int numOfRows, long long* ts, char* dataOutput, char* interBuf, char* tsOutput,
                        int* numOfOutput, short otype, short obytes, SUdfInit* buf) {
   int i;
   int r = 0;
   printf("sum_double input data:%p, type:%d, rows:%d, ts:%p,%lld, dataoutput:%p, tsOutput:%p, numOfOutput:%p, buf:%p\n", data, itype, numOfRows, ts, *ts, dataOutput, tsOutput, numOfOutput, buf);
   if (itype == 4) {
     r=*(int *)dataOutput;
     *numOfOutput=0;

     for(i=0;i<numOfRows;++i) {
       if (*((int *)data + i) == TSDB_DATA_INT_NULL) {
         continue;
       }

       *numOfOutput=1;
       r+=*((int *)data + i);
       *(int *)dataOutput=r;
     }

     printf("sum_double out, dataoutput:%d, numOfOutput:%d\n", *(int *)dataOutput, *numOfOutput);
   }
}



void sum_double_finalize(char* dataOutput, char* interBuf, int* numOfOutput, SUdfInit* buf) {
   int i;
   int r = 0;
   printf("sum_double_finalize dataoutput:%p:%d, numOfOutput:%d, buf:%p\n", dataOutput, *dataOutput, *numOfOutput, buf);
   *numOfOutput=1;
   *(int*)(buf->ptr)=*(int*)dataOutput*2;
   *(int*)dataOutput=*(int*)(buf->ptr);
   printf("sum_double finalize, dataoutput:%d, numOfOutput:%d\n", *(int *)dataOutput, *numOfOutput);
}

void sum_double_merge(char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput, SUdfInit* buf) {
   int r = 0;
   int sum = 0;
   
   printf("sum_double_merge numOfRows:%d, dataoutput:%p, buf:%p\n", numOfRows, dataOutput, buf);
   for (int i = 0; i < numOfRows; ++i) {
     printf("sum_double_merge %d - %d\n", i, *((int*)data + i));
     sum +=*((int*)data + i);
   }
   
   *(int*)dataOutput+=sum;
   if (numOfRows > 0) {
     *numOfOutput=1;
   } else {
     *numOfOutput=0;
   }
   
   printf("sum_double_merge, dataoutput:%d, numOfOutput:%d\n", *(int *)dataOutput, *numOfOutput);
}


int sum_double_init(SUdfInit* buf) {
   buf->maybe_null=1;
   buf->ptr = malloc(sizeof(int));
   printf("sum_double init\n");
   return 0;
}


void sum_double_destroy(SUdfInit* buf) {
   free(buf->ptr);
   printf("sum_double destroy\n");
}

