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

void sum_double(char* data, char type, int numOfRows, long long* ts, char* dataOutput, char* tsOutput,
                        int* numOfOutput, SUdfInit* buf) {
   int i;
   int r = 0;
   printf("sum_double input data:%p, type:%d, rows:%d, ts:%p,%lld, dataoutput:%p, tsOutput:%p, numOfOutput:%p, buf:%p\n", data, type, numOfRows, ts, *ts, dataOutput, tsOutput, numOfOutput, buf);
   if (type == 4) {
     r=*(int *)dataOutput;
     for(i=0;i<numOfRows;++i) {
       r+=*((int *)data + i);
       if (tsOutput) {
         *(long long*)tsOutput=1000000;
       }
     }
     *(int *)dataOutput=r;
     *numOfOutput=1;

     printf("sum_double out, dataoutput:%d, numOfOutput:%d\n", *(int *)dataOutput, *numOfOutput);
   }
}



void sum_double_finalize(char* dataOutput, int* numOfOutput, SUdfInit* buf) {
   int i;
   int r = 0;
   printf("sum_double_finalize dataoutput:%p, numOfOutput:%p, buf:%p\n", dataOutput, numOfOutput, buf);
   *numOfOutput=1;
   *(int*)(buf->ptr)=*(int*)dataOutput*2;
   *(int*)dataOutput=*(int*)(buf->ptr);
   printf("sum_double finalize, dataoutput:%d, numOfOutput:%d\n", *(int *)dataOutput, *numOfOutput);
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

