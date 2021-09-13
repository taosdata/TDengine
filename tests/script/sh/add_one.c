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

void add_one(char* data, short itype, short ibytes, int numOfRows, long long* ts, char* dataOutput, char* interBUf, char* tsOutput,
                        int* numOfOutput, short otype, short obytes, SUdfInit* buf) {
   int i;
   int r = 0;
   printf("add_one input data:%p, type:%d, rows:%d, ts:%p,%lld, dataoutput:%p, tsOutput:%p, numOfOutput:%p, buf:%p\n", data, itype, numOfRows, ts, *ts, dataOutput, tsOutput, numOfOutput, buf);
   if (itype == 4) {
     for(i=0;i<numOfRows;++i) {
       printf("input %d - %d", i, *((int *)data + i));
       *((int *)dataOutput+i)=*((int *)data + i) + 1;
       printf(", output %d\n", *((int *)dataOutput+i));
       if (tsOutput) {
         *(long long*)tsOutput=1000000;
       }
     }
     *numOfOutput=numOfRows;

     printf("add_one out, numOfOutput:%d\n", *numOfOutput);
   }
}


