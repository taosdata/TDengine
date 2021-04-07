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

typedef struct SDemo{
  double sum;
  int num;
  short otype;
}SDemo;

void demo(char* data, short itype, short ibytes, int numOfRows, long long* ts, char* dataOutput, char* tsOutput,
                        int* numOfOutput, short otype, short obytes, SUdfInit* buf) {
   int i;
   double r = 0;
   SDemo *p = (SDemo *)buf->ptr;
   printf("demo input data:%p, type:%d, rows:%d, ts:%p,%lld, dataoutput:%p, tsOutput:%p, numOfOutput:%p, buf:%p\n", data, itype, numOfRows, ts, *ts, dataOutput, tsOutput, numOfOutput, buf);

   for(i=0;i<numOfRows;++i) {
     if (itype == 4) {
       r=*((int *)data+i);
     } else if (itype == 6) {
       r=*((float *)data+i);
     } else if (itype == 7) {
       r=*((double *)data+i);
     }

     p->sum += r*r;
   }

   p->otype = otype;
   p->num += numOfRows;

   *numOfOutput=1;

   printf("demo out, dataoutput:%d, numOfOutput:%d\n", *(int *)dataOutput, *numOfOutput);
}



void demo_finalize(char* dataOutput, int* numOfOutput, SUdfInit* buf) {
   SDemo *p = (SDemo *)buf->ptr;
   printf("demo_finalize dataoutput:%p, numOfOutput:%p, buf:%p\n", dataOutput, numOfOutput, buf);
   if (p->otype == 6) {
     *(float *)dataOutput = (float)(p->sum / p->num);
   } else if (p->otype == 7) {
     *(double *)dataOutput = (double)(p->sum / p->num);
   }

   *numOfOutput=1;

   printf("demo finalize, dataoutput:%d, numOfOutput:%d\n", *(int *)dataOutput, *numOfOutput);
}


int demo_init(SUdfInit* buf) {
   buf->ptr = calloc(1, sizeof(SDemo));
   printf("demo init\n");
   return 0;
}


void demo_destroy(SUdfInit* buf) {
   free(buf->ptr);
   printf("demo destroy\n");
}

