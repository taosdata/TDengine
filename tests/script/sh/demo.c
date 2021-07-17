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

#define FLOAT_NULL            0x7FF00000              // it is an NAN
#define DOUBLE_NULL           0x7FFFFF0000000000L     // it is an NAN


void demo(char* data, short itype, short ibytes, int numOfRows, long long* ts, char* dataOutput, char* interBuf, char* tsOutput,
                        int* numOfOutput, short otype, short obytes, SUdfInit* buf) {
   int i;
   double r = 0;
   SDemo *p = (SDemo *)interBuf;
   SDemo *q = (SDemo *)dataOutput;
   printf("demo input data:%p, type:%d, rows:%d, ts:%p,%lld, dataoutput:%p, interBUf:%p, tsOutput:%p, numOfOutput:%p, buf:%p\n", data, itype, numOfRows, ts, *ts, dataOutput, interBuf, tsOutput, numOfOutput, buf);

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

   q->sum = p->sum;
   q->num = p->num;
   q->otype = p->otype;

   *numOfOutput=1;

   printf("demo out, sum:%f, num:%d, numOfOutput:%d\n", p->sum, p->num, *numOfOutput);
}


void demo_merge(char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput, SUdfInit* buf) {
   int i;
   SDemo *p = (SDemo *)data;
   SDemo res = {0};
   printf("demo_merge input data:%p, rows:%d, dataoutput:%p, numOfOutput:%p, buf:%p\n", data, numOfRows, dataOutput, numOfOutput, buf);

   for(i=0;i<numOfRows;++i) {
     res.sum += p->sum * p->sum;
     res.num += p->num;
     p++;
   }

   p->sum = res.sum;
   p->num = res.num;

   *numOfOutput=1;

   printf("demo out, sum:%f, num:%d, numOfOutput:%d\n", p->sum, p->num, *numOfOutput);
}



void demo_finalize(char* dataOutput, char* interBuf, int* numOfOutput, SUdfInit* buf) {
   SDemo *p = (SDemo *)interBuf;
   printf("demo_finalize interbuf:%p, numOfOutput:%p, buf:%p, sum:%f, num:%d\n", interBuf, numOfOutput, buf, p->sum, p->num);
   if (p->otype == 6) {
     if (p->num != 30000) {
       *(unsigned int *)dataOutput = FLOAT_NULL;  
     } else {
       *(float *)dataOutput = (float)(p->sum / p->num);  
     }
     printf("finalize values:%f\n", *(float *)dataOutput);
   } else if (p->otype == 7) {
     if (p->num != 30000) {
       *(unsigned long long *)dataOutput = DOUBLE_NULL;  
     } else {    
       *(double *)dataOutput = (double)(p->sum / p->num);
     }
     printf("finalize values:%f\n", *(double *)dataOutput);
   }

   *numOfOutput=1;

   printf("demo finalize, numOfOutput:%d\n", *numOfOutput);
}


int demo_init(SUdfInit* buf) {
   printf("demo init\n");
   return 0;
}


void demo_destroy(SUdfInit* buf) {
   printf("demo destroy\n");
}

