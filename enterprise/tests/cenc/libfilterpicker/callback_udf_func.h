#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "sachdr.h"
#include "ew_bridge.h"
#include "PickData.h"
#include "FilterPicker5_Memory.h"
#include "FilterPicker5.h"


typedef struct SUdfInit{
    int       maybe_null; /* 1 if function can return NULL */
    int       decimals;   /* for real functions */
    long long length;     /* For string functions */
    char     *ptr;        /* free pointer for function data */
    int       const_item; /* 0 if result is independent of arguments */
} SUdfInit;

int callback_udf_func_init(SUdfInit *buf);
void callback_udf_func_destroy(SUdfInit *buf);
void callback_udf_func(char *data, short itype, short ibytes, int numOfRows, long long *ts, char *dataOutput, char *interbuf, char *tsOutput, int *numOfOutput, short otype, short obytes, SUdfInit *buf);
