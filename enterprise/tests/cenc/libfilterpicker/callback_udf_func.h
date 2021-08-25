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


void callback_udf_func(char *data, char type, int numOfRows, long long *ts, char *dataOutput, char *tsOutput, int *numOfOutput, SUdfInit *buf);
