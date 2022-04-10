#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "tudf.h"
void udf1(int8_t step, char *state, int32_t stateSize, SUdfDataBlock input,
          char **newState, int32_t *newStateSize, SUdfDataBlock *output) {
    fprintf(stdout, "%s, step:%d\n", "udf function called", step);
    char *newStateBuf = malloc(stateSize);
    memcpy(newStateBuf, state, stateSize);
    *newState = newStateBuf;
    *newStateSize = stateSize;

    char *outputBuf = malloc(input.size);
    memcpy(outputBuf, input.data, input.size);
    output->data = outputBuf;
    output->size = input.size;
    return;
}
