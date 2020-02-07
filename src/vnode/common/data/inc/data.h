#if !defined(_TD_DATA_H_)
#define _TD_DATA_H_

#include <stdint.h>

// ---- Row data interface
typedef struct {
    int32_t numOfRows;
    char * data;
} SRData;

// ---- Column data interface
typedef struct {
    int32_t numOfPoints;
    char *data;
} SCData;

#endif // _TD_DATA_H_
