/*
 * perm_entropy — TDengine C UDF: aggregate permutation entropy
 *
 * Permutation entropy (Bandt & Pompe, PRL 2002) measures the complexity
 * of a time series by computing the Shannon entropy of the distribution of
 * ordinal patterns found in overlapping embedding windows of dimension
 * EMBED_DIM.  The result is normalised to [0, 1].
 *
 * This file is the CI build copy used by the function-test suite.
 * The canonical documented version lives at docs/examples/udf/perm_entropy.c.
 */

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "taosudf.h"

#define EMBED_DIM     5
#define DELAY         1
#define MAX_EMBED_DIM 8

typedef struct {
    int     embed_dim;
    int     delay;
    int64_t values_count;
    int64_t values_capacity;
    double *values;
} PermEntropyState;

static int32_t ensure_capacity(PermEntropyState *state, int64_t required) {
    if (required <= state->values_capacity) return TSDB_CODE_SUCCESS;

    int64_t new_cap = state->values_capacity > 0 ? state->values_capacity : 1024;
    while (new_cap < required) {
        if (new_cap > INT64_MAX / 2) { new_cap = required; break; }
        new_cap *= 2;
    }
    if (new_cap > (int64_t)(SIZE_MAX / sizeof(double))) return TSDB_CODE_OUT_OF_MEMORY;

    double *p = (double *)realloc(state->values, (size_t)new_cap * sizeof(double));
    if (p == NULL) return TSDB_CODE_OUT_OF_MEMORY;
    state->values          = p;
    state->values_capacity = new_cap;
    return TSDB_CODE_SUCCESS;
}

static double compute_perm_entropy(const double *data, int n, int embed_dim, int delay) {
    if (data == NULL || n < embed_dim || embed_dim <= 1 || embed_dim > MAX_EMBED_DIM)
        return 0.0;

    int n_windows  = n - (embed_dim - 1) * delay;
    if (n_windows <= 0) return 0.0;
    int n_patterns = 1;
    for (int i = 2; i <= embed_dim; i++) n_patterns *= i;

    int *counts = (int *)calloc(n_patterns, sizeof(int));
    if (counts == NULL) return 0.0;

    for (int w = 0; w < n_windows; w++) {
        double v[MAX_EMBED_DIM];
        int    idx[MAX_EMBED_DIM];
        int    rank[MAX_EMBED_DIM];
        for (int j = 0; j < embed_dim; j++) { v[j] = data[w + j * delay]; idx[j] = j; }
        for (int j = 0; j < embed_dim - 1; j++)
            for (int k = j + 1; k < embed_dim; k++)
                if (v[idx[j]] > v[idx[k]] ||
                    (v[idx[j]] == v[idx[k]] && idx[j] > idx[k])) {
                    int t = idx[j]; idx[j] = idx[k]; idx[k] = t;
                }
        for (int j = 0; j < embed_dim; j++) rank[idx[j]] = j;
        int pat = 0;
        for (int j = 0; j < embed_dim; j++) {
            int c = 0;
            for (int k = j + 1; k < embed_dim; k++) if (rank[k] < rank[j]) c++;
            pat = pat * (embed_dim - j) + c;
        }
        counts[pat]++;
    }

    double entropy = 0.0;
    for (int i = 0; i < n_patterns; i++) {
        if (counts[i] > 0) {
            double p = (double)counts[i] / n_windows;
            entropy -= p * log2(p);
        }
    }
    free(counts);

    double max_entropy = log2((double)n_patterns);
    return max_entropy > 0 ? entropy / max_entropy : 0.0;
}

DLL_EXPORT int32_t perm_entropy_init()    { return TSDB_CODE_SUCCESS; }
DLL_EXPORT int32_t perm_entropy_destroy() { return TSDB_CODE_SUCCESS; }

DLL_EXPORT int32_t perm_entropy_start(SUdfInterBuf *interBuf) {
    if (interBuf->bufLen < (int32_t)sizeof(PermEntropyState))
        return TSDB_CODE_UDF_INVALID_BUFSIZE;
    PermEntropyState *state = (PermEntropyState *)interBuf->buf;
    memset(state, 0, sizeof(PermEntropyState));
    state->embed_dim = EMBED_DIM;
    state->delay     = DELAY;
    return TSDB_CODE_SUCCESS;
}

DLL_EXPORT int32_t perm_entropy(SUdfDataBlock *block, SUdfInterBuf *interBuf,
                                SUdfInterBuf *newInterBuf) {
    if (block->numOfCols != 1) return TSDB_CODE_UDF_INVALID_INPUT;
    SUdfColumn *col = block->udfCols[0];

    switch (col->colMeta.type) {
        case TSDB_DATA_TYPE_TINYINT:
        case TSDB_DATA_TYPE_SMALLINT:
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_FLOAT:
        case TSDB_DATA_TYPE_DOUBLE:
            break;
        default:
            return TSDB_CODE_UDF_INVALID_INPUT;
    }

    int64_t valid = 0;
    for (int32_t i = 0; i < block->numOfRows; i++)
        if (!udfColDataIsNull(col, i)) valid++;

    PermEntropyState newState = *(PermEntropyState *)interBuf->buf;

    if (valid > 0) {
        int32_t code = ensure_capacity(&newState, newState.values_count + valid);
        if (code != TSDB_CODE_SUCCESS) {
            PermEntropyState *orig = (PermEntropyState *)interBuf->buf;
            free(orig->values);
            orig->values = NULL;
            return code;
        }
        for (int32_t i = 0; i < block->numOfRows; i++) {
            if (udfColDataIsNull(col, i)) continue;
            char  *raw = udfColDataGetData(col, i);
            double v   = 0.0;
            switch (col->colMeta.type) {
                case TSDB_DATA_TYPE_TINYINT:  v = (double)(*(int8_t  *)raw);  break;
                case TSDB_DATA_TYPE_SMALLINT: v = (double)(*(int16_t *)raw);  break;
                case TSDB_DATA_TYPE_INT:      v = (double)(*(int32_t *)raw);  break;
                case TSDB_DATA_TYPE_BIGINT:   v = (double)(*(int64_t *)raw);  break;
                case TSDB_DATA_TYPE_FLOAT:    v = (double)(*(float   *)raw);  break;
                case TSDB_DATA_TYPE_DOUBLE:   v = *(double *)raw;             break;
                default: continue;
            }
            newState.values[newState.values_count++] = v;
        }
    }

    if (newInterBuf->buf == NULL ||
        newInterBuf->bufLen < (int32_t)sizeof(PermEntropyState)) {
        /* Free the heap array unconditionally and clear orig->values:
         *  - realloc moved array: newState.values is the new block;
         *    orig->values is already dangling (freed internally by realloc).
         *  - realloc in-place or no realloc: newState.values == orig->values;
         *    freeing once via newState.values is correct.
         * Clear orig->values so freeUdfInterBuf() cannot double-free. */
        PermEntropyState *orig = (PermEntropyState *)interBuf->buf;
        free(newState.values);
        newState.values = NULL;
        if (orig != NULL) orig->values = NULL;
        return TSDB_CODE_UDF_INVALID_BUFSIZE;
    }
    memcpy(newInterBuf->buf, &newState, sizeof(PermEntropyState));
    newInterBuf->bufLen      = sizeof(PermEntropyState);
    newInterBuf->numOfResult = 0;
    return TSDB_CODE_SUCCESS;
}

DLL_EXPORT int32_t perm_entropy_finish(SUdfInterBuf *interBuf,
                                       SUdfInterBuf *resultData) {
    if (interBuf->buf == NULL) { resultData->numOfResult = 0; return TSDB_CODE_SUCCESS; }

    PermEntropyState *state = (PermEntropyState *)interBuf->buf;

    if (state->values_count < state->embed_dim || state->values == NULL) {
        if (state->values != NULL) { free(state->values); state->values = NULL; }
        resultData->numOfResult = 0;
        return TSDB_CODE_SUCCESS;
    }

    double entropy = compute_perm_entropy(state->values, (int)state->values_count,
                                          state->embed_dim, state->delay);

    if (resultData->buf == NULL || resultData->bufLen < (int32_t)sizeof(double)) {
        free(state->values); state->values = NULL;
        return TSDB_CODE_UDF_INVALID_BUFSIZE;
    }
    *(double *)resultData->buf = entropy;
    resultData->bufLen         = sizeof(double);
    resultData->numOfResult    = 1;

    free(state->values);
    state->values          = NULL;
    state->values_count    = 0;
    state->values_capacity = 0;
    return TSDB_CODE_SUCCESS;
}
