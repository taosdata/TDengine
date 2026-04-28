/*
 * perm_entropy — TDengine C UDF: aggregate permutation entropy
 *
 * Permutation entropy (Bandt & Pompe, PRL 2002) measures the complexity
 * of a time series by computing the Shannon entropy of the distribution of
 * ordinal patterns (permutation patterns) found in overlapping embedding
 * windows of dimension EMBED_DIM.  The result is normalised to [0, 1].
 *
 * This function exemplifies the "accumulate-all-data-then-compute" pattern,
 * which is required whenever the algorithm cannot produce a partial result
 * from a single data chunk:
 *
 *   perm_entropy_start   — initialise state in the framework-provided buffer
 *   perm_entropy         — append each delivered chunk to a heap-allocated
 *                          values array kept inside the intermediate buffer
 *   perm_entropy_finish  — compute the final result from all accumulated
 *                          values, release the heap array, write the result
 *
 * =========================================================================
 * Memory management rules for TDengine aggregate UDFs
 * =========================================================================
 * Rule 1 — Never replace the framework-provided buffer pointer.
 *   The framework calls taosMemoryMalloc(bufSize) before every AGG_PROC
 *   invocation and stores the result in interBuf->buf / newInterBuf->buf.
 *   If the UDF overwrites these pointers with its own malloc the original
 *   allocation leaks (bufSize × number-of-AGG_PROC-calls bytes total).
 *   Always write state into the pre-allocated buffer via memcpy.
 *
 * Rule 2 — The UDF owns every heap pointer stored inside the state struct.
 *   freeUdfInterBuf() frees only the container buffer (interBuf->buf), not
 *   any pointers embedded in the state.  The UDF must release state->values
 *   in perm_entropy_finish and in every error path that abandons the state.
 *
 * =========================================================================
 * Compile:
 *   gcc -fPIC -shared perm_entropy.c \
 *       -I/usr/local/taos/include \
 *       -lm -o libperm_entropy.so
 *
 * Register:
 *   CREATE AGGREGATE FUNCTION perm_entropy
 *     AS '/path/to/libperm_entropy.so'
 *     OUTPUTTYPE DOUBLE
 *     BUFSIZE 256;
 * =========================================================================
 */

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"
#include "taoserror.h"
#include "taosudf.h"

#define EMBED_DIM     5
#define DELAY         1
#define MAX_EMBED_DIM 8

/* Intermediate state stored inside interBuf->buf.
 * The 'values' pointer is heap-allocated by the UDF and must be freed
 * by perm_entropy_finish. */
typedef struct {
    int     embed_dim;
    int     delay;
    int64_t values_count;
    int64_t values_capacity;
    double *values;   /* heap array – owned by the UDF, NOT by the framework */
} PermEntropyState;

/* ------------------------------------------------------------------ helpers */

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

        /* insertion sort by value (stable: tie-break by index) */
        for (int j = 0; j < embed_dim - 1; j++)
            for (int k = j + 1; k < embed_dim; k++)
                if (v[idx[j]] > v[idx[k]] ||
                    (v[idx[j]] == v[idx[k]] && idx[j] > idx[k])) {
                    int t = idx[j]; idx[j] = idx[k]; idx[k] = t;
                }

        for (int j = 0; j < embed_dim; j++) rank[idx[j]] = j;

        /* Lehmer code → pattern index */
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

/* ------------------------------------------------------------------ UDF API */

DLL_EXPORT int32_t perm_entropy_init()    { return TSDB_CODE_SUCCESS; }
DLL_EXPORT int32_t perm_entropy_destroy() { return TSDB_CODE_SUCCESS; }

DLL_EXPORT int32_t perm_entropy_start(SUdfInterBuf *interBuf) {
    if (interBuf->bufLen < (int32_t)sizeof(PermEntropyState)) {
        udfError("perm_entropy_start: bufLen %d < required %d",
                 interBuf->bufLen, (int32_t)sizeof(PermEntropyState));
        return TSDB_CODE_UDF_INVALID_BUFSIZE;
    }
    /* Write directly into the framework-provided buffer – do NOT malloc. */
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

    /* Reject non-numeric column types up front. */
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

    /* Count valid (non-NULL) rows in this chunk. */
    int64_t valid = 0;
    for (int32_t i = 0; i < block->numOfRows; i++)
        if (!udfColDataIsNull(col, i)) valid++;

    /* Work on a value copy of the current state.  At the end we commit it
     * back into the framework container newInterBuf->buf via memcpy.
     */
    PermEntropyState newState = *(PermEntropyState *)interBuf->buf;

    if (valid > 0) {
        int32_t code = ensure_capacity(&newState, newState.values_count + valid);
        if (code != TSDB_CODE_SUCCESS) {
            /* realloc leaves the original pointer intact on failure, so
             * newState.values still aliases interBuf->buf->values.
             * Free the heap array through the framework buffer;
             * freeUdfInterBuf() only frees the container, not this pointer. */
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

    /*
     * Commit the updated state into the framework's pre-allocated
     * newInterBuf->buf via memcpy.  The framework owns this buffer;
     * never replace the pointer with a new allocation.
     */
    if (newInterBuf->buf == NULL ||
        newInterBuf->bufLen < (int32_t)sizeof(PermEntropyState)) {
        udfError("perm_entropy: newInterBuf too small or NULL (bufLen=%d, required=%d)",
                 newInterBuf->bufLen, (int32_t)sizeof(PermEntropyState));
        /* Free the heap array unconditionally and clear orig->values:
         *  - realloc moved array: newState.values is the new block;
         *    orig->values is already dangling (freed internally by realloc).
         *  - realloc in-place or no realloc: newState.values == orig->values;
         *    freeing once via newState.values is correct.
         * Clear orig->values in both cases so freeUdfInterBuf() cannot double-free. */
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
        /* Free heap array before returning an insufficient-data result. */
        if (state->values != NULL) { free(state->values); state->values = NULL; }
        resultData->numOfResult = 0;
        return TSDB_CODE_SUCCESS;
    }

    double entropy = compute_perm_entropy(state->values, (int)state->values_count,
                                          state->embed_dim, state->delay);

    /* resultData->buf is also pre-allocated by the framework. */
    if (resultData->buf == NULL ||
        resultData->bufLen < (int32_t)sizeof(double)) {
        udfError("perm_entropy_finish: resultData buf too small or NULL");
        free(state->values); state->values = NULL;
        return TSDB_CODE_UDF_INVALID_BUFSIZE;
    }
    *(double *)resultData->buf = entropy;
    resultData->bufLen         = sizeof(double);
    resultData->numOfResult    = 1;

    /* Free the heap array now that computation is complete. */
    free(state->values);
    state->values          = NULL;
    state->values_count    = 0;
    state->values_capacity = 0;
    return TSDB_CODE_SUCCESS;
}
