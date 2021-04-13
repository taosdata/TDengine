#include "taos.h"
#include "callback_udf_func.h"
#if 0
#include "libmseed.h"
#endif


typedef struct cb_udf_samples_s {
  float    samples[131072];
  int64_t  time[131072];
  int      first_call;
  int      offset;
  int      count;
} cb_udf_samples_t;


typedef struct cb_udf_params_s {
  cb_udf_samples_t       history;
  FilterPicker5_Memory  *memory;
} cb_udf_params_t;


void
callback_udf_func(char *data, short itype, short ibytes, int numOfRows, long long *ts, char *dataOutput, char *tsOutput, int *numOfOutput, short otype, short obytes, SUdfInit *buf)
{
    BOOLEAN_INT            useMemory        = TRUE_INT;
    double                 longTermWindow   = 10.0; 
    double                 threshold1       = 8.0;
    double                 threshold2       = 8.0;
    double                 tUpEvent         = 0.5;
    double                 filterWindow     = 4.0;
    double                 dt               = 0.01;
    long                   iFilterWindow;
    long                   ilongTermWindow;
    long                   itUpEvent;
    int                    flen;
    int                    llen;
    int                    index, n, hcount;
    int                    valid;
    int                    num_picks        = 0;
#if 0
    char                   timestr[64];
#endif
    char                  *cp;
    short                 *sp;
    long long             *llp;
    int                   *ip;
    float                 *fp;
    float                 *amps             = NULL;
    FilterPicker5_Memory  *mem              = NULL;
    PickData              *pick             = NULL;
    PickData             **pick_list        = NULL;
    cb_udf_params_t       *cbp              = NULL;

    filterWindow = 300.0 * dt;
    iFilterWindow = (long) (0.5 + filterWindow * 1000.0);
    if (iFilterWindow > 1) {
        filterWindow = (double) iFilterWindow / 1000.0;
    }

    longTermWindow = 500.0 * dt; 
    ilongTermWindow = (long) (0.5 + longTermWindow * 1000.0);
    if (ilongTermWindow > 1) {
        longTermWindow = (double) ilongTermWindow / 1000.0;
    }

    tUpEvent = 20.0 * dt;
    itUpEvent = (long) (0.5 + tUpEvent * 1000.0);
    if (itUpEvent > 1) {
        tUpEvent = (double) itUpEvent / 1000.0;
    }

    amps = (float *) malloc(numOfRows * sizeof(float));
    if (amps == NULL) {
        fprintf(stderr, "failed to allocate for amps\r\n");
        return;
    }

    valid = 0;
    memset(amps, 0, numOfRows * sizeof(float));

    switch (itype) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
        for (n = 0; n < numOfRows; n++) {
            amps[n] = (float) data[n];
        }
        break;
    case TSDB_DATA_TYPE_SMALLINT:
        sp = (short *) data;
        for (n = 0; n < numOfRows; n++) {
            amps[n] = (float) sp[n];
        }
        break;
    case TSDB_DATA_TYPE_INT:
        ip = (int *) data;
        for (n = 0; n < numOfRows; n++) {
            amps[n] = (float) ip[n];
        }
        break;
    case TSDB_DATA_TYPE_FLOAT:
        fp = (float *) data;
        for (n = 0; n < numOfRows; n++) {
            amps[n] = fp[n];
        }
        break;
    default:
        return;
    }

    /* applications always pass in buf */
    if (buf && buf->ptr) {
        cbp = (cb_udf_params_t *) buf->ptr;
        mem = cbp->memory;
    }

    Pick(0.01,
         amps,
         numOfRows,
         filterWindow,   //多少道滤波
         longTermWindow, //长期平均值时间窗
         threshold1,     //平均值阈值
         threshold2,     //积分阈值
         tUpEvent,       //积分时间窗
         &mem,
         useMemory,
         &pick_list,
         &num_picks,
         "UDF"
    );

    hcount = (int) longTermWindow * 100;

    for (n = 0; n < num_picks; n++) {
        pick = *(pick_list + n);
        index = (int) (pick->indices[0] * 0.5 + pick->indices[1] * 0.5);

        if (index < 0) {
            if (cbp == NULL) {
                continue;
            }

            if (cbp->history.first_call) {
                fprintf(stderr, "no history data\r\n");
                continue;
            } else {
                if (cbp->history.count + index < 0) {
                    fprintf(stderr, "no enough history data\r\n");
                    continue;
                }

                if (dataOutput) {
                    switch(otype) {
                    case TSDB_DATA_TYPE_BOOL:
                    case TSDB_DATA_TYPE_TINYINT:
                        cp = &dataOutput[valid++];
                        *cp = (char) cbp->history.samples[cbp->history.count + index];
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                        sp = (short *) &dataOutput[valid];
                        valid += 2;
                        *sp = (short) cbp->history.samples[cbp->history.count + index];
                        break;
                    case TSDB_DATA_TYPE_INT:
                        ip = (int *) &dataOutput[valid];
                        valid += 4;
                        *ip = (int) cbp->history.samples[cbp->history.count + index];
                        break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        llp = (long long *) &dataOutput[valid];
                        valid += 8;
                        *llp = (long long) cbp->history.samples[cbp->history.count + index];
                        break;
                    default:
                        return;
                    }
                }

                if (tsOutput) {
                    llp = (long long *) tsOutput;
                    llp[valid++] = cbp->history.time[cbp->history.count + index];
                }
#if 0
                ms_nstime2timestrz(cbp->history.time[cbp->history.count + index], timestr, ISOMONTHDAY, MICRO);
                fprintf(stderr, "%s %f\n", timestr, (float) amps[index]);
#endif
            }
        } else {
            if (dataOutput) {
                switch(otype) {
                case TSDB_DATA_TYPE_BOOL:
                case TSDB_DATA_TYPE_TINYINT:
                    cp = &dataOutput[valid++];
                    *cp = (char) amps[index];
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                    sp = (short *) &dataOutput[valid];
                    valid += 2;
                    *sp = (short) amps[index];
                    break;
                case TSDB_DATA_TYPE_INT:
                    ip = (int *) &dataOutput[valid];
                    valid += 4;
                    *ip = (int) amps[index];
                    break;
                case TSDB_DATA_TYPE_TIMESTAMP:
                    llp = (long long *) &dataOutput[valid];
                    valid += 8;
                    *llp = (long long) amps[index];
                    break;
                default:
                    return;
                }
            }

            if (tsOutput) {
                llp = (long long *) tsOutput;
                llp[valid++] = ts[index];
            }

#if 0
            ms_nstime2timestrz(ts[index], timestr, ISOMONTHDAY, MICRO);
            fprintf(stderr, "%s %f\n", timestr, (float) amps[index]);
#endif
        }

        if (cbp == NULL) {
            continue;
        }

        flen = sizeof(float);
        llen = sizeof(int64_t);

        if (cbp->history.first_call) {
            cbp->history.first_call = 0;

            if (numOfRows < hcount) {
                memcpy((void *) cbp->history.samples, (void *) amps, numOfRows * flen);
                memcpy((void *) cbp->history.time, (void *) ts, numOfRows * llen);
                cbp->history.offset = numOfRows;
                cbp->history.count = numOfRows;
            } else {
                memcpy((void *) cbp->history.samples, (void *) &amps[numOfRows - hcount], hcount * flen);
                memcpy((void *) cbp->history.time, (void *) &ts[numOfRows - hcount], hcount * llen);
                cbp->history.count = hcount;
            }
        } else {
            if (numOfRows + cbp->history.count < hcount) {
                memcpy((void *) &cbp->history.samples[cbp->history.offset], (void *) amps, numOfRows * flen);
                memcpy((void *) &cbp->history.time[cbp->history.offset], (void *) ts, numOfRows * llen);
                cbp->history.offset += numOfRows;
                cbp->history.count += numOfRows;
            } else {
                if (numOfRows >= hcount) {
                    memcpy((void *) cbp->history.samples, (void *) &amps[numOfRows - hcount], hcount * flen);
                    memcpy((void *) cbp->history.time, (void *) &ts[numOfRows - hcount], hcount * llen);
                } else {
                    memmove((void *) cbp->history.samples, (void *) &cbp->history.samples[cbp->history.count + numOfRows - hcount], (hcount - numOfRows) * flen);
                    memmove((void *) &cbp->history.samples[hcount - numOfRows], (void *) amps, numOfRows * flen);

                    memmove((void *) cbp->history.time, (void *) &cbp->history.time[cbp->history.count + numOfRows - hcount], (hcount - numOfRows) * llen);
                    memmove((void *) &cbp->history.time[hcount - numOfRows], (void *) ts, numOfRows * llen);
                }

                cbp->history.offset = 0;
                cbp->history.count = hcount;
            }
        }
    }

    /* applications always pass in buf */
    if (buf) {
        cbp->memory = mem;
        buf->ptr = (char *) cbp;
    } else {
        free_FilterPicker5_Memory(&mem);
    }

    if (numOfOutput) {
        *numOfOutput = num_picks;
    }

    free(amps);

    if (pick_list) {
        free(pick_list);
    }
}


int
callback_udf_func_init(SUdfInit *buf)
{
    if (buf == NULL) {
        return 0;
    }

    buf->ptr = (char *) malloc(sizeof(cb_udf_params_t));
    if (buf->ptr == NULL) {
        return -1;
    }

    return 0;
}


void
callback_udf_func_destroy(SUdfInit *buf)
{
    cb_udf_params_t      *cbp;
    FilterPicker5_Memory *mem;

    if (buf == NULL || buf->ptr == NULL) {
        return;
    }

    cbp = (cb_udf_params_t *) buf->ptr;

    mem = cbp->memory;
    free_FilterPicker5_Memory(&mem);

    free(buf->ptr);
    buf->ptr = NULL;
}
