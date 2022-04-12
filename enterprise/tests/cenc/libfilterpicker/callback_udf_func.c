#include "callback_udf_func.h"
#if 0
#include "libmseed.h"
#endif


void
callback_udf_func(char *data, char type, int numOfRows, long long *ts, char *dataOutput, char *tsOutput, int *numOfOutput, SUdfInit *buf)
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
    int                    index, n;
    int                    valid;
    int                    num_picks        = 0;
#if 0
    char                   timestr[64];
#endif
    char                  *cp;
    short                 *sp;
    long long             *llp;
    float                 *fp;
    float                 *amps             = NULL;
    FilterPicker5_Memory  *mem              = NULL;
    PickData              *pick             = NULL;
    PickData             **pick_list        = NULL;

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

    amps = (float *) taosMemoryMalloc(numOfRows * sizeof(float));
    if (amps == NULL) {
        fprintf(stderr, "failed to allocate for amps\r\n");
        return;
    }

    valid = 0;
    memset(amps, 0, numOfRows * sizeof(float));

    switch (type) {
    case 1:
        for (n = 0; n < numOfRows; n++) {
            amps[n] = (float) data[n];
        }
        break;
    case 2:
        sp = (short *) data;
        for (n = 0; n < numOfRows; n++) {
            amps[n] = (float) sp[n];
        }
        break;
    case 4:
        fp = (float *) data;
        for (n = 0; n < numOfRows; n++) {
            amps[n] = (float) fp[n];
        }
        break;
    default:
        return;
    }

    /* applications always pass in buf */
    //if (buf) {
    //    mem = (FilterPicker5_Memory *) buf->ptr;
    //}

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

    for (n = 0; n < num_picks; n++) {
        pick = *(pick_list + n);
        index = (int) (pick->indices[0] * 0.5 + pick->indices[1] * 0.5);

        if (index < 0) {
            continue;
        }

        if (dataOutput) {
            switch(type) {
            case 1:
                cp = &dataOutput[valid++];
                *cp = (char) amps[index];
                break;
            case 2:
                sp = (short *) &dataOutput[valid];
                valid += 2;
                *sp = (short) amps[index];
                break;
            case 4:
                fp = (float *) &dataOutput[valid];
                valid += 4;
                *fp = (float) amps[index];
                break;
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

    /* applications always pass in buf */
    //if (buf) {
    //    buf->ptr = (char *) mem;
    //} else {
        free_FilterPicker5_Memory(&mem);
    //}

    if (numOfOutput) {
        *numOfOutput = num_picks;
    }

    taosMemoryFree(amps);

    if (pick_list) {
        taosMemoryFree(pick_list);
    }
}
