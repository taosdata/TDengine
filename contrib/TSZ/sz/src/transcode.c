/**
 * @file transcode.c
 * @author Yu Zhong (lx1zhong@qq.com)
 * @brief Source file for ADT-FSE algorithm
 * @date 2022-08-29
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "transcode.h"
#include "fse.h"
#include "bitstream.h"
#include <stdlib.h>


#ifdef WINDOWS
int32_t BUILDIN_CLZ_EX(uint32_t val) {
  unsigned long r = 0;
  _BitScanReverse(&r, val);
  return (int)(31 - r);
}
#endif

/**
 * @brief transform type array to FseCode & tranCodeBits
 * [type] ---minus md---> [factor] ---transcode---> [tp_code] + [bitstream of diff]
 * 
 */
void encode_with_fse(int *type, size_t dataSeriesLength, unsigned int intervals, 
                    unsigned char **FseCode, size_t *FseCode_size, 
                    unsigned char **transCodeBits, size_t *transCodeBits_size) 
{
    int nbits = 0;
    int dstCapacity = dataSeriesLength * sizeof(int);
    uint8_t *tp_code = (uint8_t *)malloc(dataSeriesLength);
    BIT_CStream_t transCodeStream;
    (*transCodeBits) = (unsigned char*)malloc(dstCapacity);
    BIT_initCStream(&transCodeStream, (void *)(*transCodeBits), dstCapacity);

    // transcoding
    int md = intervals / 2;
    uint8_t*   type2code = (uint8_t *)malloc(intervals * sizeof(uint8_t));
    uint32_t*  diff      = (uint32_t *)malloc(intervals * sizeof(uint32_t));

    for (int i = md; i < intervals; i++)
    {
        type2code[i] = (uint8_t)Int2code(i - md);
        diff[i] = i - md - code2int[type2code[i]][0];
    }

    for (int i = md - 1; i > 0; i--) 
    {
        type2code[i] = 67 - type2code[2 * md - i];
        diff[i] = diff[2*md-i];
    }

    for (int i = 0; i < dataSeriesLength; i++) 
    {
        if (type[i] == 0) 
        {
            // unpredictable data
            tp_code[i] = 67;
            nbits = 0;
        }
        else 
        {
            tp_code[i] = type2code[type[i]];
            nbits = code2int[tp_code[i]][1];
            BIT_addBitsFast(&transCodeStream, diff[type[i]], nbits);
            BIT_flushBitsFast(&transCodeStream);
        }
    }

    (*FseCode) = (unsigned char*)malloc(2 * dataSeriesLength);
    size_t fse_size = FSE_compress((*FseCode), 2 * dataSeriesLength, tp_code, dataSeriesLength);
    if (FSE_isError(fse_size)) 
    {
        printf("ADT-FSE: FSE_compress error!\n");
        exit(1);
    }
    (*FseCode_size) = fse_size;

    size_t const streamSize = BIT_closeCStream(&transCodeStream);
    (*transCodeBits_size) = streamSize;
    if (streamSize == 0) 
    {
        printf("ADT-FSE: transCodeBits_size too small!\n");
        exit(1);
    }
    free(tp_code);
    free(type2code);
    free(diff);

    return;
}

/**
 * @brief transform FseCode & tranCodeBits to type array 
 * 
 */
void decode_with_fse(int *type, size_t dataSeriesLength, unsigned int intervals, 
                    unsigned char *FseCode, size_t FseCode_size, 
                    unsigned char *transCodeBits, size_t transCodeBits_size) 
{
    uint8_t *tp_code = (uint8_t *)malloc(dataSeriesLength);

    if (FseCode_size <= 1) 
    {
        // all zeros
        memset((void *)type, 0, sizeof(int) * dataSeriesLength);
        free(tp_code);
        return;
    }
    size_t fse_size = FSE_decompress(tp_code, dataSeriesLength, FseCode, FseCode_size);
    if (FSE_isError(fse_size)) 
    {
        printf("ADT-FSE: FSE_decompress error!\n");
        exit(1);
    }
    if (fse_size != dataSeriesLength) 
    {
        printf("ADT-FSE: FSE_decompress error! fse_size(%lu) != dataSeriesLength(%lu)!\n", fse_size, dataSeriesLength);
        exit(1);
    }

    BIT_DStream_t transCodeStream;
    size_t stream_size = BIT_initDStream(&transCodeStream, transCodeBits, transCodeBits_size);
    if (stream_size == 0) 
    {
        printf("ADT-FSE: transcode stream empty!\n");
        exit(1);
    }

    int md = intervals / 2;
    int code2type[67];
    for (int i = 0; i < 67; i++) 
    {
        code2type[i] = code2int[i][0] + md;
    }

    int nbits;
    size_t diff;
    for (int i = dataSeriesLength-1; i >= 0; i--) 
    {
        if (tp_code[i] == 67) {
            type[i] = 0;
            continue;
        }
        nbits = code2int[tp_code[i]][1];

        if (nbits >= 1)
            diff = BIT_readBitsFast(&transCodeStream, nbits);
        else
            diff =0;
        BIT_reloadDStream(&transCodeStream);
        
        if (tp_code[i] < 34) {
            // positive number
            type[i] = code2type[tp_code[i]] + diff;
        }
        else {
            // negative number
            type[i] = code2type[tp_code[i]] - diff;
        }
    }
    free(tp_code);

    return;
}

