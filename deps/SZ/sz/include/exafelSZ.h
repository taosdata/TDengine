#ifndef EXAFELSZ_H
#define EXAFELSZ_H
#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdlib.h>

typedef struct exafelSZ_params{
  //uint8_t *peaks;
  uint16_t *peaksSegs;
  uint16_t *peaksRows;
  uint16_t *peaksCols;
  uint64_t numPeaks;

  uint8_t *calibPanel;

  uint8_t binSize; //Binning: (pr->binSize x pr->binSize) to (1 x 1)
  double tolerance; //SZ pr->tolerance
  uint8_t szDim; //1D/2D/3D compression/decompression
  //uint8_t szBlockSize; //Currently unused
  uint8_t peakSize; //MUST BE ODD AND NOT EVEN! Each peak will have size of: (peakSize x peakSize)
 
  // uint64_t nEvents;
  // uint64_t panels;
  // uint64_t rows;
  // uint64_t cols;
  
  //CALCULATED VARIBALES:
  uint64_t binnedRows;
  uint64_t binnedCols;
  uint8_t peakRadius; //Will be calculated using peakSize

} exafelSZ_params;


void exafelSZ_params_process(exafelSZ_params*pr, size_t panels, size_t rows, size_t cols);
void exafelSZ_params_checkDecomp(exafelSZ_params*pr, size_t panels, size_t rows, size_t cols);
void exafelSZ_params_checkComp(exafelSZ_params*pr, size_t panels, size_t rows, size_t cols);

unsigned char * exafelSZ_Compress(void* _pr,
                         void* _origData,
                        size_t events, size_t panels, size_t rows, size_t cols,
                        size_t *compressedSize);
					   
void* exafelSZ_Decompress(void *_pr,
                         unsigned char*_compressedBuffer,
                         size_t events, size_t panels, size_t rows, size_t cols,
                         size_t compressedSize);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _EXAFELSZ_H  ----- */

