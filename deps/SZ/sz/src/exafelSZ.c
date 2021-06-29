#ifdef __cplusplus
extern "C" {
#endif

#include "sz.h"

void exafelSZ_params_process(exafelSZ_params*pr, size_t panels, size_t rows, size_t cols){
  pr->binnedRows=(rows+pr->binSize-1)/pr->binSize;
  pr->binnedCols=(cols+pr->binSize-1)/pr->binSize;
  
  pr->peakRadius=(pr->peakSize-1)/2;
}

void exafelSZ_params_checkDecomp(exafelSZ_params*pr, size_t panels, size_t rows, size_t cols){
  if(pr->calibPanel==NULL){
    printf("ERROR: calibPanel is NULL : calibPanel=%ld\n",(long)pr->calibPanel);
    assert(0);
  }
  if(pr->binSize<1 || pr->tolerance<0 || pr->szDim<1 || pr->szDim>3){
    printf("ERROR: Something wrong with the following:\n");
    printf("binSize=%d\n",(int)pr->binSize);
    printf("tolerance=%d\n",(int)pr->tolerance);
    printf("szDim=%d\n",(int)pr->szDim);
    assert(0);
  }
  if(!(pr->peakSize%2)){
    printf("ERROR: peakSize = %d cannot be even. It must be odd!\n",(int)pr->peakSize);
    assert(0);
  }  
  //if(nEvents<1 || panels<1 || rows<1 || cols<1){
  if(panels<1 || rows<1 || cols<1){
    printf("ERROR: Something wrong with the following:\n");
    printf("panels=%d\n",(int)panels);
    printf("rows=%d\n",(int)rows);
    printf("cols=%d\n",(int)cols);
    assert(0);
  }
}

void exafelSZ_params_checkComp(exafelSZ_params*pr, size_t panels, size_t rows, size_t cols){
  if(pr->peaksSegs==NULL || pr->peaksRows==NULL || pr->peaksCols==NULL){
    printf("ERROR: One or more of the following are NULL : peaksSegs , peaksRows , peaksCols\n");
    assert(0);
  }
  exafelSZ_params_checkDecomp(pr, panels, rows, cols);
}

void exafelSZ_params_print(exafelSZ_params*pr){
  printf("Configuration (exafelSZ_params) :\n");
  printf("binSize: %d\n",pr->binSize);
  printf("tolerance:%e\n",pr->tolerance);
  printf("szDim:%d\n",pr->szDim);
  printf("peakSize:%d\n",pr->peakSize);
  //printf("nEvents:%d\n",pr->nEvents);
  //printf("panels:%d\n",pr->panels);
  //printf("rows:%d\n",pr->rows);
  //printf("cols:%d\n",pr->cols);
  printf("\n");
  printf("CALCULATED VARIABLES\n");
  printf("binnedRows:%ld\n",pr->binnedRows);
  printf("binnedCols:%ld\n",pr->binnedCols);
  printf("peakRadius:%d\n",pr->peakRadius);
  printf("\n");
  // outs<<"Configuration (exafelSZ_params) : "<<endl;
  // outs<<"SMOOTHING: NO"<<"  (ROI and RONI are NOT replaced by local avg values)"<<endl;
  // outs<<"binSize:"<<binSize<<endl;
  // outs<<"tolerance:"<<tolerance<<endl;
  // outs<<"szDim:"<<szDim<<endl;
  // outs<<"peakSize:"<<peakSize<<endl;
  // outs<<"nEvents:"<<nEvents<<" (# of events per batch)"<<endl;
  // outs<<"panels:"<<panels<<" (Panels per event)"<<endl;
  // outs<<"rows:"<<rows<<" (Rows per panel)"<<endl;
  // outs<<"cols:"<<cols<<" (Columns per panel)"<<endl;
  // outs<<endl;
  // outs<<"CALCULATED VARIABLES"<<endl;
  // outs<<"binnedRows:"<<binnedRows<<" (Rows per panel after binning)"<<endl;
  // outs<<"binnedCols:"<<binnedCols<<" (Columns per panel after binning)"<<endl;
  // outs<<"peakRadius:"<<peakRadius<<" (Peak radius = (peakSize-1)/2 )"<<endl;
  // outs<<endl;
}

//*********************************************************************************
//*********************************************************************************
//*********************************************************************************

//Index Calculator
static inline size_t calcIdx_4D(int i3, int i2, int i1, int i0, int size2, int size1, int size0){ 
  return i0+size0*(i1+size1*(i2+size2*i3));
}
static inline size_t calcIdx_3D(int i2, int i1, int i0, int size1, int size0){ 
  return i0+size0*(i1+size1*i2);
}
static inline size_t calcIdx_2D(int i1, int i0, int size0){ 
  return i0+size0*i1;
}

unsigned char * exafelSZ_Compress(void* _pr,
                       void* _origData,
                       size_t r4, size_t r3, size_t r2, size_t r1,
                       size_t *compressedSize)
{
  //printf("COMPRESS\n"); *compressedSize=0; return NULL;
  size_t nEvents,panels,rows,cols;
  if(r4==0)
    nEvents=1;
  else
    nEvents=r4;
  panels=r1;
  rows=r2;
  cols=r3;
  //printf("AMG : exafelSZ_Compress : nEvents,panels,rows,cols = %d , %d , %d , %d\n",nEvents,panels,rows,cols);

  float *origData=(float*)_origData;
  exafelSZ_params *pr=(exafelSZ_params*)_pr;  

  exafelSZ_params_process(pr, panels, rows, cols);
  exafelSZ_params_checkComp(pr, panels, rows, cols); 
  //exafelSZ_params_print(pr);  

  uint8_t *roiM=(uint8_t*)malloc(nEvents*panels*rows*cols) ;
  float *roiData=(float*)malloc(nEvents*panels*rows*cols*sizeof(float)) ;
  float *binnedData=(float*)malloc(nEvents*panels*pr->binnedRows*pr->binnedCols*sizeof(float)) ;
  //float *binnedData=(float*)malloc(nEvents*panels*rows*cols*sizeof(float)) ;
  
  size_t e,p,r,c,pk,ri,ci,br,bc,roii,bi;
  /*
  printf("AMG : exafelSZ_Compress : pr->numPeaks = %d\n",pr->numPeaks);
  printf("S:\n");
  for(e=0;e<pr->numPeaks;e++)
    printf("%d ",pr->peaksSegs[e]);
  printf("\nR:\n");
  for(e=0;e<pr->numPeaks;e++)
    printf("%d ",pr->peaksRows[e]);
  printf("\nC:\n");
  for(e=0;e<pr->numPeaks;e++)
    printf("%d ",pr->peaksCols[e]);
  printf("\n");
  */

  //Generate the ROI mask: NOTE: 0 means affirmative in ROI mask! This comes from the python scripts!
  //First, initialize with calibration panel:
  for(e=0;e<nEvents;e++){ //Event
    for(p=0;p<panels;p++){ //Panel
      for(r=0;r<rows;r++){ //Row
        for(c=0;c<cols;c++){ //Column
          //roiM[calcIdx_4D(e,p,r,c,panels,rows,cols)]=pr->calibPanel[calcIdx_2D(r,c,cols)]; //calibPanel is a single segment copied over all the event(image)
          roiM[calcIdx_4D(e,p,r,c,panels,rows,cols)]=pr->calibPanel[calcIdx_3D(p,r,c,rows,cols)];  //calibPanel is as big as the event(image) itself
        }
      }
    }
  }
  //uint64_t peaksBytePos=0; //Position in the peaks buffer
  //Now process the peaks and generate the mask:
  uint64_t nPeaksTotal=0;  //Total number of peaks
  for(e=0;e<nEvents;e++){ //Event
    //uint64_t nPeaks=*(uint64_t*)(&pr->peaks[peaksBytePos]);
    //peaksBytePos+=8;

    //peaksBytePos+=8;//Skip the second one! This is due to the problem in Python.

    nPeaksTotal+=pr->numPeaks;
    for(pk=0;pk<pr->numPeaks;pk++){
      //uint16_t p_=*(uint16_t*)(&pr->peaks[peaksBytePos]); //Panel for the current peak
      //peaksBytePos+=2;
      //uint16_t r_=*(uint16_t*)(&pr->peaks[peaksBytePos]); //Row for the current peak
      //peaksBytePos+=2;
      //uint16_t c_=*(uint16_t*)(&pr->peaks[peaksBytePos]); //Col for the current peak
      //peaksBytePos+=2;
      
      uint16_t p_=pr->peaksSegs[pk];
      uint16_t r_=pr->peaksRows[pk];
      uint16_t c_=pr->peaksCols[pk];

      if(p_>=panels){
        printf("ERROR: Peak coordinate out of bounds: Panel=%d, Valid range: 0,%d\n",(int)p_,(int)panels-1);
        assert(0);
        printf("Skipping this peak...\n");
        continue;
      }
      if(r_>=rows){
        printf("ERROR: Peak coordinate out of bounds: Row=%d, Valid range: 0,%d\n",(int)r_,(int)rows-1);
        assert(0);
        printf("Skipping this peak...\n");
        continue;
      }
      if(c_>=cols){
        printf("ERROR: Peak coordinate out of bounds: Col=%d, Valid range: 0,%d\n",(int)c_,(int)cols-1);
        assert(0);
        printf("Skipping this peak...\n");
        continue;
      }
      
      for(ri=r_-pr->peakRadius;ri<=r_+pr->peakRadius;ri++){  //ri: row index. Just a temporary variable.
        for(ci=c_-pr->peakRadius;ci<=c_+pr->peakRadius;ci++){  //ci: column index. Just a temporary variable.
          if(ri<rows && ci<cols){  //Check whether inside the bounds or not
            roiM[calcIdx_4D(e,p_,ri,ci,panels,rows,cols)]=0;
          }
        }
      }
    }
  }
  
  //Save ROI:
  uint64_t roiSavedCount=0;
  for(e=0;e<nEvents;e++){ //Event
    for(p=0;p<panels;p++){ //Panel
      for(r=0;r<rows;r++){ //Row
        for(c=0;c<cols;c++){ //Column
          if(!roiM[calcIdx_4D(e,p,r,c,panels,rows,cols)]){
            roiData[roiSavedCount]=origData[calcIdx_4D(e,p,r,c,panels,rows,cols)];
            roiSavedCount++;
          }
          
          //AMG: Replace ROI and RONI pixels with avg values!
          
        }
      }
    }
  }
  
  //Binning:
  for(e=0;e<nEvents;e++){ //Event
    for(p=0;p<panels;p++){  //Panel
      for(r=0;r<pr->binnedRows;r++){ //Row of the binnedData
        for(c=0;c<pr->binnedCols;c++){ //Column of the binnedData
          float sum=0;
          int nPts=0;
          for(br=0;br<pr->binSize;br++) //Bin Row (from origData)
            for(bc=0;bc<pr->binSize;bc++) //Bin Column (from origData)
              if(r*pr->binSize+br<rows && c*pr->binSize+bc<cols){
                // cout<<p<<" "<<r<<" "<<c<<" "<<br<<" "<<bc<<" "<<r*pr->binSize+br<<" "<<c*pr->binSize+bc<<endl;
                sum+=origData[calcIdx_4D(e,p,r*pr->binSize+br,c*pr->binSize+bc,panels,rows,cols)];
                nPts++;
              }
          // cout<<"p:"<<p<<" r:"<<r<<" c:"<<c<<" nPts:"<<nPts<<endl;
          binnedData[calcIdx_4D(e,p,r,c,panels,pr->binnedRows,pr->binnedCols)]=sum/nPts;
        }
      }
    }
  }

  //Additional compression using SZ:    
  size_t szCompressedSize=0;
  unsigned char* szComp;
   
  switch(pr->szDim){
    case 1:
      // szComp=sz_compress_3D(binnedData, 0, 0, nEvents * panels * pr->binnedRows * pr->binnedCols, pr->tolerance, szCompressedSize); //1D
      szComp=SZ_compress_args(SZ_FLOAT, binnedData, &szCompressedSize, SZ_ABS, pr->tolerance, 0, 0, 0, 0,0,0, nEvents * panels * pr->binnedRows * pr->binnedCols);
      break;
    case 2:
      // szComp=sz_compress_3D(binnedData, 0, nEvents * panels * pr->binnedRows, pr->binnedCols, pr->tolerance, szCompressedSize); //2D
      szComp=SZ_compress_args(SZ_FLOAT, binnedData, &szCompressedSize, SZ_ABS, pr->tolerance, 0, 0, 0, 0,0, nEvents * panels * pr->binnedRows, pr->binnedCols);
      break;
    case 3:
      // szComp=sz_compress_3D(binnedData, nEvents * panels, pr->binnedRows, pr->binnedCols, pr->tolerance, szCompressedSize); //3D
      szComp=SZ_compress_args(SZ_FLOAT, binnedData, &szCompressedSize, SZ_ABS, pr->tolerance, 0, 0, 0, 0, nEvents * panels, pr->binnedRows, pr->binnedCols);
      break;
    default:
      printf("ERROR: Wrong szDim : %d It must be 1,2 or 3.\n",(int)pr->szDim);
      assert(0);
  }
  
  /*      
  Compressed buffer format: (Types are indicated in parenthesis)
    WRITE: nPeaksTotal(uint64_t) (Total number of peaks in this batch)
    for(e=0;e<nEvents;e++){  (e for "event")
      WRITE: nPeaks[e]  (uint64_t) (Number of peaks in this event)
      for(p=0;p<nPeaks;p++){  (p for "peak")
       nPeaks{
         WRITE: peak[e][p] (uint16_t x 3)
       }
    }
    WRITE: roiSavedCount  (uint64_t) (How many pixels there are in the ROI data.)
       (roiSavedCount is the same # as # of 0's in ROI mask.) 
       (NOTE:0 means affirmative in ROI mask!)
    for(roii=0;roii<roiSavedCount;roii++){  (roii for "ROI data index")
      WRITE: ROI_data[roii]  (float, 32-bit)
    }
    WRITE: szCompressedSize  (uint64_t) (Compressed data size from SZ.)
    WRITE: szComp (unsigned char x SZ_compressed_buffer_size)  (Compressed data from SZ.)
    
    NOTE: Calibration panel is not saved. It should be handled by the user.
    
    SUMMARY:
    nPeaksTotal : 8 bytes : (1 x uint64_t)
    peaks : (8 x nEvents + nPeaksTotal x 3 x 2) bytes : (nEvents x (nPeaks + nPeaks x 3 x uint16_t))
    roiSavedCount : 8 Bytes : (1 x uint64_t)
    ROI_data : roiSavedCount x 4 : roiSavedCount x float 
    szCompressedSize : 8 : uint64_t
    szComp : szComp x 1 : szComp x (unsigned char)
  */
  (*compressedSize)=8+nEvents*8+nPeaksTotal*(2+2+2)+8+roiSavedCount*4+8+szCompressedSize;
  //compressedBuffer=new uint8_t[(*compressedSize)];
  uint8_t * compressedBuffer=(uint8_t*)malloc(*compressedSize);
  uint64_t bytePos;
  
  bytePos=0;
  //*(uint64_t*)(&compressedBuffer[bytePos])=nEvents;
  //bytePos+=8;
  *(uint64_t*)(&compressedBuffer[bytePos])=nPeaksTotal;
  bytePos+=8;
  // cout<<endl;
  // cout<<"COMPRESS:"<<endl;
  // cout<<"nPeaksTotal="<<nPeaksTotal<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("\nCOMPRESS:\n");
  //printf("nPeaksTotal=%d\n",nPeaksTotal);
  //printf("bytePos=%d\n",bytePos);
  
  //peaksBytePos=0;
  for(e=0;e<nEvents;e++){
    //uint64_t nPeaks=*(uint64_t*)(&pr->peaks[peaksBytePos]);
    //peaksBytePos+=8;
    ////peaksBytePos+=8;//Skip the second one. This is due to the error in Python!
    
    //*(uint64_t*)(&compressedBuffer[bytePos])=nPeaks;
    *(uint64_t*)(&compressedBuffer[bytePos])=pr->numPeaks;
    bytePos+=8;
    //for(pk=0;pk<nPeaks;pk++){
    for(pk=0;pk<pr->numPeaks;pk++){
      //*(uint16_t*)(&compressedBuffer[bytePos])=*(uint16_t*)(&pr->peaks[peaksBytePos]); //Panel for the current peak
      //bytePos+=2;
      //peaksBytePos+=2;
      //*(uint16_t*)(&compressedBuffer[bytePos])=*(uint16_t*)(&pr->peaks[peaksBytePos]); //Row for the current peak
      //bytePos+=2;
      //peaksBytePos+=2;      
      //*(uint16_t*)(&compressedBuffer[bytePos])=*(uint16_t*)(&pr->peaks[peaksBytePos]); //Column for the current peak
      //bytePos+=2;
      //peaksBytePos+=2;

      *(uint16_t*)(&compressedBuffer[bytePos])=pr->peaksSegs[pk]; //Panel for the current peak
      bytePos+=2;
      *(uint16_t*)(&compressedBuffer[bytePos])=pr->peaksRows[pk]; //Row for the current peak
      bytePos+=2;
      *(uint16_t*)(&compressedBuffer[bytePos])=pr->peaksCols[pk]; //Column for the current peak
      bytePos+=2;
    }
  }
  // cout<<"peaks"<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("peaks\n");
  //printf("bytePos=%d\n",bytePos);

  *(uint64_t*)(&compressedBuffer[bytePos])=roiSavedCount;
  bytePos+=8;
  // cout<<"roiSavedCount="<<roiSavedCount<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  // cout<<"roiData"<<endl;
  //printf("roiSavedCount=%d\n",roiSavedCount);
  //printf("bytePos=%d\n",bytePos);
  //printf("roiData\n");
  for(roii=0;roii<roiSavedCount;roii++){
    *(float*)(&compressedBuffer[bytePos])=roiData[roii];
    // cout<<roiData[roii]<<",";
    bytePos+=4;
  }
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("bytePos=%d\n",bytePos);
  *(uint64_t*)(&compressedBuffer[bytePos])=szCompressedSize;
  bytePos+=8;
  // cout<<"szCompressedSize="<<szCompressedSize<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("szCompressedSize=%d\n",szCompressedSize);
  //printf("bytePos=%d\n",bytePos);
  for(bi=0;bi<szCompressedSize;bi++){  //bi for "byte index"
    *(unsigned char*)(&compressedBuffer[bytePos])=szComp[bi];
    bytePos+=1;
  }
  // cout<<"szComp"<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("szComp\n");
  //printf("bytePos=%d\n",bytePos);
  
  if(bytePos!=(*compressedSize)){
    printf("ERROR: bytePos = %ld != %ld = compressedSize\n",(long)bytePos,(long)compressedSize);
    assert(0);
  }
  
  free(szComp);
  free(roiM);
  free(roiData);
  free(binnedData);
  // delete [] roiM;
  // delete [] roiData;
  // delete [] binnedData;
  
  return compressedBuffer;
}

void* exafelSZ_Decompress(void *_pr,
                         unsigned char*_compressedBuffer,
                         size_t r4, size_t r3, size_t r2, size_t r1,
                         size_t compressedSize)
{ 
  size_t nEvents,panels,rows,cols;
  if(r4==0)
    nEvents=1;
  else
    nEvents=r4;
  panels=r1;
  rows=r2;
  cols=r3;
  //printf("AMG : exafelSZ_Decompress : nEvents,panels,rows,cols = %d , %d , %d , %d\n",nEvents,panels,rows,cols);

  //printf("DECOMPRESS\n");return NULL;
  uint8_t *compressedBuffer=(uint8_t *)_compressedBuffer;
  exafelSZ_params *pr=(exafelSZ_params *)_pr;
  exafelSZ_params_process(pr, panels, rows, cols); 
  exafelSZ_params_checkDecomp(pr, panels, rows, cols); 
  
  float *decompressedBuffer=(float*)malloc(nEvents*panels*rows*cols*sizeof(float));
  
  uint8_t *roiM=(uint8_t*)malloc(nEvents*panels*rows*cols);
  size_t e,p,r,c,pk,ri,ci,br,bc;
  
  /*
  Compressed Data Layout:
  nPeaksTotal : 8 bytes : (1 x uint64_t)
  peaks : (8 x nEvents + nPeaksTotal x 3 x 2) bytes : (nEvents x (nPeaks + nPeaks x 3 x uint16_t))
  roiSavedCount : 8 Bytes : (1 x uint64_t)
  ROI_data : roiSavedCount x 4 : roiSavedCount x float 
  szCompressedSize : 8 : uint64_t
  szComp : szComp x 1 : szComp x (unsigned char)
  */
  uint64_t bytePos=0;
  uint64_t nPeaksTotal=*(uint64_t*)(&compressedBuffer[bytePos]);
  bytePos += 8; 
  // cout<<endl;
  // cout<<"DECOMPRESS:"<<endl;
  // cout<<"nPeaksTotal="<<nPeaksTotal<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("\nDECOMPRESS:\n");
  //printf("nPeaksTotal=%d\n",nPeaksTotal);
  //printf("bytePos=%d\n",bytePos);
  
  uint8_t *peaks=(uint8_t*)(&compressedBuffer[bytePos]);
  bytePos += (8 * nEvents + nPeaksTotal * 3 * 2);
  // cout<<"peaks"<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("peaks\n");
  //printf("bytePos=%d\n",bytePos);
  
  uint64_t roiSavedCount=*(uint64_t*)(&compressedBuffer[bytePos]);
  bytePos+=8;
  // cout<<"roiSavedCount="<<roiSavedCount<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("roiSavedCount=%d\n",roiSavedCount);
  //printf("bytePos=%d\n",bytePos);
  
  // cout<<"roiData"<<endl;
  float *roiData=(float*)(&compressedBuffer[bytePos]);
  bytePos+=(roiSavedCount*4);
  // for(uint64_t roii=0;roii<roiSavedCount;roii++){
    // cout<<roiData[roii]<<",";
  // }
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("bytePos=%d\n",bytePos);
  
  uint64_t szCompressedSize=*(uint64_t*)(&compressedBuffer[bytePos]);
  bytePos+=8;
  // cout<<"szCompressedSize="<<szCompressedSize<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  //printf("szCompressedSize=%d\n",szCompressedSize);
  //printf("bytePos=%d\n",bytePos);
  
  unsigned char *szComp=(unsigned char*)(&compressedBuffer[bytePos]);
  bytePos+=szCompressedSize;
  // cout<<"szComp"<<endl;
  // cout<<"bytePos="<<bytePos<<endl;
  // cout<<endl;
  //printf("szComp\n");
  //printf("bytePos=%d\n\n",bytePos);
  
  //We should have inputs ready by now. Now process them:
  
  //Generate the ROI mask: NOTE: 0 means affirmative in ROI mask! This comes from the python scripts!
  //First, initialize with calibration panel:
  for(e=0;e<nEvents;e++){ //Event
    for(p=0;p<panels;p++){ //Panel
      for(r=0;r<rows;r++){ //Row
        for(c=0;c<cols;c++){ //Column
          if(calcIdx_2D(r,c,cols)<0 ||calcIdx_2D(r,c,cols)>=rows*cols){
            printf("ERROR: calcIdx_2D(r,c,cols) = calcIdx_2D(%d,%d,%d) = %d",(int)r,(int)c,(int)cols,(int)calcIdx_2D(r,c,cols));
            printf("       is NOT in the correct range: [0,%ld]",(int)rows*cols-1);
            assert(0);
          }
          if(calcIdx_4D(e,p,r,c,panels,rows,cols)<0 ||calcIdx_4D(e,p,r,c,panels,rows,cols)>=nEvents*panels*rows*cols){
            printf("ERROR: calcIdx_4D(e,p,r,c,panels,rows,cols) = calcIdx_4D(%d,%d,%d,%d,%d,%d,%d) = %d",(int)e,(int)p,(int)r,(int)c,(int)panels,(int)rows,(int)cols,(int)calcIdx_4D(e,p,r,c,panels,rows,cols));
            assert(0);
          }
          //roiM[calcIdx_4D(e,p,r,c,panels,rows,cols)]=pr->calibPanel[calcIdx_2D(r,c,cols)]; //calibPanel is a single segment copied over all the event(image)
          roiM[calcIdx_4D(e,p,r,c,panels,rows,cols)]=pr->calibPanel[calcIdx_3D(p,r,c,rows,cols)];  //calibPanel is as big as the event(image) itself
        }
      }
    }
  }
  uint64_t peaksBytePos=0; //Position in the peaks buffer
  //Now process the peaks and generate the mask:
  for(e=0;e<nEvents;e++){ //Event
    uint64_t nPeaks=*(uint64_t*)(&peaks[peaksBytePos]);
    peaksBytePos+=8;
    
    for(pk=0;pk<nPeaks;pk++){
      uint16_t p_=*(uint16_t*)(&peaks[peaksBytePos]); //Panel for the current peak
      peaksBytePos+=2;
      uint16_t r_=*(uint16_t*)(&peaks[peaksBytePos]); //Row for the current peak
      peaksBytePos+=2;
      uint16_t c_=*(uint16_t*)(&peaks[peaksBytePos]); //Col for the current peak
      peaksBytePos+=2;
      
      if(p_>=panels){
        printf("ERROR: Peak coordinate out of bounds: Panel=%d, Valid range: 0,%d\n",(int)p_,(int)panels-1);
        assert(0);
        printf("Skipping this peak...\n");
        continue;
      }
      if(r_>=rows){
        printf("ERROR: Peak coordinate out of bounds: Row=%d, Valid range: 0,%d\n",(int)r_,(int)rows-1);
        assert(0);
        printf("Skipping this peak...\n");
        continue;
      }
      if(c_>=cols){
        printf("ERROR: Peak coordinate out of bounds: Col=%d, Valid range: 0,%d\n",(int)c_,(int)cols-1);
        assert(0);
        printf("Skipping this peak...\n");
        continue;
      }
      
      for(ri=r_-pr->peakRadius;ri<=r_+pr->peakRadius;ri++){  //ri: row index. Just a temporary variable.
        for(ci=c_-pr->peakRadius;ci<=c_+pr->peakRadius;ci++){  //ci: column index. Just a temporary variable.
          if(ri>=0 && ri<rows && ci>=0 && ci<cols){  //Check whether inside bounds or not
            roiM[calcIdx_4D(e,p_,ri,ci,panels,rows,cols)]=0;
          }
        }
      }
    }
  }
  
  //De-compress using SZ:
  float* szDecomp;
  size_t _szCompressedSize=szCompressedSize;
  switch(pr->szDim){
    case 1:
      szDecomp=SZ_decompress(SZ_FLOAT,szComp,_szCompressedSize,0,0,0,0, nEvents * panels * pr->binnedRows * pr->binnedCols);
      break;
    case 2:
      szDecomp=SZ_decompress(SZ_FLOAT,szComp,_szCompressedSize,0,0,0, nEvents * panels * pr->binnedRows, pr->binnedCols);
      break;
    case 3:
      szDecomp=SZ_decompress(SZ_FLOAT,szComp,_szCompressedSize,0,0,nEvents * panels, pr->binnedRows, pr->binnedCols);
      break;
    default:
      printf("ERROR: Wrong szDim : %d It must be 1,2 or 3.\n",(int)pr->szDim);
      assert(0);
  }
  //szDecomp=(void*)malloc(nEvents*panels*rows*cols*sizeof(float));
  
  // double max_err = 0;
  // for(int i=0; i<nEvents * panels * pr->binnedRows * pr->binnedCols; i++){
    // double err = fabs(szDecomp[i]-binnedData[i]);
    // if(err > max_err) max_err = err;
  // }
  // cout << "Max err = \t\t\t" << max_err << endl;
  

  //De-binning:
  for(e=0;e<nEvents;e++)//Event
    for(p=0;p<panels;p++)  //Panel
      for(r=0;r<pr->binnedRows;r++) //Row of the binnedData
        for(c=0;c<pr->binnedCols;c++) //Column of the binnedData
            for(br=0;br<pr->binSize;br++) //Bin Row (from origData)
              for(bc=0;bc<pr->binSize;bc++) //Bin Column (from origData)
                if(r*pr->binSize+br<rows && c*pr->binSize+bc<cols){
                  decompressedBuffer[calcIdx_4D(e,p,r*pr->binSize+br,c*pr->binSize+bc,panels,rows,cols)] = szDecomp[calcIdx_4D(e,p,r,c,panels,pr->binnedRows,pr->binnedCols)];
                }
  //Restore ROI:
  uint64_t current=0;
  for(e=0;e<nEvents;e++)//Event
    for(p=0;p<panels;p++)  //Panel
      for(r=0;r<rows;r++) //Row of the binnedData
        for(c=0;c<cols;c++) //Column of the binnedData
          if(!roiM[calcIdx_4D(e,p,r,c,panels,rows,cols)]){
            decompressedBuffer[calcIdx_4D(e,p,r,c,panels,rows,cols)]=roiData[current];
            current++;
          }
  // delete [] roiM;
  free(roiM);
  free(szDecomp);
  
  return ((void*)decompressedBuffer);
}

#ifdef __cplusplus
}
#endif
