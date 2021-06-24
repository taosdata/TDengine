#include "pastri.h"
#include "pastriD.h"
#include "pastriF.h"

void SZ_pastriReadParameters(char paramsFilename[512],pastri_params *paramsPtr){
  FILE *paramsF;
  paramsF=fopen(paramsFilename,"r");
  
  if(paramsF==NULL){
    printf("ERROR: Parameters file cannot be opened.\n");
    printf("Filename: %s\n",paramsFilename);
    assert(0);
  }
  
  fscanf(paramsF,"%d %d %d %d %lf %d %d",&paramsPtr->bf[0],&paramsPtr->bf[1],&paramsPtr->bf[2],&paramsPtr->bf[3],&paramsPtr->originalEb,&paramsPtr->dataSize,&paramsPtr->numBlocks);
  //printf("Params: %d %d %d %d %.3e %d\n",paramsPtr->bf[0],paramsPtr->bf[1],paramsPtr->bf[2],paramsPtr->bf[3],paramsPtr->originalEb,paramsPtr->numBlocks);
  fclose(paramsF);
}

void SZ_pastriPreprocessParameters(pastri_params *p){
  //Preprocess by calculating some pastri_params:
  //Calculate sbSize, sbNum, etc.:
  p->idxRange[0]=(p->bf[0]+1)*(p->bf[0]+2)/2;
  p->idxRange[1]=(p->bf[1]+1)*(p->bf[1]+2)/2;
  p->idxRange[2]=(p->bf[2]+1)*(p->bf[2]+2)/2;
  p->idxRange[3]=(p->bf[3]+1)*(p->bf[3]+2)/2;
  p->sbSize=p->idxRange[2]*p->idxRange[3];
  p->sbNum=p->idxRange[0]*p->idxRange[1];
  p->bSize=p->sbSize*p->sbNum;
  p->usedEb=p->originalEb*0.999;  //This is needed just to eliminate some rounding errors. It has almost no effect on compression rate/ratios.
}

void SZ_pastriCompressBatch(pastri_params *p,unsigned char *originalBuf, unsigned char** compressedBufP,size_t *compressedBytes){
  (*compressedBufP) = (unsigned char*)calloc(p->numBlocks*p->bSize*p->dataSize,sizeof(char));
  int bytes; //bytes for this block
  int i;
  size_t bytePos=0; //Current byte pos in the outBuf
  
  memcpy(*compressedBufP, p, sizeof(pastri_params));
  bytePos+=sizeof(pastri_params);
  
  for(i=0;i<p->numBlocks;i++){
    if(p->dataSize==8){
      pastri_double_Compress(originalBuf + (i*p->bSize*p->dataSize),p,(*compressedBufP) + bytePos,&bytes);
    }else if(p->dataSize==4){
      pastri_float_Compress(originalBuf + (i*p->bSize*p->dataSize),p,(*compressedBufP) + bytePos,&bytes);
    }
    bytePos+=bytes;
    //printf("bytes:%d\n",bytes);
  }
  *compressedBytes=bytePos;
  //printf("totalBytesWritten:%d\n",*compressedBytes);
}

void SZ_pastriDecompressBatch(unsigned char*compressedBuf, pastri_params *p, unsigned char** decompressedBufP ,size_t *decompressedBytes){
  int bytePos=0; //Current byte pos in the outBuf 
  memcpy(p, compressedBuf, sizeof(pastri_params));
  bytePos+=sizeof(pastri_params);	
	
  (*decompressedBufP) = (unsigned char*)malloc(p->numBlocks*p->bSize*p->dataSize*sizeof(char)); 
  int bytes; //bytes for this block
  int i;
  
  for(i=0;i<p->numBlocks;i++){
    if(p->dataSize==8){
      pastri_double_Decompress(compressedBuf + bytePos,p->dataSize,p,(*decompressedBufP) + (i*p->bSize*p->dataSize),&bytes);
    }else if(p->dataSize==4){
      pastri_float_Decompress(compressedBuf + bytePos,p->dataSize,p,(*decompressedBufP) + (i*p->bSize*p->dataSize),&bytes);
    }
          
    bytePos += bytes;
    //printf("bytes:%d\n",bytes);
  }
  //printf("totalBytesRead:%d\n",bytePos);
  *decompressedBytes=p->numBlocks*p->bSize*p->dataSize;
}

void SZ_pastriCheckBatch(pastri_params *p,unsigned char*originalBuf,unsigned char*decompressedBuf){        
  int i;
  for(i=0;i<p->numBlocks;i++){
    if(p->dataSize==8){
      pastri_double_Check(originalBuf+(i*p->bSize*p->dataSize),p->dataSize,decompressedBuf+(i*p->bSize*p->dataSize),p);
    }else if(p->dataSize==4){
      pastri_float_Check(originalBuf+(i*p->bSize*p->dataSize),p->dataSize,decompressedBuf+(i*p->bSize*p->dataSize),p);
    }
  }
}
