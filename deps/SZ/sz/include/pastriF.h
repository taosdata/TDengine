#ifndef PASTRIF_H
#define PASTRIF_H

static inline int64_t pastri_float_quantize(float x, float binSize){
  //Add or sub 0.5, depending on the sign:
  x=x/binSize;
  
  u_UI64I64D u1,half;
  u1.d=x;
  
  half.d=0.5;
  
  ////printf("pastri_float_quantize:\nx=%lf  x=0x%lx\n",x,(*((uint64_t *)(&x))));
  ////printf("sign(x):0x%lx\n", x);
  ////printf("0.5:0x%lx\n", (*((uint64_t *)(&half))));
  half.ui64 |= (u1.ui64 & (uint64_t)0x8000000000000000);
  ////printf("sign(x)*0.5:0x%lx\n", (*((uint64_t *)(&half))));
  return (int64_t)(x + half.d);
}

static inline void pastri_float_PatternMatch(float*data,pastri_params* p,pastri_blockParams* bp,int64_t* patternQ,int64_t *scalesQ, int64_t* ECQ){
  //Find the pattern.
  //First, find the extremum point:
  float absExt=0; //Absolute value of Extremum
  int extIdx=-1; //Index of Extremum
  bp->nonZeros=0;
  int i,sb;
  for(i=0;i<p->bSize;i++){
    ////printf("data[%d] = %.16lf\n",i,data[i]);//DEBUG
    if(abs_FastD(data[i])>p->usedEb){
      bp->nonZeros++;
      ////if(DEBUG)printf("data[%d]:%.6e\n",i,data[i]); //DEBUG
    }
    if(abs_FastD(data[i])>absExt){
      absExt=abs_FastD(data[i]);
      extIdx=i;
    }
  }
  int patternIdx; //Starting Index of Pattern
  patternIdx=(extIdx/p->sbSize)*p->sbSize;
  
  float patternExt=data[extIdx];
  bp->binSize=2*p->usedEb;
  
  ////if(DEBUG){printf("Extremum  : data[%d] = %.6e\n",extIdx,patternExt);} //DEBUG
  ////if(DEBUG){printf("patternIdx: %d\n",patternIdx);} //DEBUG
  
  ////if(DEBUG){for(i=0;i<p->sbSize;i++){printf("pattern[%d]=data[%d]=%.6e Quantized:%d\n",i,patternIdx+i,data[patternIdx+i],pastri_float_quantize(data[patternIdx+i]/binSize)  );}   }//DEBUG
  
  //int64_t *patternQ=(int64_t*)(outBuf+15);  //Possible Improvement!

  
  for(i=0;i<p->sbSize;i++){
    patternQ[i]=pastri_float_quantize(data[patternIdx+i],bp->binSize);
    //if(D_W){printf("patternQ[%d]=%ld\n",i,patternQ[i]);}
  }
  
  bp->patternBits=bitsNeeded_float((abs_FastD(patternExt)/bp->binSize)+1)+1;
  bp->scaleBits=bp->patternBits;
  bp->scalesBinSize=1/(float)(((uint64_t)1<<(bp->scaleBits-1))-1);
  ////if(DEBUG){printf("(patternExt/binSize)+1: %.6e\n",(patternExt/binSize)+1);} //DEBUG
  ////if(DEBUG){printf("scaleBits=patternBits: %d\n",scaleBits);} //DEBUG
  //if(D_W){printf("scalesBinSize: %.6e\n",bp->scalesBinSize);} //DEBUG
  
  //Calculate Scales.
  //The index part of the input buffer will be reused to hold Scale, Pattern, etc. values.
  int localExtIdx=extIdx%p->sbSize; //Local extremum index. This is not the actual extremum of the current sb, but rather the index that correspond to the global (block) extremum.
  //int64_t *scalesQ=(int64_t*)(outBuf+15+p->sbSize*8);  //Possible Improvement!
  int patternExtZero=(patternExt==0);
  ////if(DEBUG){printf("patternExtZero: %d\n",patternExtZero);} //DEBUG
  for(sb=0;sb<p->sbNum;sb++){
    //scales[sb]=data[sb*p->sbSize+localExtIdx]/patternExt;
    //scales[sb]=patternExtZero ? 0 : data[sb*p->sbSize+localExtIdx]/patternExt;
    //assert(scales[sb]<=1);
    scalesQ[sb]=pastri_float_quantize((patternExtZero ? 0 : data[sb*p->sbSize+localExtIdx]/patternExt),bp->scalesBinSize);
    //if(D_W){printf("scalesQ[%d]=%ld\n",sb,scalesQ[sb]);}
  }
  ////if(DEBUG){for(i=0;i<p->sbSize;i++){printf("scalesQ[%d]=%ld \n",i,scalesQ[i]);}} //DEBUG

  //int64_t *ECQ=(int64_t*)(outBuf+p->bSize*8); //ECQ is written into outBuf, just be careful when handling it.

  //uint64_t wVal;
  bp->ECQExt=0;
  int _1DIdx;
  bp->ECQ1s=0;
  bp->ECQOthers=0;
  float PS_binSize=bp->scalesBinSize*bp->binSize;
  for(sb=0;sb<p->sbNum;sb++){
    for(i=0;i<p->sbSize;i++){
      _1DIdx=sb*p->sbSize+i;
      ECQ[_1DIdx]=pastri_float_quantize( (scalesQ[sb]*patternQ[i]*PS_binSize-data[_1DIdx]),bp->binSize );
      float absECQ=abs_FastD(ECQ[_1DIdx]);
      if(absECQ > bp->ECQExt)
        bp->ECQExt=absECQ;
      ////if(DEBUG){printf("EC[%d]: %.6e Quantized:%ld \n",_1DIdx,(scalesQ[sb]*patternQ[i]*scalesBinSize*binSize-data[_1DIdx]),ECQ[_1DIdx]);} //DEBUG
      switch (ECQ[_1DIdx]){
        case 0:
          //ECQ0s++; //Currently not needed
          break;
        case 1:
          bp->ECQ1s++;
          break;
        case -1:
          bp->ECQ1s++;
          break;
        default:
          bp->ECQOthers++;
          break;
      }
    }
  }
  
  /*
  //DEBUG: Self-check. Remove this later.
  for(sb=0;sb<p->sbNum;sb++){
    for(i=0;i<p->sbSize;i++){
      _1DIdx=sb*p->sbSize+i;
      float decompressed=scalesQ[sb]*patternQ[i]*scalesBinSize*binSize-ECQ[_1DIdx]*binSize;
      if(abs_FastD(decompressed-data[_1DIdx])>(p->usedEb)){
        //printf("p->usedEb=%.6e\n",p->usedEb);
        //printf("data[%d]=%.6e decompressed[%d]=%.6e diff=%.6e\n",_1DIdx,data[_1DIdx],_1DIdx,decompressed,abs_FastD(data[_1DIdx]-decompressed));
        assert(0);
      }
    }
  }
  */
}

static inline void pastri_float_Encode(float *data,int64_t* patternQ,int64_t* scalesQ,int64_t* ECQ,pastri_params *p,pastri_blockParams* bp,unsigned char* outBuf,int *numOutBytes){
  bp->ECQBits=bitsNeeded_UI64(bp->ECQExt)+1;
  bp->_1DIdxBits=bitsNeeded_UI64(p->bSize);
  //(*numOutBytes)=0;
  
  int i;
  
  //Encode: 3 options:
  //Compressed, Sparse ECQ
  //Compressed, Non-Sparse ECQ
  //Uncompressed, Sparse Data
  //Uncompressed, Non-spsarse Data
  
  unsigned int UCSparseBits;  //Uncompressed, Sparse bits. Just like the original GAMESS data. Includes: mode, nonZeros, {indexes, data}
  unsigned int UCNonSparseBits;  //Uncompressed, NonSparse bits. Includes: mode, data
  unsigned int CSparseBits;  //Includes: mode, compressedBytes, patternBits, ECQBits,numOutliers,P, S, {Indexes(Sparse), ECQ}
  unsigned int CNonSparseBits;  //Includes: mode, compressedBytes, patternBits, ECQBits,P, S, {ECQ}
  //int BOOKKEEPINGBITS=120; //Includes: mode, compressedBytes, patternBits, ECQBits (8+64+32+8+8) //Moved to much earlier!
    
  //Consider: ECQ0s, ECQ1s, ECQOthers. Number of following values in ECQ: {0}, {1,-1}, { val<=-2, val>=2}
  //ECQ0s is actually not needed, but others are needed.

  UCSparseBits = p->dataSize*(1 + 2 + bp->nonZeros*16);  //64 bits for 4 indexes, 64 bit for data.
  UCNonSparseBits = p->dataSize*(1 + p->bSize*8);
  bp->numOutliers=bp->ECQ1s+bp->ECQOthers;
  if(bp->ECQBits==2){
    CSparseBits = p->dataSize*(1+4+1+1+2) + bp->patternBits*p->sbSize + bp->scaleBits*p->sbNum + bp->ECQ1s*(1+bp->_1DIdxBits);
    CNonSparseBits = p->dataSize*(1+4+1+1) + bp->patternBits*p->sbSize + bp->scaleBits*p->sbNum + p->bSize + bp->ECQ1s ;  //Or: ECQ0s+ECQ1s*2;
  }else{ //ECQBits>2
    CSparseBits = p->dataSize*(1+4+1+1+2) + bp->patternBits*p->sbSize + bp->scaleBits*p->sbNum + bp->ECQ1s*(2+bp->_1DIdxBits) + bp->ECQOthers*(1+bp->_1DIdxBits+bp->ECQBits);
    //CNonSparseBits = 8+32+8+8+ patternBits*p->sbSize + scaleBits*p->sbNum + p->bSize + ECQ0s + ECQ1s*3 + ECQOthers*(2+ECQBits);
    CNonSparseBits = p->dataSize*(1+4+1+1)+ bp->patternBits*p->sbSize + bp->scaleBits*p->sbNum + p->bSize + bp->ECQ1s*2 + bp->ECQOthers*(1+bp->ECQBits);
  }
  
  int UCSparseBytes=(UCSparseBits+7)/8; 
  int UCNonSparseBytes=(UCNonSparseBits+7)/8; 
  int CSparseBytes=(CSparseBits+7)/8; 
  int CNonSparseBytes=(CNonSparseBits+7)/8; 
  uint64_t bitPos=0;
  uint64_t bytePos=0;
  int i0,i1,i2,i3;
  int _1DIdx;
  
  //*(uint16_t*)(&outBuf[1])=p->idxOffset[0];
  //*(uint16_t*)(&outBuf[3])=p->idxOffset[1];
  //*(uint16_t*)(&outBuf[5])=p->idxOffset[2];
  //*(uint16_t*)(&outBuf[7])=p->idxOffset[3];
    
  //if(D_W){printf("ECQ0s:%d ECQ1s:%d ECQOthers:%d Total:%d\n",p->bSize-bp->ECQ1s-bp->ECQOthers,bp->ECQ1s,bp->ECQOthers,p->bSize);} //DEBUG
  //if(D_W){printf("numOutliers:%d\n",bp->numOutliers);} //DEBUG
  
  //****************************************************************************************
  //if(0){ //DEBUG
  //W:UCSparse
  if((UCSparseBytes<UCNonSparseBytes) && (UCSparseBytes<CSparseBytes) && (UCSparseBytes<CNonSparseBytes) ){ 
    //Uncompressed, Sparse bits. Just like the original GAMESS data. Includes: mode, indexOffsets, nonZeros, indexes, data
    *numOutBytes=UCSparseBytes;
    //if(D_G){printf("UCSparse\n");} //DEBUG
    //if(D_G)printf("ECQBits:%d\n",bp->ECQBits); //DEBUG
    outBuf[0]=0; //mode
    
    //*(uint16_t*)(&outBuf[9])=nonZeros;
    //bytePos=11;//0:mode, 1-8:indexOffsets 9-10:NonZeros. So start from 11.
    *(uint16_t*)(&outBuf[1])=bp->nonZeros;
    bytePos=3;//0:mode, 2-3:NonZeros. So start from 3.
    
    for(i0=0;i0<p->idxRange[0];i0++)
      for(i1=0;i1<p->idxRange[1];i1++)
        for(i2=0;i2<p->idxRange[2];i2++)
          for(i3=0;i3<p->idxRange[3];i3++){
            _1DIdx=p->idxRange[3]*(i2+p->idxRange[2]*(i1+i0*p->idxRange[1]))+i3;
            if(abs_FastD(data[_1DIdx])>p->usedEb){
              //*(uint16_t*)(&outBuf[bytePos])=i0+1+p->idxOffset[0];
              *(uint16_t*)(&outBuf[bytePos])=i0;
              bytePos+=2;
              //*(uint16_t*)(&outBuf[bytePos])=i1+1+p->idxOffset[1];
              *(uint16_t*)(&outBuf[bytePos])=i1;
              bytePos+=2;
              //*(uint16_t*)(&outBuf[bytePos])=i2+1+p->idxOffset[2];
              *(uint16_t*)(&outBuf[bytePos])=i2;
              bytePos+=2;
              //*(uint16_t*)(&outBuf[bytePos])=i3+1+p->idxOffset[3];
              *(uint16_t*)(&outBuf[bytePos])=i3;
              bytePos+=2;
              
              *(float*)(&outBuf[bytePos])=data[_1DIdx];
              bytePos+=p->dataSize;
            }
          }
    
    //if(D_G)printf("UCSparseBytes:%d \n",UCSparseBytes); //DEBUG
    
  //****************************************************************************************
  //}else if(0){ //DEBUG
  //W:UCNonSparse
  }else if((UCNonSparseBytes<UCSparseBytes) && (UCNonSparseBytes<CSparseBytes) && (UCNonSparseBytes<CNonSparseBytes) ){ 
    //Uncompressed, NonSparse bits. Includes: mode, indexOffsets, data
    *numOutBytes=UCNonSparseBytes;
    //if(D_G){printf("UCNonSparse\n");} //DEBUG
    //if(D_G)printf("ECQBits:%d\n",bp->ECQBits); //DEBUG
    outBuf[0]=1; //mode
    
    //memcpy(&outBuf[9], &inBuf[p->bSize*8], UCNonSparseBytes-9);
    memcpy(&outBuf[1], data, p->bSize*p->dataSize);
    
    //if(D_G)printf("UCNonSparseBytes:%d \n",UCNonSparseBytes); //DEBUG
    /*
    for(i=0;i<UCNonSparseBytes-17;i++){
      //printf("%d ",inBuf[p->bSize*8+i]);
    }
    //printf("\n");
    for(i=0;i<UCNonSparseBytes-17;i++){
      //printf("%d ",outBuf[17+i]);
    }
    //printf("\n");
    */
  //****************************************************************************************
  //}else if(1){ //DEBUG
  //W:CSparse
  }else if((CSparseBytes<UCNonSparseBytes) && (CSparseBytes<UCSparseBytes) && (CSparseBytes<CNonSparseBytes) ){ 
    //Includes: mode, indexOffsets, compressedBytes, patternBits, ECQBits,numOutliers,P, S, {Indexes(Sparse), ECQ}
    *numOutBytes=CSparseBytes;
    //if(D_G){printf("CSparse\n");} //DEBUG
    //if(D_G)printf("ECQBits:%d\n",bp->ECQBits); //DEBUG
    ////if(DEBUG){printf("patternBits:%d _1DIdxBits:%d\n",patternBits,_1DIdxBits);} //DEBUG
    outBuf[0]=2; //mode
    
    ////outBuf bytes [1:8] are indexOffsets, which are already written. outBuf bytes [9:12] are reserved for compressedBytes.
    //outBuf[13]=patternBits;
    //outBuf[14]=ECQBits;
    ////Currently, we are at the end of 15th byte.
    //*(uint16_t*)(&outBuf[15])=numOutliers;
    //bitPos=17*8; //Currently, we are at the end of 17th byte.
    
    //outBuf bytes [1:4] are reserved for compressedBytes.
    outBuf[5]=bp->patternBits;
    outBuf[6]=bp->ECQBits;
    //Currently, we are at the end of 7th byte.
    
    *(uint16_t*)(&outBuf[7])=bp->numOutliers; 
    //Now, we are at the end of 9th byte.
    bitPos=9*8; 
    
    ////if(DEBUG){printf("bitPos_B:%ld\n",bitPos);} //DEBUG

    for(i=0;i<p->sbSize;i++){
      writeBits_Fast(outBuf,&bitPos,bp->patternBits,patternQ[i]);//Pattern point
    }
    ////if(DEBUG){printf("bitPos_P:%ld\n",bitPos);} //DEBUG
    for(i=0;i<p->sbNum;i++){
      writeBits_Fast(outBuf,&bitPos,bp->scaleBits,scalesQ[i]);//Scale
    }
    ////if(DEBUG){printf("bitPos_S:%ld\n",bitPos);} //DEBUG
    ////if(DEBUG)printf("ECQBits:%d\n",ECQBits);
    switch(bp->ECQBits){
      case 2:
        for(i=0;i<p->bSize;i++){
          switch(ECQ[i]){
            case 0:
              break;
            case 1:
              ////if(DEBUG)printf("Index:%d ECQ:%ld Written:0x0\n",i,ECQ[i]); //DEBUG
              writeBits_Fast(outBuf,&bitPos,bp->_1DIdxBits,i);
              //writeBits_Fast(outBuf,&bitPos,2,0x10);
              //writeBits_Fast(outBuf,&bitPos,2,0);//0x00
              //writeBits_Fast(outBuf,&bitPos,2,0);//0x00
              writeBits_Fast(outBuf,&bitPos,1,0);//0x00
              break;
            case -1:
              ////if(DEBUG)printf("Index:%d ECQ:%ld Written:0x1\n",i,ECQ[i]); //DEBUG
              writeBits_Fast(outBuf,&bitPos,bp->_1DIdxBits,i);
              //writeBits_Fast(outBuf,&bitPos,2,0x11);
              //writeBits_Fast(outBuf,&bitPos,2,1);//0x01
              //writeBits_Fast(outBuf,&bitPos,1,0);
              writeBits_Fast(outBuf,&bitPos,1,1);
              break;
            default:
              assert(0);
              break;
          }
        }
        break;
      default: //ECQBits>2
      for(i=0;i<p->bSize;i++){
        switch(ECQ[i]){
          case 0:
            break;
          case 1:
            ////if(DEBUG)printf("Index:%d ECQ:%ld Written:0x00\n",i,ECQ[i]); //DEBUG
            writeBits_Fast(outBuf,&bitPos,bp->_1DIdxBits,i);
            //writeBits_Fast(outBuf,&bitPos,3,0);//0x000
            //writeBits_Fast(outBuf,&bitPos,1,0);
            writeBits_Fast(outBuf,&bitPos,1,0);
            writeBits_Fast(outBuf,&bitPos,1,0);
            break;
          case -1:
            ////if(DEBUG)printf("Index:%d ECQ:%ld Written:0x01\n",i,ECQ[i]); //DEBUG
            writeBits_Fast(outBuf,&bitPos,bp->_1DIdxBits,i);
            //writeBits_Fast(outBuf,&bitPos,3,1);//0x001
            //writeBits_Fast(outBuf,&bitPos,1,0);
            writeBits_Fast(outBuf,&bitPos,1,0);
            writeBits_Fast(outBuf,&bitPos,1,1);
            break;
          default:
            ////if(DEBUG)printf("Index:%d ECQ:%ld Written:0x1 0x%lx\n",i,ECQ[i],ECQ[i]); //DEBUG
            writeBits_Fast(outBuf,&bitPos,bp->_1DIdxBits,i);
            //writeBits_Fast(outBuf,&bitPos,2+ECQBits,((uint64_t)0x11<<ECQBits)|ECQ[i]);
            //writeBits_Fast(outBuf,&bitPos,2+ECQBits,(ECQ[i]&((uint64_t)0x00<<ECQBits))|((uint64_t)0x01<<ECQBits));
            //writeBits_Fast(outBuf,&bitPos,1,0);
            writeBits_Fast(outBuf,&bitPos,1,1);
            writeBits_Fast(outBuf,&bitPos,bp->ECQBits,ECQ[i]);
            break;
        }
      }
      break;
    }
    
    ////if(DEBUG){printf("bitPos_E:%ld\n",bitPos);} //DEBUG
    //if(D_C){if(!((bp->ECQBits>=2)||((bp->ECQBits==1) && (bp->numOutliers==0)))){printf("ERROR: ECQBits:%d numOutliers:%d This should not have happened!\n",bp->ECQBits,bp->numOutliers);assert(0);}} //DEBUG
          

    uint32_t bytePos=(bitPos+7)/8;
    //*(uint32_t*)(&outBuf[9])=bytePos;
    *(uint32_t*)(&outBuf[1])=bytePos;
    
    //if(D_G)printf("bitPos:%ld CSparseBits:%d bytePos:%d CSparseBytes:%d\n",bitPos,CSparseBits,bytePos,CSparseBytes); //DEBUG
    if(D_G){assert(bitPos==CSparseBits);}
    
  //****************************************************************************************
  //W:CNonSparse
  }else { 
    //Includes: mode, indexOffsets, compressedBytes, patternBits, ECQBits,P, S, {ECQ}
    *numOutBytes=CNonSparseBytes;
    //if(D_G){printf("CNonSparse\n");} //DEBUG
    //if(D_G)printf("ECQBits:%d\n",bp->ECQBits); //DEBUG
    ////if(DEBUG){printf("patternBits:%d _1DIdxBits:%d\n",patternBits,_1DIdxBits);} //DEBUG
    outBuf[0]=3; //mode
    
    ////outBuf bytes [1:8] are indexOffsets, which are already written. outBuf bytes [9:12] are reserved for compressedBytes.
    //outBuf[13]=patternBits;
    //outBuf[14]=ECQBits;
    //bitPos=15*8; //Currently, we are at the end of 15th byte.
    
    //outBuf bytes [1:4] are reserved for compressedBytes.
    outBuf[5]=bp->patternBits;
    outBuf[6]=bp->ECQBits;
    bitPos=7*8; //Currently, we are at the end of 7th byte.
    
    ////if(DEBUG){printf("bitPos_B:%ld\n",bitPos);} //DEBUG

    for(i=0;i<p->sbSize;i++){
      writeBits_Fast(outBuf,&bitPos,bp->patternBits,patternQ[i]);//Pattern point
    }
    ////if(DEBUG){printf("bitPos_P:%ld\n",bitPos);} //DEBUG
    for(i=0;i<p->sbNum;i++){
      writeBits_Fast(outBuf,&bitPos,bp->scaleBits,scalesQ[i]);//Scale
    }
    ////if(DEBUG){printf("bitPos_S:%ld\n",bitPos);} //DEBUG
    ////if(DEBUG)printf("ECQBits:%d\n",ECQBits);
    switch(bp->ECQBits){
      case 2:
        for(i=0;i<p->bSize;i++){
          switch(ECQ[i]){
            case 0:
              ////if(DEBUG)printf("Index:%d ECQ:%d Written:0x1\n",i,ECQ[i]); //DEBUG
              writeBits_Fast(outBuf,&bitPos,1,1);//0x1
              break;
            case 1:
              ////if(DEBUG)printf("Index:%d ECQ:%d Written:0x00\n",i,ECQ[i]); //DEBUG
              //writeBits_Fast(outBuf,&bitPos,2,0);//0x00
              writeBits_Fast(outBuf,&bitPos,1,0);
              writeBits_Fast(outBuf,&bitPos,1,0);
              break;
            case -1:
              ////if(DEBUG)printf("Index:%d ECQ:%d Written:0x01\n",i,ECQ[i]); //DEBUG
              //writeBits_Fast(outBuf,&bitPos,2,2); //0x01
              writeBits_Fast(outBuf,&bitPos,1,0);
              writeBits_Fast(outBuf,&bitPos,1,1);
              break;
            default:
              assert(0);
              break;
          }
        }
        break;
      default: //ECQBits>2
        ////if(DEBUG) printf("AMG_W1:bitPos:%ld\n",bitPos); //DEBUG
        for(i=0;i<p->bSize;i++){
          ////if(DEBUG){printf("AMG_W3:bitPos:%ld buffer[%ld]=0x%lx\n",bitPos,bitPos/8,*(uint64_t*)(&outBuf[bitPos/8]));}; //DEBUG
          ////if(DEBUG) printf("AMG_W2:bitPos:%ld\n",bitPos); //DEBUG
          ////if(DEBUG) printf("ECQ[%d]:%ld\n",i,ECQ[i]); //DEBUG
          switch(ECQ[i]){
            case 0:
              ////if(DEBUG)printf("Index:%d ECQ:%ld Written:0x1\n",i,ECQ[i]); //DEBUG
              ////if(DEBUG){printf("AMG_WB3:bitPos:%ld buffer[%ld]=0x%lx\n",bitPos,bitPos/8,*(uint64_t*)(&outBuf[bitPos/8]));}; //DEBUG
              //temp1=bitPos;
              writeBits_Fast(outBuf,&bitPos,1,1);  //0x1
              //wVal=1; writeBits_Fast(outBuf,&bitPos,1,wVal); //0x1
              ////if(DEBUG){printf("AMG_WA3:bitPos:%ld buffer[%ld]=0x%lx\n",temp1,temp1/8,*(uint64_t*)(&outBuf[temp1/8]));}; //DEBUG
              break;
            case 1:
              ////if(DEBUG)printf("Index:%d ECQ:%ld Written:0x000\n",i,ECQ[i]); //DEBUG
              ////if(DEBUG){printf("AMG_WB3:bitPos:%ld buffer[%ld]=0x%lx\n",bitPos,bitPos/8,*(uint64_t*)(&outBuf[bitPos/8]));}; //DEBUG
              //temp1=bitPos;
              //writeBits_Fast(outBuf,&bitPos,3,0); //0x000
              writeBits_Fast(outBuf,&bitPos,1,0);
              writeBits_Fast(outBuf,&bitPos,1,0);
              writeBits_Fast(outBuf,&bitPos,1,0);
              //wVal=0; writeBits_Fast(outBuf,&bitPos,3,wVal); //0x000
              ////if(DEBUG){printf("AMG_WA3:bitPos:%ld buffer[%ld]=0x%lx\n",temp1,temp1/8,*(uint64_t*)(&outBuf[temp1/8]));}; //DEBUG
              break;
            case -1:
              ////if(DEBUG)printf("Index:%d ECQ:%ld Written:0x001\n",i,ECQ[i]); //DEBUG
              ////if(DEBUG){printf("AMG_WB3:bitPos:%ld buffer[%ld]=0x%lx\n",bitPos,bitPos/8,*(uint64_t*)(&outBuf[bitPos/8]));}; //DEBUG
              //temp1=bitPos;
              //writeBits_Fast(outBuf,&bitPos,3,8); //0x001
              writeBits_Fast(outBuf,&bitPos,1,0); 
              writeBits_Fast(outBuf,&bitPos,1,0); 
              writeBits_Fast(outBuf,&bitPos,1,1); 
              //wVal=8; writeBits_Fast(outBuf,&bitPos,3,wVal); //0x001
              ////if(DEBUG){printf("AMG_WA3:bitPos:%ld buffer[%ld]=0x%lx\n",temp1,temp1/8,*(uint64_t*)(&outBuf[temp1/8]));}; //DEBUG
              break;
            default:
              ////if(DEBUG)printf("Index:%d ECQ:%ld Written:0x01 0x%lx\n",i,ECQ[i]); //DEBUG
              ////if(DEBUG){printf("AMG_WB3:bitPos:%ld buffer[%ld]=0x%lx\n",bitPos,bitPos/8,*(uint64_t*)(&outBuf[bitPos/8]));}; //DEBUG
              //temp1=bitPos;
              //writeBits_Fast(outBuf,&bitPos,2,2); //0x01
              writeBits_Fast(outBuf,&bitPos,1,0); 
              writeBits_Fast(outBuf,&bitPos,1,1); 
              //wVal=2; writeBits_Fast(outBuf,&bitPos,2,wVal); //0x01
              writeBits_Fast(outBuf,&bitPos,bp->ECQBits,ECQ[i]);
              ////if(DEBUG){printf("AMG_WA3:bitPos:%ld buffer[%ld]=0x%lx\n",temp1,temp1/8,*(uint64_t*)(&outBuf[temp1/8]));}; //DEBUG
              break;
          }
        }
        break;
    }
    
    ////if(DEBUG){printf("bitPos_E:%ld\n",bitPos);} //DEBUG
    //if(D_C){if(!((bp->ECQBits>=2)||((bp->ECQBits==1) && (bp->numOutliers==0)))){printf("ERROR: ECQBits:%d numOutliers:%d This should not have happened!\n",bp->ECQBits,bp->numOutliers);assert(0);}} //DEBUG
    
          

    uint32_t bytePos=(bitPos+7)/8;
    //*(uint32_t*)(&outBuf[9])=bytePos;
    *(uint32_t*)(&outBuf[1])=bytePos;
    
    //if(D_G)printf("bitPos:%ld CNonSparseBits:%d bytePos:%d CNonSparseBytes:%d\n",bitPos,CNonSparseBits,bytePos,CNonSparseBytes); //DEBUG
    if(D_G){assert(bitPos==CNonSparseBits);}
    
  }
  ////for(i=213;i<233;i++)if(DEBUG)printf("AMG_WE:bitPos:%d buffer[%d]=0x%lx\n",i*8,i,*(uint64_t*)(&outBuf[i])); //DEBUG
  
}
static inline int pastri_float_Compress(unsigned char*inBuf,pastri_params *p,unsigned char*outBuf,int *numOutBytes){
  pastri_blockParams bp;

  //if(D_G2){printf("Parameters: dataSize:%d\n",p->dataSize);}  //DEBUG
  //if(D_G2){printf("Parameters: bfs:%d %d %d %d originalEb:%.3e\n",p->bf[0],p->bf[1],p->bf[2],p->bf[3],p->usedEb);}  //DEBUG
  //if(D_G2){printf("Parameters: idxRanges:%d %d %d %d\n",p->idxRange[0],p->idxRange[1],p->idxRange[2],p->idxRange[3]);} //DEBUG
  //if(D_G2){printf("Parameters: sbSize:%d sbNum:%d bSize:%d\n",p->sbSize,p->sbNum,p->bSize); }//DEBUG
  
  int64_t patternQ[MAX_PS_SIZE];
  int64_t scalesQ[MAX_PS_SIZE];
  int64_t ECQ[MAX_BLOCK_SIZE];

  float *data;
  data=(float*)inBuf;
  
  //STEP 0: PREPROCESSING:
  //This step can include flattening the block, determining the period, etc.
  //Currently not needed.
  
  //STEP 1: PATTERN MATCH
  pastri_float_PatternMatch(data,p,&bp,patternQ,scalesQ,ECQ);
  
  //STEP 2: ENCODING(Include QUANTIZE)
  pastri_float_Encode(data,patternQ,scalesQ,ECQ,p,&bp,outBuf,numOutBytes);
  

  return 0;
}

static inline float pastri_float_InverseQuantization(int64_t q, float binSize){
  return q*binSize;
}

static inline void pastri_float_PredictData(pastri_params *p,pastri_blockParams *bp,float *data,int64_t* patternQ,int64_t* scalesQ,int64_t* ECQ){
  int j;
  float PS_binSize=bp->scalesBinSize*bp->binSize;
  for(j=0;j<p->bSize;j++){
    //data[j]=scalesQ[j/p->sbSize]*patternQ[j%p->sbSize]*PS_binSize - ECQ[j]*bp->binSize;
    data[j]=pastri_float_InverseQuantization(scalesQ[j/p->sbSize]*patternQ[j%p->sbSize],PS_binSize) - pastri_float_InverseQuantization(ECQ[j],bp->binSize);
  }
}

static inline void pastri_float_Decode(unsigned char*inBuf,pastri_params *p,pastri_blockParams *bp,unsigned char*outBuf,int *numReadBytes,int64_t* patternQ,int64_t* scalesQ,int64_t* ECQ){
  int j;
  bp->_1DIdxBits=bitsNeeded_UI64(p->bSize);
  //float *data=(float*)(outBuf+p->bSize*8);
  float *data=(float*)(outBuf);
  int i0,i1,i2,i3;
  //uint16_t *idx0,*idx1,*idx2,*idx3;
  int _1DIdx;

  int64_t ECQTemp;
  uint64_t bytePos=0;
  uint64_t bitPos=0;
  uint64_t temp,temp2;
  //int sb,localIdx;

  
  //idx0=(uint16_t*)(outBuf           );
  //idx1=(uint16_t*)(outBuf+p->bSize*2);
  //idx2=(uint16_t*)(outBuf+p->bSize*4);
  //idx3=(uint16_t*)(outBuf+p->bSize*6);
  //p->idxOffset[0]=*(uint32_t*)(&inBuf[1]);
  //p->idxOffset[1]=*(uint32_t*)(&inBuf[3]);
  //p->idxOffset[2]=*(uint32_t*)(&inBuf[5]);
  //p->idxOffset[3]=*(uint32_t*)(&inBuf[7]);
  /*
  for(i0=0;i0<p->idxRange[0];i0++)
    for(i1=0;i1<p->idxRange[1];i1++)
      for(i2=0;i2<p->idxRange[2];i2++)
        for(i3=0;i3<p->idxRange[3];i3++){
            //_1DIdx=i0*p->idxRange[1]*p->idxRange[2]*p->idxRange[3]+i1*p->idxRange[2]*p->idxRange[3]+i2*p->idxRange[3]+i3;
            _1DIdx=p->idxRange[3]*(i2+p->idxRange[2]*(i1+i0*p->idxRange[1]))+i3;
            idx0[_1DIdx]=i0+1+p->idxOffset[0];
            idx1[_1DIdx]=i1+1+p->idxOffset[1];
            idx2[_1DIdx]=i2+1+p->idxOffset[2];
            idx3[_1DIdx]=i3+1+p->idxOffset[3];
        }
  */
  
  //*numOutBytes=p->bSize*16;  
  
  //inBuf[0] is "mode"
  switch(inBuf[0]){
    //R:UCSparse
    case 0:
      //if(D_G){printf("\nDC:UCSparse\n");} //DEBUG
      //bp->nonZeros=*(uint16_t*)(&inBuf[9]);
      //bytePos=11;
      bp->nonZeros=*(uint16_t*)(&inBuf[1]);
      bytePos=3;
      for(j=0;j<p->bSize;j++){
          data[j]=0;
      }
      for(j=0;j<bp->nonZeros;j++){
        //i0=*(uint16_t*)(&inBuf[bytePos])-1-p->idxOffset[0]; //i0
        i0=*(uint16_t*)(&inBuf[bytePos]); //i0
        bytePos+=2;
        //i1=*(uint16_t*)(&inBuf[bytePos])-1-p->idxOffset[1]; //i1
        i1=*(uint16_t*)(&inBuf[bytePos]); //i1
        bytePos+=2;
        //i2=*(uint16_t*)(&inBuf[bytePos])-1-p->idxOffset[2]; //i2
        i2=*(uint16_t*)(&inBuf[bytePos]); //i2
        bytePos+=2;
        //i3=*(uint16_t*)(&inBuf[bytePos])-1-p->idxOffset[3]; //i3
        i3=*(uint16_t*)(&inBuf[bytePos]); //i3
        bytePos+=2;
        _1DIdx=p->idxRange[3]*(i2+p->idxRange[2]*(i1+i0*p->idxRange[1]))+i3;
        data[_1DIdx]=*(float*)(&inBuf[bytePos]);
        bytePos+=8; 
      }
      //if(D_G){printf("\nDC:bytePos:%ld\n",bytePos);} //DEBUG
      break;
    //R:UCNonSparse
    case 1:
      //if(D_G){printf("\nDC:UCNonSparse\n");} //DEBUG
      //memcpy(&outBuf[p->bSize*8], &inBuf[9], p->bSize*8);
      memcpy(data, &inBuf[1], p->bSize*8);
      bytePos=p->bSize*8;
      //if(D_G){printf("\nDC:bytePos:%ld\n",bytePos);} //DEBUG
      break;
    //R:CSparse
    case 2:
      //if(D_G){printf("\nDC:CSparse\n");} //DEBUG
      //for(j=0;j<p->bSize;j++){
      //  data[j]=0;
      //}
      
      //bp->patternBits=inBuf[13];
      //bp->ECQBits=inBuf[14];      
      
      bp->patternBits=inBuf[5];
      bp->ECQBits=inBuf[6];
      
      //if(D_R){printf("bp->patternBits:%d bp->ECQBits:%d bp->_1DIdxBits:%d\n",bp->patternBits,bp->ECQBits,bp->_1DIdxBits);} //DEBUG
      
      //bp->numOutliers=*(uint16_t*)(&inBuf[15]);
      //bitPos=17*8;
      bp->numOutliers=*(uint16_t*)(&inBuf[7]);
      bitPos=9*8;
      //if(D_R){printf("bp->numOutliers:%d\n",bp->numOutliers);} //DEBUG

      bp->scalesBinSize=1/(float)(((uint64_t)1<<(bp->patternBits-1))-1);
  
      bp->binSize=p->usedEb*2;
      
      //if(D_R){printf("bp->scalesBinSize:%.6e bp->binSize:%.6e bp->scalesBinSize*bp->binSize:%.6e\n",bp->scalesBinSize,bp->binSize,bp->scalesBinSize*bp->binSize);} //DEBUG

      for(j=0;j<p->sbSize;j++){
        patternQ[j]=readBits_I64(inBuf,&bitPos,bp->patternBits);//Pattern point
        //if(D_R){printf("R:patternQ[%d]=%ld\n",j,patternQ[j]);}
      }
      for(j=0;j<p->sbNum;j++){
        scalesQ[j]=readBits_I64(inBuf,&bitPos,bp->patternBits);//Scale
        //if(D_R){printf("R:scalesQ[%d]=%ld\n",j,scalesQ[j]);}
      }
      
      /* //Splitting
      for(j=0;j<p->bSize;j++){
        data[j]=scalesQ[j/p->sbSize]*patternQ[j%p->sbSize]*bp->scalesBinSize*bp->binSize;
      }
      */
      for(j=0;j<p->bSize;j++){
        ECQ[j]=0;
      }
      switch(bp->ECQBits){
        case 2:
          for(j=0;j<bp->numOutliers;j++){
            ////if(DEBUG){printf("readBits_UI64:%ld\n",readBits_UI64(inBuf,&bitPos,bp->_1DIdxBits));} //DEBUG
            ////if(DEBUG){printf("readBits_UI64:%ld\n",readBits_I64(inBuf,&bitPos,2));} //DEBUG
            
            _1DIdx=readBits_UI64(inBuf,&bitPos,bp->_1DIdxBits);
            ECQTemp=readBits_I64(inBuf,&bitPos,1);
            ECQTemp= ((ECQTemp<<63)>>63)|(uint64_t)0x1;
            ////if(D_R)printf("R:ECQ[%d]: %ld \n",_1DIdx,ECQTemp);
            //continue;
            //sb=_1DIdx/p->sbSize; 
            //localIdx=_1DIdx%p->sbSize;
            
            ////data[_1DIdx]-=ECQTemp*bp->binSize;//Splitting
            ECQ[_1DIdx]=ECQTemp;
            
            ////if(DEBUG){printf("decompressed[%d]:%.6e\n",_1DIdx,data[_1DIdx]);} //DEBUG
          }
          break;
        default: //bp->ECQBits>2
          //if(D_C){if(!((bp->ECQBits>=2)||((bp->ECQBits==1) && (bp->numOutliers==0)))){printf("ERROR: bp->ECQBits:%d bp->numOutliers:%d This should not have happened!\n",bp->ECQBits,bp->numOutliers);assert(0);}} //DEBUG
    
          for(j=0;j<bp->numOutliers;j++){
            _1DIdx=readBits_UI64(inBuf,&bitPos,bp->_1DIdxBits);
            //sb=_1DIdx/p->sbSize; 
            //localIdx=_1DIdx%p->sbSize;
            temp=readBits_UI64(inBuf,&bitPos,1);
            ////if(DEBUG){printf("temp:%ld\n",temp);} //DEBUG
            switch(temp){
              case 0:  //+-1
                ECQTemp=readBits_I64(inBuf,&bitPos,1);
                ECQTemp= ((ECQTemp<<63)>>63)|(uint64_t)0x1;
                ////if(DEBUG){printf("_1DIdx:%ld ECQTemp:0x%ld\n",_1DIdx,ECQTemp);} //DEBUG
                ////if(D_R)printf("R:ECQ[%d]: %ld \n",_1DIdx,ECQTemp);
                break;
              case 1: //Others
                ECQTemp=readBits_I64(inBuf,&bitPos,bp->ECQBits);
                ////if(DEBUG){printf("_1DIdx:%ld ECQTemp:0x%ld\n",_1DIdx,ECQTemp);} //DEBUG
                ////if(D_R)printf("R:ECQ[%d]: %ld \n",_1DIdx,ECQTemp);
                break;
              //default:
              ////  printf("ERROR: Bad 2-bit value: 0x%lx",temp);
              // assert(0); //AMG
              //  break;
            }
            
            //data[_1DIdx]-=ECQTemp*bp->binSize;//Splitting
            ECQ[_1DIdx]=ECQTemp;
            
            ////if(DEBUG){printf("decompressed[%d]:%.6e\n",_1DIdx,data[_1DIdx]);} //DEBUG
          }
          break;
      }
      //static inline uint64_t readBits_UI64(unsigned char* buffer,uint64_t *bitPosPtr,uint64_t numBits){ // numBits must be in range [0:56]
      //patternQ=(int64_t*)(inBuf+15); 
      //scalesQ=(int64_t*)(inBuf+15+p->sbSize*8);
      
      bytePos=(bitPos+7)/8;
      //if(D_G){printf("\nDC:bytePos:%ld\n",bytePos);} //DEBUG
      
      //STEP 2: PREDICT DATA(Includes INVERSE QUANTIZATION)
      pastri_float_PredictData(p,bp,data,patternQ,scalesQ,ECQ);

      break;
    //R:CNonSparse
    case 3:
      //if(D_G){printf("\nDC:CNonSparse\n");} //DEBUG
      
      //for(j=0;j<p->bSize;j++){
      //  data[j]=0;
      //}
      
      //bp->patternBits=inBuf[13];
      //bp->ECQBits=inBuf[14];
      
      bp->patternBits=inBuf[5];
      bp->ECQBits=inBuf[6];
      
      //if(D_R){printf("bp->patternBits:%d bp->ECQBits:%d bp->_1DIdxBits:%d\n",bp->patternBits,bp->ECQBits,bp->_1DIdxBits);} //DEBUG
      
      //bitPos=15*8;
      bitPos=7*8;

      bp->scalesBinSize=1/(float)(((uint64_t)1<<(bp->patternBits-1))-1);
      bp->binSize=p->usedEb*2;
      
      //if(D_R){printf("bp->scalesBinSize:%.6e bp->binSize:%.6e bp->scalesBinSize*bp->binSize:%.6e\n",bp->scalesBinSize,bp->binSize,bp->scalesBinSize*bp->binSize);} //DEBUG

      for(j=0;j<p->sbSize;j++){
        patternQ[j]=readBits_I64(inBuf,&bitPos,bp->patternBits);//Pattern point
        //if(D_R){printf("R:patternQ[%d]=%ld\n",j,patternQ[j]);}
      }
      for(j=0;j<p->sbNum;j++){
        scalesQ[j]=readBits_I64(inBuf,&bitPos,bp->patternBits);//Scale
        //if(D_R){printf("R:scalesQ[%d]=%ld\n",j,scalesQ[j]);}
      }
      /* //Splitting
      for(j=0;j<p->bSize;j++){
        data[j]=scalesQ[j/p->sbSize]*patternQ[j%p->sbSize]*bp->scalesBinSize*bp->binSize;
        ////if(DEBUG){printf("DC:PS[%d]=%.6e\n",j,data[j]);}
      }
      */
      switch(bp->ECQBits){
        case 2:
          for(j=0;j<p->bSize;j++){
            ////if(DEBUG){printf("readBits_UI64:%ld\n",readBits_UI64(inBuf,&bitPos,bp->_1DIdxBits));} //DEBUG
            ////if(DEBUG){printf("readBits_UI64:%ld\n",readBits_I64(inBuf,&bitPos,2));} //DEBUG
            //_1DIdx=readBits_UI64(inBuf,&bitPos,bp->_1DIdxBits);
            temp=readBits_UI64(inBuf,&bitPos,1);
            switch(temp){
              case 0:
                ECQTemp=readBits_I64(inBuf,&bitPos,1);
                ECQTemp= ((ECQTemp<<63)>>63)|(uint64_t)0x1;
                break;
              case 1:
                ECQTemp=0;
                break;
              default:
                assert(0);
                break;
            }
            
            ////if(DEBUG){printf("_1DIdx:%ld ECQTemp:0x%ld\n",_1DIdx,ECQTemp);} //DEBUG
            //continue;
            //sb=_1DIdx/p->sbSize; 
            //localIdx=_1DIdx%p->sbSize;
            
            //data[j]-=ECQTemp*bp->binSize; //Splitting
            ECQ[j]=ECQTemp;
            
            ////if(DEBUG){printf("decompressed[%d]:%.6e\n",_1DIdx,data[_1DIdx]);} //DEBUG
          }
          break;
        default: //bp->ECQBits>2
          ////if(DEBUG)printf("AMG_R1:bitPos: %ld\n",bitPos);
          
          for(j=0;j<p->bSize;j++){
            ////if(DEBUG){printf("AMG_R3:bitPos:%ld buffer[%ld]=0x%lx\n",bitPos,bitPos/8,*(uint64_t*)(&inBuf[bitPos/8]));}; //DEBUG
            ////if(DEBUG)printf("AMG_R2:bitPos: %ld\n",bitPos);

            ////if(DEBUG){printf("readBits_UI64:%ld\n",readBits_UI64(inBuf,&bitPos,bp->_1DIdxBits));} //DEBUG
            ////if(DEBUG){printf("readBits_UI64:%ld\n",readBits_I64(inBuf,&bitPos,2));} //DEBUG
            //_1DIdx=readBits_UI64(inBuf,&bitPos,bp->_1DIdxBits);
            temp=readBits_UI64(inBuf,&bitPos,1);
            ////if(DEBUG){printf("AMG_R3:bitPos:%ld buffer[%ld]=0x%lx\n",bitPos,bitPos/8,*(uint64_t*)(&inBuf[bitPos/8]));}; //DEBUG
            switch(temp){
              case 0:
                ////if(DEBUG)printf("Read:0");
                temp2=readBits_UI64(inBuf,&bitPos,1);
                switch(temp2){
                  case 0:
                    ////if(DEBUG)printf("0");
                    ECQTemp=readBits_I64(inBuf,&bitPos,1);
                    ////if(DEBUG){printf("AMG_R3:bitPos:%ld buffer[%ld]=0x%lx\n",bitPos,bitPos/8,*(uint64_t*)(&inBuf[bitPos/8]));}; //DEBUG
                    ////if(DEBUG)printf("R:ECQTemp:%ld\n",ECQTemp);
                    ECQTemp= ((ECQTemp<<63)>>63)|(uint64_t)0x1;
                    ////if(DEBUG)printf("R:ECQ[%d]: %ld\n",j,ECQTemp);
                    break;
                  case 1:
                    ////if(DEBUG)printf("1\n");
                    ECQTemp=readBits_I64(inBuf,&bitPos,bp->ECQBits);
                    ////if(DEBUG){printf("AMG_R3:bitPos:%ld buffer[%ld]=0x%lx\n",bitPos,bitPos/8,*(uint64_t*)(&inBuf[bitPos/8]));}; //DEBUG
                    ////if(DEBUG)printf("R:ECQ[%d]: %ld\n",j,ECQTemp);
                    break;
                  default:
                    assert(0);
                    break;
                }
                break;
              case 1:
                ////if(DEBUG)printf("Read:1\n");
                ECQTemp=0;
                ////if(DEBUG)printf("R:ECQ[%d]: %ld\n",j,ECQTemp);
                break;
              default:
                assert(0);
                break;
            }
            
            ////if(DEBUG){printf("_1DIdx:%ld ECQTemp:0x%ld\n",_1DIdx,ECQTemp);} //DEBUG
            //continue;
            //sb=_1DIdx/p->sbSize; 
            //localIdx=_1DIdx%p->sbSize;
            
            //data[j]-=ECQTemp*bp->binSize; //Splitting
            ECQ[j]=ECQTemp;
            
            ////if(DEBUG){printf("DC:data[%d]:%.6e\n",j,data[j]);} //DEBUG
          }
          break;
      }
      //static inline uint64_t readBits_UI64(unsigned char* buffer,uint64_t *bitPosPtr,uint64_t numBits){ // numBits must be in range [0:56]
      //patternQ=(int64_t*)(inBuf+15); 
      //scalesQ=(int64_t*)(inBuf+15+p->sbSize*8);
      bytePos=(bitPos+7)/8;
      //if(D_G){printf("\nDC:bytePos:%ld\n",bytePos);} //DEBUG
      
      //STEP 2: PREDICT DATA(Includes INVERSE QUANTIZATION)
      pastri_float_PredictData(p,bp,data,patternQ,scalesQ,ECQ);
      break;
      
    default:
      assert(0);
      break;
  } 
  (*numReadBytes)=bytePos;
}

static inline void pastri_float_Decompress(unsigned char*inBuf,int dataSize,pastri_params *p,unsigned char*outBuf,int *numReadBytes){
  int64_t patternQ[MAX_PS_SIZE]; 
  int64_t scalesQ[MAX_PS_SIZE];
  int64_t ECQ[MAX_BLOCK_SIZE];
  
  pastri_blockParams bp;
  
  //STEP 1: DECODE (Includes PREDICT DATA(Includes INVERSE QUANTIZATION))
  //(Further steps are called inside pastri_float_Decode function)
  pastri_float_Decode(inBuf,p,&bp,outBuf,numReadBytes,patternQ,scalesQ,ECQ);

  return;
}

//inBuf vs Decompressed
static inline int pastri_float_Check(unsigned char*inBuf,int dataSize,unsigned char*DC,pastri_params *p){
  int i;
  
  float *data=(float*)(inBuf);
  float *data_dc=(float*)(DC);
  
  //Comparing Indexes:
  /*
  for(i=0;i<p->bSize;i++){
    if(idx0[i]!=idx0_dc[i]){
      //printf("idx0[%d]=%d  !=  %d=idx0_dc[%d]",i,idx0[i],idx0_dc[i],i);
      assert(0);
    }
    if(idx1[i]!=idx1_dc[i]){
      //printf("idx1[%d]=%d  !=  %d=idx1_dc[%d]",i,idx1[i],idx1_dc[i],i);
      assert(0);
    }
    if(idx2[i]!=idx2_dc[i]){
      //printf("idx2[%d]=%d  !=  %d=idx2_dc[%d]",i,idx2[i],idx2_dc[i],i);
      assert(0);
    }
    if(idx3[i]!=idx3_dc[i]){
      //printf("idx3[%d]=%d  !=  %d=idx3_dc[%d]",i,idx3[i],idx3_dc[i],i);
      assert(0);
    }
  }
  */
  
  //Comparing Data:
  for(i=0;i<p->bSize;i++){
    if(abs_FastD(data[i]-data_dc[i])>p->usedEb){
      //printf("|data[%d]-data_dc[%d]|>originalEb : %.3e - %.3e = %.3e > %.3e\n",i,i,data[i],data_dc[i],abs_FastD(data[i]-data_dc[i]),p->usedEb);
      assert(0);
    }
  }
  return 0;
}


#endif
