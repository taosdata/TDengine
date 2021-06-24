#ifndef PASTRIGENERAL_H
#define PASTRIGENERAL_H


static inline double abs_FastD(double x){
  u_UI64I64D u1;
  u1.d=x;
  //(*((uint64_t *)(&x)))&=(int64_t)0x7FFFFFFFFFFFFFFF;
  u1.ui64&=(int64_t)0x7FFFFFFFFFFFFFFF;
  return u1.d;
}

static inline int64_t abs_FastI64(int64_t x){
  return (x^((x&(int64_t)0x8000000000000000)>>63))+((x&(int64_t)0x8000000000000000)!=0);
}
/*
int abs(int x) {
   int mask = (x >> (sizeof(int) * CHAR_BIT - 1));
   return (x + mask) ^ mask;
}
*/




//Returns the min. bits needed to represent x.
//Same as: ceil(log2(abs(x))) 
//Actually to be completely safe, it correspond to: ceil(log2(abs(i)+1))+0.1
//+0.1 was for fixing rounding errors
//REMEMBER: To represent the whole range [-x:x], the number of bits required is bitsNeeded(x)+1
static inline int bitsNeeded_double(double x){
  u_UI64I64D u1;
  u1.d=x;
  return (((u1.ui64<<1)>>53)-1022) & (((x!=0)<<31)>>31);
}

//Returns the min. bits needed to represent x.
//Same as: ceil(log2(abs(x))) 
//NEEDS OPTIMIZATION!
static inline int bitsNeeded_float(float x){
  u_UI64I64D u1;
  u1.d=x; //Casting to Double!
  return (((u1.ui64<<1)>>53)-1022) & (((x!=0)<<31)>>31);
}

static inline int bitsNeeded_UI64(uint64_t x){
  int shift;
  int res=0;
  
  //Get the absolute value of x:
  //x=(x^((x&(int64_t)0x8000000000000000)>>63))+((x&(int64_t)0x8000000000000000)!=0);
  //x=abs_FastI64(x);
  
  //printf("%d\n",(x&(uint64_t)0xFFFFFFFF00000000)!=0);
  shift=(((x&(uint64_t)0xFFFFFFFF00000000)!=0)*32);
  x>>=shift;
  res+=shift;
  
  //printf("%d\n",(x&(uint64_t)0x00000000FFFF0000)!=0);
  shift=(((x&(uint64_t)0x00000000FFFF0000)!=0)*16);
  x>>=shift;
  res+=shift;
  
  //printf("%d\n",(x&(uint64_t)0x000000000000FF00)!=0);
  shift=(((x&(uint64_t)0x000000000000FF00)!=0)*8);
  x>>=shift;
  res+=shift;
  
  //printf("%d\n",(x&(uint64_t)0x00000000000000F0)!=0);
  shift=(((x&(uint64_t)0x00000000000000F0)!=0)*4);
  x>>=shift;
  res+=shift;
  
  //printf("%d\n",(x&(uint64_t)0x000000000000000C)!=0);
  shift=(((x&(uint64_t)0x000000000000000C)!=0)*2);
  x>>=shift;
  res+=shift;
  
  //printf("%d\n",(x&(uint64_t)0x0000000000000002)!=0);
  shift=((x&(uint64_t)0x0000000000000002)!=0);
  x>>=shift;
  res+=shift;
  
  //printf("%d\n",(x&(uint64_t)0x0000000000000001)!=0);
  shift=((x&(uint64_t)0x0000000000000001)!=0);
  x>>=shift;
  res+=shift;
  
  //printf("BITS NEEDED: %d\n",res);
  return res;
}

static inline int bitsNeeded_I64(int64_t x){
  uint64_t ux;
  ux=abs_FastI64(x);
  return bitsNeeded_UI64(ux);
}

//Implementations(They are inline, so they should be in this header file)

static inline int myEndianType(){ //Should work for most cases. May not work at mixed endian systems.
  uint64_t n=1;
  if (*(unsigned char*)&n == 1){
    //cout<<"Little-Endian"<<endl;
    return 0;  //0 for little endian
  }
  else{
    //cout<<"Big-Endian"<<endl;
    return 1; //1 for big endian
  }
}

static inline void flipBytes_UI64(uint64_t *dataPtr){
  unsigned char*tempA;
  char temp8b;
  tempA=(unsigned char*)dataPtr;
  temp8b=tempA[7];
  tempA[7]=tempA[0];
  tempA[0]=temp8b;
  temp8b=tempA[6];
  tempA[6]=tempA[1];
  tempA[1]=temp8b;
  temp8b=tempA[5];
  tempA[5]=tempA[2];
  tempA[2]=temp8b;
  temp8b=tempA[4];
  tempA[4]=tempA[3];
  tempA[3]=temp8b;
  return;
}

//WARNING: readBits works properly only on Little Endian machines! (For Big Endians, some modifications are needed)

static inline uint64_t readBits_UI64(unsigned char* buffer,uint64_t *bitPosPtr,char numBits){ // numBits must be in range [0:56]
    uint64_t mask = ((uint64_t)0x0000000000000001<<numBits)-1;
    //cout<<"bitPos:"<<(*bitPosPtr)<<"\tbitPos>>3:"<<(*bitPosPtr>>3)<<endl;
    uint64_t temp64b = *(uint64_t*)(buffer + ( *bitPosPtr >> 3)); 
    //NOTE: bitPos>>3 is the same as bitPos/8
    temp64b >>= (*bitPosPtr) & (uint64_t)0x0000000000000007;
    
    //cout<<endl;
    //cout<<"bitpos>>3:"<<(bitPos>>3)<<" bitPos&0x7:"<<(bitPos & 0x00000007)<<" bitPos%8:"<<(bitPos%8)<<endl;
    //cout<<"Read:"<<(temp64b & mask)<<" temp64b:"<<temp64b<<" Mask:"<<mask<<" numBits:"<<numBits<<endl;
    
    (*bitPosPtr) += numBits;
    return (temp64b & mask);
}

static inline int64_t readBits_I64(unsigned char* buffer,uint64_t *bitPosPtr,char numBits){ // numBits must be in range [0:56]
  int64_t val;
  val=readBits_UI64(buffer,bitPosPtr,numBits);//Read value
  int64_t shiftAmount=64-numBits;
  val=(val<<shiftAmount)>>shiftAmount;//Sign correction
  return val;
}

//WARNING: readBits_EndianSafe is not tested on Big-Endian machines
static inline uint64_t readBits_EndianSafe(unsigned char* buffer,uint64_t *bitPosPtr,char numBits){ // numBits must be in range [0:56]
    uint64_t mask = ((uint64_t)0x0000000000000001<<numBits)-1;
    uint64_t temp64b = *(uint64_t*)(buffer + ((*bitPosPtr)>>3)); 
    //NOTE: (*bitPosPtr)>>3 is the same as (*bitPosPtr)/8
    if(myEndianType())
      flipBytes_UI64(&temp64b);
    temp64b >>= (*bitPosPtr) & (uint64_t)0x0000000000000007;
    (*bitPosPtr) += numBits;
    return temp64b & mask;
}

//WARNING: writeBits_Fast works properly only on Little Endian machines! (For Big Endians, some modifications are needed)
//The buffer should be initialized as 0's for this to work!
//Also, the range of data is not checked!(If data exceeds numBits, it may be cause problems)
static inline void writeBits_Fast(unsigned char* buffer,uint64_t *bitPosPtr,char numBits,int64_t data){
    //if(DEBUG){printf("writeBits_Fast: data:0x%lx %ld\n",data,data);} //DEBUG
    //if(DEBUG){printf("writeBits_Fast: numBits:0x%lx %ld\n",numBits,numBits);} //DEBUG
    uint64_t mask = ((uint64_t)0x0000000000000001<<numBits)-1;
    //if(DEBUG){printf("writeBits_Fast: mask:0x%lx %ld\n",mask,mask);} //DEBUG
    //if(DEBUG){printf("writeBits_Fast: data&mask:0x%lx %ld\n",((*(uint64_t*)&data)&mask),((*(uint64_t*)&data)&mask));} //DEBUG
    
    //if(DEBUG){printf("writeBits_Fast: buffer_O:0x%lx\n",*(uint64_t*)(buffer + ((*bitPosPtr)>>3)));} //DEBUG
    *(uint64_t*)(buffer + ((*bitPosPtr)>>3)) |= ((*(uint64_t*)&data)&mask) << ((*bitPosPtr) & (uint64_t)0x0000000000000007);
    //if(DEBUG){printf("writeBits_Fast: buffer_N:0x%lx\n",*(uint64_t*)(buffer + ((*bitPosPtr)>>3)));} //DEBUG

    
    (*bitPosPtr) += numBits;
}

//WARNING: writeBits_EndianSafe is not tested on Big-Endian machines
static inline void writeBits_EndianSafe(unsigned char* buffer,uint64_t *bitPosPtr,char numBits,uint64_t data){
    uint64_t mask = ((uint64_t)0x0000000000000001<<numBits)-1;
    data=data&mask;
    uint64_t temp64b_inBuffer=*(uint64_t*)(buffer + ((*bitPosPtr)>>3));
    uint64_t temp64b_outBuffer=data << ((*bitPosPtr) & (uint64_t)0x0000000000000007);
    if(myEndianType()){
      flipBytes_UI64(&temp64b_inBuffer);
    }
    temp64b_outBuffer |= temp64b_inBuffer;
    if(myEndianType()){
      flipBytes_UI64(&temp64b_outBuffer);
    }
    *(uint64_t*)(buffer + ((*bitPosPtr)>>3))=temp64b_outBuffer;  // "|=" may also work
    (*bitPosPtr) += numBits;
}


#endif
