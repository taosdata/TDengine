/**
 *  @file ByteToolkit.c
 *  @author Sheng Di
 *  @date April, 2016
 *  @brief Byte Toolkit
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
 
#include <stdlib.h>
#include "sz.h" 	
#include "zlib.h"

inline unsigned short bytesToUInt16_bigEndian(unsigned char* bytes)
{
	int temp = 0;
	unsigned short res = 0;
	
	temp = bytes[0] & 0xff;
	res |= temp;	

	res <<= 8;
	temp = bytes[1] & 0xff;
	res |= temp;
	
	return res;
}	
	
inline unsigned int bytesToUInt32_bigEndian(unsigned char* bytes)
{
	unsigned int temp = 0;
	unsigned int res = 0;
	
	res <<= 8;
	temp = bytes[0] & 0xff;
	res |= temp;	

	res <<= 8;
	temp = bytes[1] & 0xff;
	res |= temp;

	res <<= 8;
	temp = bytes[2] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = bytes[3] & 0xff;
	res |= temp;
	
	return res;
}

inline unsigned long bytesToUInt64_bigEndian(unsigned char* b) {
	unsigned long temp = 0;
	unsigned long res = 0;

	res <<= 8;
	temp = b[0] & 0xff;
	res |= temp;

	res <<= 8;
	temp = b[1] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[2] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[3] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[4] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[5] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[6] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[7] & 0xff;
	res |= temp;						
	
	return res;
}
	
inline short bytesToInt16_bigEndian(unsigned char* bytes)
{
	int temp = 0;
	short res = 0;
	
	temp = bytes[0] & 0xff;
	res |= temp;	

	res <<= 8;
	temp = bytes[1] & 0xff;
	res |= temp;
	
	return res;
}	
	
inline int bytesToInt32_bigEndian(unsigned char* bytes)
{
	int temp = 0;
	int res = 0;
	
	res <<= 8;
	temp = bytes[0] & 0xff;
	res |= temp;	

	res <<= 8;
	temp = bytes[1] & 0xff;
	res |= temp;

	res <<= 8;
	temp = bytes[2] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = bytes[3] & 0xff;
	res |= temp;
	
	return res;
}

inline long bytesToInt64_bigEndian(unsigned char* b) {
	long temp = 0;
	long res = 0;

	res <<= 8;
	temp = b[0] & 0xff;
	res |= temp;

	res <<= 8;
	temp = b[1] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[2] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[3] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[4] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[5] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[6] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[7] & 0xff;
	res |= temp;						
	
	return res;
}

inline int bytesToInt_bigEndian(unsigned char* bytes)
{
	int temp = 0;
	int res = 0;
	
	res <<= 8;
	temp = bytes[0] & 0xff;
	res |= temp;	

	res <<= 8;
	temp = bytes[1] & 0xff;
	res |= temp;

	res <<= 8;
	temp = bytes[2] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = bytes[3] & 0xff;
	res |= temp;
	
	return res;
}

/**
 * @unsigned char *b the variable to store the converted bytes (length=4)
 * @unsigned int num
 * */
inline void intToBytes_bigEndian(unsigned char *b, unsigned int num)
{
	b[0] = (unsigned char)(num >> 24);	
	b[1] = (unsigned char)(num >> 16);	
	b[2] = (unsigned char)(num >> 8);	
	b[3] = (unsigned char)(num);	
	
	//note: num >> xxx already considered endian_type...
//if(dataEndianType==LITTLE_ENDIAN_DATA)
//		symTransform_4bytes(*b); //change to BIG_ENDIAN_DATA
}

inline void int64ToBytes_bigEndian(unsigned char *b, uint64_t num)
{
	b[0] = (unsigned char)(num>>56);
	b[1] = (unsigned char)(num>>48);
	b[2] = (unsigned char)(num>>40);
	b[3] = (unsigned char)(num>>32);
	b[4] = (unsigned char)(num>>24);
	b[5] = (unsigned char)(num>>16);
	b[6] = (unsigned char)(num>>8);
	b[7] = (unsigned char)(num);
}

inline void int32ToBytes_bigEndian(unsigned char *b, uint32_t num)
{
	b[0] = (unsigned char)(num >> 24);	
	b[1] = (unsigned char)(num >> 16);	
	b[2] = (unsigned char)(num >> 8);	
	b[3] = (unsigned char)(num);		
}

inline void int16ToBytes_bigEndian(unsigned char *b, uint16_t num)
{
	b[0] = (unsigned char)(num >> 8);	
	b[1] = (unsigned char)(num);
}

/**
 * @endianType: refers to the endian_type of unsigned char* b.
 * */
inline long bytesToLong_bigEndian(unsigned char* b) {
	long temp = 0;
	long res = 0;

	res <<= 8;
	temp = b[0] & 0xff;
	res |= temp;

	res <<= 8;
	temp = b[1] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[2] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[3] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[4] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[5] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[6] & 0xff;
	res |= temp;
	
	res <<= 8;
	temp = b[7] & 0xff;
	res |= temp;						
	
	return res;
}

inline void longToBytes_bigEndian(unsigned char *b, unsigned long num) 
{
	b[0] = (unsigned char)(num>>56);
	b[1] = (unsigned char)(num>>48);
	b[2] = (unsigned char)(num>>40);
	b[3] = (unsigned char)(num>>32);
	b[4] = (unsigned char)(num>>24);
	b[5] = (unsigned char)(num>>16);
	b[6] = (unsigned char)(num>>8);
	b[7] = (unsigned char)(num);
//	if(dataEndianType==LITTLE_ENDIAN_DATA)
//		symTransform_8bytes(*b);
}


inline long doubleToOSEndianLong(double value)
{
	ldouble buf;
	buf.value = value;
	return buf.lvalue;
}

inline int floatToOSEndianInt(float value)
{
	lfloat buf;
	buf.value = value;
	return buf.ivalue;
}

//TODO: debug: lfBuf.lvalue could be actually little_endian....
inline short getExponent_float(float value)
{
	//int ivalue = floatToBigEndianInt(value);

	lfloat lbuf;
	lbuf.value = value;
	int ivalue = lbuf.ivalue;
	
	int expValue = (ivalue & 0x7F800000) >> 23;
	expValue -= 127;
	return (short)expValue;
}

inline short getPrecisionReqLength_float(float precision)
{
	lfloat lbuf;
	lbuf.value = precision;
	int ivalue = lbuf.ivalue;
	
	int expValue = (ivalue & 0x7F800000) >> 23;
	expValue -= 127;
//	unsigned char the1stManBit = (unsigned char)((ivalue & 0x00400000) >> 22);
//	if(the1stManBit==1)
//		expValue--;	
	return (short)expValue;
}

inline short getExponent_double(double value)
{
	//long lvalue = doubleToBigEndianLong(value);
	
	ldouble lbuf;
	lbuf.value = value;
	long lvalue = lbuf.lvalue;
	
	int expValue = (int)((lvalue & 0x7FF0000000000000) >> 52);
	expValue -= 1023;
	return (short)expValue;
}

inline short getPrecisionReqLength_double(double precision)
{
	ldouble lbuf;
	lbuf.value = precision;
	long lvalue = lbuf.lvalue;
	
	int expValue = (int)((lvalue & 0x7FF0000000000000) >> 52);
	expValue -= 1023;
//	unsigned char the1stManBit = (unsigned char)((lvalue & 0x0008000000000000) >> 51);
//	if(the1stManBit==1)
//		expValue--;
	return (short)expValue;
}

unsigned char numberOfLeadingZeros_Int(int i) {
	if (i == 0)
		return 32;
	unsigned char n = 1;
	if (((unsigned int)i) >> 16 == 0) { n += 16; i <<= 16; }
	if (((unsigned int)i) >> 24 == 0) { n +=  8; i <<=  8; }
	if (((unsigned int)i) >> 28 == 0) { n +=  4; i <<=  4; }
	if (((unsigned int)i) >> 30 == 0) { n +=  2; i <<=  2; }
	n -= ((unsigned int)i) >> 31;
	return n;
}

unsigned char numberOfLeadingZeros_Long(long i) {
	 if (i == 0)
		return 64;
	unsigned char n = 1;
	int x = (int)(((unsigned long)i) >> 32);
	if (x == 0) { n += 32; x = (int)i; }
	if (((unsigned int)x) >> 16 == 0) { n += 16; x <<= 16; }
	if (((unsigned int)x) >> 24 == 0) { n +=  8; x <<=  8; }
	if (((unsigned int)x) >> 28 == 0) { n +=  4; x <<=  4; }
	if (((unsigned int)x) >> 30 == 0) { n +=  2; x <<=  2; }
	n -= ((unsigned int)x) >> 31;
	return n;
}

unsigned char getLeadingNumbers_Int(int v1, int v2)
{
	int v = v1 ^ v2;
	return (unsigned char)numberOfLeadingZeros_Int(v);
}

unsigned char getLeadingNumbers_Long(long v1, long v2)
{
	long v = v1 ^ v2;
	return (unsigned char)numberOfLeadingZeros_Long(v);
}

/**
 * By default, the endian type is OS endian type.
 * */
short bytesToShort(unsigned char* bytes)
{
	lint16 buf;
	memcpy(buf.byte, bytes, 2);
	
	return buf.svalue;
}

void shortToBytes(unsigned char* b, short value)
{
	lint16 buf;
	buf.svalue = value;
	memcpy(b, buf.byte, 2);
}

int bytesToInt(unsigned char* bytes)
{
	lfloat buf;
	memcpy(buf.byte, bytes, 4);
	return buf.ivalue;
}

long bytesToLong(unsigned char* bytes)
{
	ldouble buf;
	memcpy(buf.byte, bytes, 8);
	return buf.lvalue;
}

//the byte to input is in the big-endian format
inline float bytesToFloat(unsigned char* bytes)
{
	lfloat buf;
	memcpy(buf.byte, bytes, 4);
	if(sysEndianType==LITTLE_ENDIAN_SYSTEM)
		symTransform_4bytes(buf.byte);	
	return buf.value;
}

inline void floatToBytes(unsigned char *b, float num)
{
	lfloat buf;
	buf.value = num;
	memcpy(b, buf.byte, 4);
	if(sysEndianType==LITTLE_ENDIAN_SYSTEM)
		symTransform_4bytes(b);		
}

//the byte to input is in the big-endian format
inline double bytesToDouble(unsigned char* bytes)
{
	ldouble buf;
	memcpy(buf.byte, bytes, 8);
	if(sysEndianType==LITTLE_ENDIAN_SYSTEM)
		symTransform_8bytes(buf.byte);
	return buf.value;
}

inline void doubleToBytes(unsigned char *b, double num)
{
	ldouble buf;
	buf.value = num;
	memcpy(b, buf.byte, 8);
	if(sysEndianType==LITTLE_ENDIAN_SYSTEM)
		symTransform_8bytes(b);
}

int extractBytes(unsigned char* byteArray, size_t k, int validLength)
{
	size_t outIndex = k/8;
	int innerIndex = k%8;
	unsigned char intBytes[4];
	int length = innerIndex + validLength;
	int byteNum = 0;
	if(length%8==0)
		byteNum = length/8;
	else
		byteNum = length/8+1;
	
	int i;
	for(i = 0;i<byteNum;i++)
		intBytes[exe_params->SZ_SIZE_TYPE-byteNum+i] = byteArray[outIndex+i];
	int result = bytesToInt_bigEndian(intBytes);
	int rightMovSteps = innerIndex +(8 - (innerIndex+validLength)%8)%8;
	result = result << innerIndex;
	switch(byteNum)
	{
	case 1:
		result = result & 0xff;
		break;
	case 2:
		result = result & 0xffff;
		break;
	case 3:
		result = result & 0xffffff;
		break;
	case 4:
		break;
	default: 
		printf("Error: other cases are impossible...\n");
		exit(0);
	}
	result = result >> rightMovSteps;
	
	return result;
}

inline int getMaskRightCode(int m) {
	switch (m) {
	case 1:
		return 0x01;
	case 2:
		return 0x03;
	case 3:
		return 0x07;
	case 4:
		return 0x0F;
	case 5:
		return 0x1F;
	case 6:
		return 0x3F;
	case 7:
		return 0X7F;
	case 8:
		return 0XFF;
	default:
		return 0;
	}
}

inline int getLeftMovingCode(int kMod8)
{
	return getMaskRightCode(8 - kMod8);
}

inline int getRightMovingSteps(int kMod8, int resiBitLength) {
	return 8 - kMod8 - resiBitLength;
}

inline int getRightMovingCode(int kMod8, int resiBitLength)
{
	int rightMovingSteps = 8 - kMod8 - resiBitLength;
	if(rightMovingSteps < 0)
	{
		switch(-rightMovingSteps)
		{
		case 1:
			return 0x80;
		case 2:
			return 0xC0;
		case 3:
			return 0xE0;
		case 4:
			return 0xF0;
		case 5:
			return 0xF8;
		case 6:
			return 0xFC;
		case 7:
			return 0XFE;
		default:
			return 0;
		}    		
	}
	else //if(rightMovingSteps >= 0)
	{
		int a = getMaskRightCode(8 - kMod8);
		int b = getMaskRightCode(8 - kMod8 - resiBitLength);
		int c = a - b;
		return c;
	}
}

short* convertByteDataToShortArray(unsigned char* bytes, size_t byteLength)
{
	lint16 ls;
	size_t i, stateLength = byteLength/2;
	short* states = (short*)malloc(stateLength*sizeof(short));
	if(sysEndianType==dataEndianType)
	{	
		for(i=0;i<stateLength;i++)
		{
			ls.byte[0] = bytes[i*2];
			ls.byte[1] = bytes[i*2+1];
			states[i] = ls.svalue;
		}
	}
	else
	{
		for(i=0;i<stateLength;i++)
		{
			ls.byte[0] = bytes[i*2+1];
			ls.byte[1] = bytes[i*2];
			states[i] = ls.svalue;
		}		
	}
	return states;
} 

unsigned short* convertByteDataToUShortArray(unsigned char* bytes, size_t byteLength)
{
	lint16 ls;
	size_t i, stateLength = byteLength/2;
	unsigned short* states = (unsigned short*)malloc(stateLength*sizeof(unsigned short));
	if(sysEndianType==dataEndianType)
	{	
		for(i=0;i<stateLength;i++)
		{
			ls.byte[0] = bytes[i*2];
			ls.byte[1] = bytes[i*2+1];
			states[i] = ls.usvalue;
		}
	}
	else
	{
		for(i=0;i<stateLength;i++)
		{
			ls.byte[0] = bytes[i*2+1];
			ls.byte[1] = bytes[i*2];
			states[i] = ls.usvalue;
		}		
	}
	return states;
} 

void convertShortArrayToBytes(short* states, size_t stateLength, unsigned char* bytes)
{
	lint16 ls;
	size_t i;
	if(sysEndianType==dataEndianType)
	{
		for(i=0;i<stateLength;i++)
		{
			ls.svalue = states[i];
			bytes[i*2] = ls.byte[0];
			bytes[i*2+1] = ls.byte[1];
		}		
	}
	else
	{
		for(i=0;i<stateLength;i++)
		{
			ls.svalue = states[i];
			bytes[i*2] = ls.byte[1];
			bytes[i*2+1] = ls.byte[0];
		}			
	}
}

void convertUShortArrayToBytes(unsigned short* states, size_t stateLength, unsigned char* bytes)
{
	lint16 ls;
	size_t i;
	if(sysEndianType==dataEndianType)
	{
		for(i=0;i<stateLength;i++)
		{
			ls.usvalue = states[i];
			bytes[i*2] = ls.byte[0];
			bytes[i*2+1] = ls.byte[1];
		}		
	}
	else
	{
		for(i=0;i<stateLength;i++)
		{
			ls.usvalue = states[i];
			bytes[i*2] = ls.byte[1];
			bytes[i*2+1] = ls.byte[0];
		}			
	}
}

void convertIntArrayToBytes(int* states, size_t stateLength, unsigned char* bytes)
{
	lint32 ls;
	size_t index = 0;
	size_t i;
	if(sysEndianType==dataEndianType)
	{
		for(i=0;i<stateLength;i++)
		{
			index = i << 2; //==i*4
			ls.ivalue = states[i];
			bytes[index] = ls.byte[0];
			bytes[index+1] = ls.byte[1];
			bytes[index+2] = ls.byte[2];
			bytes[index+3] = ls.byte[3];
		}		
	}
	else
	{
		for(i=0;i<stateLength;i++)
		{
			index = i << 2; //==i*4
			ls.ivalue = states[i];
			bytes[index] = ls.byte[3];
			bytes[index+1] = ls.byte[2];
			bytes[index+2] = ls.byte[1];
			bytes[index+3] = ls.byte[0];
		}			
	}
}

void convertUIntArrayToBytes(unsigned int* states, size_t stateLength, unsigned char* bytes)
{
	lint32 ls;
	size_t index = 0;
	size_t i;
	if(sysEndianType==dataEndianType)
	{
		for(i=0;i<stateLength;i++)
		{
			index = i << 2; //==i*4
			ls.uivalue = states[i];
			bytes[index] = ls.byte[0];
			bytes[index+1] = ls.byte[1];
			bytes[index+2] = ls.byte[2];
			bytes[index+3] = ls.byte[3];
		}		
	}
	else
	{
		for(i=0;i<stateLength;i++)
		{
			index = i << 2; //==i*4
			ls.uivalue = states[i];
			bytes[index] = ls.byte[3];
			bytes[index+1] = ls.byte[2];
			bytes[index+2] = ls.byte[1];
			bytes[index+3] = ls.byte[0];
		}			
	}
}

void convertLongArrayToBytes(int64_t* states, size_t stateLength, unsigned char* bytes)
{
	lint64 ls;
	size_t index = 0;
	size_t i;
	if(sysEndianType==dataEndianType)
	{
		for(i=0;i<stateLength;i++)
		{
			index = i << 3; //==i*8
			ls.lvalue = states[i];
			bytes[index] = ls.byte[0];
			bytes[index+1] = ls.byte[1];
			bytes[index+2] = ls.byte[2];
			bytes[index+3] = ls.byte[3];
			bytes[index+4] = ls.byte[4];
			bytes[index+5] = ls.byte[5];
			bytes[index+6] = ls.byte[6];
			bytes[index+7] = ls.byte[7];	
		}		
	}
	else
	{
		for(i=0;i<stateLength;i++)
		{
			index = i << 3; //==i*8
			ls.lvalue = states[i];
			bytes[index] = ls.byte[7];
			bytes[index+1] = ls.byte[6];
			bytes[index+2] = ls.byte[5];
			bytes[index+3] = ls.byte[4];
			bytes[index+4] = ls.byte[3];
			bytes[index+5] = ls.byte[2];
			bytes[index+6] = ls.byte[1];
			bytes[index+7] = ls.byte[0];	
		}			
	}
}

void convertULongArrayToBytes(uint64_t* states, size_t stateLength, unsigned char* bytes)
{
	lint64 ls;
	size_t index = 0;
	size_t i;
	if(sysEndianType==dataEndianType)
	{
		for(i=0;i<stateLength;i++)
		{
			index = i << 3; //==i*8
			ls.ulvalue = states[i];
			bytes[index] = ls.byte[0];
			bytes[index+1] = ls.byte[1];
			bytes[index+2] = ls.byte[2];
			bytes[index+3] = ls.byte[3];
			bytes[index+4] = ls.byte[4];
			bytes[index+5] = ls.byte[5];
			bytes[index+6] = ls.byte[6];
			bytes[index+7] = ls.byte[7];			
		}		
	}
	else
	{
		for(i=0;i<stateLength;i++)
		{
			index = i << 3; //==i*8
			ls.ulvalue = states[i];
			bytes[index] = ls.byte[7];
			bytes[index+1] = ls.byte[6];
			bytes[index+2] = ls.byte[5];
			bytes[index+3] = ls.byte[4];
			bytes[index+4] = ls.byte[3];
			bytes[index+5] = ls.byte[2];
			bytes[index+6] = ls.byte[1];
			bytes[index+7] = ls.byte[0];	
		}			
	}
}


inline size_t bytesToSize(unsigned char* bytes)
{
	size_t result = 0;
	if(exe_params->SZ_SIZE_TYPE==4)	
		result = bytesToInt_bigEndian(bytes);//4		
	else
		result = bytesToLong_bigEndian(bytes);//8	
	return result;
}

inline void sizeToBytes(unsigned char* outBytes, size_t size)
{
	if(exe_params->SZ_SIZE_TYPE==4)
		intToBytes_bigEndian(outBytes, size);//4
	else
		longToBytes_bigEndian(outBytes, size);//8
}

/**
 * put 'buf_nbBits' bits represented by buf into a long byte stream (the current output byte pointer is p, where offset is the number of bits already filled out for this byte so far)
 * */
void put_codes_to_output(unsigned int buf, int bitSize, unsigned char** p, int* lackBits, size_t *outSize)
{
	int byteSize, byteSizep;
	if(*lackBits == 0)
	{
		byteSize = bitSize%8==0 ? bitSize/8 : bitSize/8+1; //it's equal to the number of bytes involved (for *outSize)
		byteSizep = bitSize >> 3; //it's used to move the pointer p for next data
		intToBytes_bigEndian(*p, buf);
		(*p) += byteSizep;
		*outSize += byteSize;
		(*lackBits) = bitSize%8==0 ? 0 : 8 - bitSize%8;
	}
	else
	{
		**p = (**p) | (unsigned char)(buf >> (32 - *lackBits));
		if((*lackBits) < bitSize)
		{
			(*p)++;
			int newCode = buf << (*lackBits);
			intToBytes_bigEndian(*p, newCode);
			bitSize -= *lackBits;
			byteSizep = bitSize >> 3; // =bitSize/8
			byteSize = bitSize%8==0 ? byteSizep : byteSizep+1;
			*p += byteSizep;
			(*outSize)+=byteSize;
			(*lackBits) = bitSize%8==0 ? 0 : 8 - bitSize%8;
		}
		else
		{
			(*lackBits) -= bitSize;
			if(*lackBits==0)
				(*p)++;
		}
	}
}

void convertSZParamsToBytes(sz_params* params, unsigned char* result)
{
	//unsigned char* result = (unsigned char*)malloc(16);
	unsigned char buf = 0;
	//flag1: exe_params->optQuantMode(1bit), dataEndianType(1bit), sysEndianType(1bit), conf_params->szMode (1bit), conf_params->gzipMode (2bits), pwrType (2bits)
	buf = exe_params->optQuantMode;
	buf = (buf << 1) | dataEndianType;
	buf = (buf << 1) | sysEndianType;
	buf = (buf << 2) | params->szMode;
	
	int tmp = 0;
	switch(params->gzipMode)
	{
	case Z_BEST_SPEED:
		tmp = 0;
		break;
	case Z_DEFAULT_STRATEGY:
		tmp = 1;
		break;
	case Z_BEST_COMPRESSION:
		tmp = 2;
		break;
	}
	buf = (buf << 2) | tmp;
	//buf = (buf << 2) |  params->pwr_type; //deprecated
	result[0] = buf;
	
    //sampleDistance; //2 bytes
    int16ToBytes_bigEndian(&result[1], params->sampleDistance);
    
    //conf_params->predThreshold;  // 2 bytes
    short tmp2 = params->predThreshold * 10000;
    int16ToBytes_bigEndian(&result[3], tmp2);
     
    //errorBoundMode; //4bits(0.5 byte)
    result[5] = params->errorBoundMode;
    
    //data type (float, double, int8, int16, ....) //10 choices, so 4 bits
    result[5] = (result[5] << 4) | (params->dataType & 0x17);
     
    //result[5]: abs_err_bound or psnr //4 bytes
    //result[9]: rel_bound_ratio or pwr_err_bound//4 bytes 
    switch(params->errorBoundMode)
    {
	case ABS:
		floatToBytes(&result[6], (float)(params->absErrBound)); //big_endian
		memset(&result[10], 0, 4);
		break;
	case REL:
		memset(&result[6], 0, 4);
		floatToBytes(&result[10], (float)(params->relBoundRatio)); //big_endian
		break;
	case ABS_AND_REL:
	case ABS_OR_REL:
		floatToBytes(&result[6], (float)(params->absErrBound));
		floatToBytes(&result[10], (float)(params->relBoundRatio)); //big_endian
		break;
	case PSNR:
		floatToBytes(&result[6], (float)(params->psnr));
		memset(&result[9], 0, 4);
		break;
	case ABS_AND_PW_REL:
	case ABS_OR_PW_REL:
		floatToBytes(&result[6], (float)(params->absErrBound));
		floatToBytes(&result[10], (float)(params->pw_relBoundRatio)); //big_endian	
		break;
	case REL_AND_PW_REL:
	case REL_OR_PW_REL:
		floatToBytes(&result[6], (float)(params->relBoundRatio));
		floatToBytes(&result[10], (float)(params->pw_relBoundRatio)); //big_endian	
		break;
	case PW_REL:
		memset(&result[6], 0, 4);
		floatToBytes(&result[10], (float)(params->pw_relBoundRatio)); //big_endian
		break;		
	}
   
    //compressor
    result[14] = (unsigned char)params->sol_ID;
    
    //int16ToBytes_bigEndian(&result[14], (short)(params->segment_size));
    
    if(exe_params->optQuantMode==1)
		int32ToBytes_bigEndian(&result[16], params->max_quant_intervals);
	else
		int32ToBytes_bigEndian(&result[16], params->quantization_intervals);
	
	if(params->dataType==SZ_FLOAT)
	{
		floatToBytes(&result[20], params->fmin);
		floatToBytes(&result[24], params->fmax);		
	}
	else
	{
		doubleToBytes(&result[20], params->dmin);
		doubleToBytes(&result[28], params->dmax);		
	}

}

void convertBytesToSZParams(unsigned char* bytes, sz_params* params)
{
	unsigned char flag1 = bytes[0];
	exe_params->optQuantMode = (flag1 & 0x40) >> 6;
	dataEndianType = (flag1 & 0x20) >> 5;
	//sysEndianType = (flag1 & 0x10) >> 4;
	
	params->szMode = (flag1 & 0x0c) >> 2;
	
	int tmp = (flag1 & 0x03);
	switch(tmp)
	{
	case 0:
		params->gzipMode = Z_BEST_SPEED;
		break;
	case 1:
		params->gzipMode = Z_DEFAULT_STRATEGY;
		break;
	case 2:
		params->gzipMode = Z_BEST_COMPRESSION;
		break;
	}
	
	//params->pwr_type = (flag1 & 0x03) >> 0;

	params->sampleDistance = bytesToInt16_bigEndian(&bytes[1]);
	
	params->predThreshold = 1.0*bytesToInt16_bigEndian(&bytes[3])/10000.0;
    
    params->dataType = bytes[5] & 0x07;

	params->errorBoundMode = (bytes[5] & 0xf0) >> 4;

    switch(params->errorBoundMode)
    {
	case ABS:
		params->absErrBound = bytesToFloat(&bytes[6]);
		break;
	case REL:
		params->relBoundRatio = bytesToFloat(&bytes[10]);
		break;
	case ABS_AND_REL:
	case ABS_OR_REL:
		params->absErrBound = bytesToFloat(&bytes[6]);
		params->relBoundRatio = bytesToFloat(&bytes[10]);
		break;
	case PSNR:
		params->psnr = bytesToFloat(&bytes[6]);
		break;
	case ABS_AND_PW_REL:
	case ABS_OR_PW_REL:
		params->absErrBound = bytesToFloat(&bytes[6]);
		params->pw_relBoundRatio = bytesToFloat(&bytes[10]);	
		break;
	case REL_AND_PW_REL:
	case REL_OR_PW_REL:
		params->relBoundRatio = bytesToFloat(&bytes[6]);
		params->pw_relBoundRatio = bytesToFloat(&bytes[10]);	
		break;
	case PW_REL:
		params->pw_relBoundRatio = bytesToFloat(&bytes[10]);		
	}
	
    //segment_size  // 2 bytes
    //params->segment_size = bytesToInt16_bigEndian(&bytes[14]);	
    params->sol_ID = (int)(bytes[14]);
    
    if(exe_params->optQuantMode==1)
    {
		params->max_quant_intervals = bytesToInt32_bigEndian(&bytes[16]);
		params->quantization_intervals = 0;
	}
	else
	{
		params->max_quant_intervals = 0;
		params->quantization_intervals = bytesToInt32_bigEndian(&bytes[16]);  
	}
	
	if(params->dataType==SZ_FLOAT)
	{
		params->fmin = bytesToFloat(&bytes[20]);
		params->fmax = bytesToFloat(&bytes[24]);		
	}
	else if(params->dataType==SZ_DOUBLE)
	{
		params->dmin = bytesToDouble(&bytes[20]);
		params->dmax = bytesToDouble(&bytes[28]);				
	}

}
