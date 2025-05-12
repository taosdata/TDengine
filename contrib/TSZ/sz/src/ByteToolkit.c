/**
 *  @file ByteToolkit.c
 *  @author Sheng Di
 *  @date April, 2016
 *  @brief Byte Toolkit
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
 
#include <stdlib.h>
#include <string.h>
#include "sz.h" 	

INLINE int bytesToInt_bigEndian(unsigned char* bytes)
{
	int res;
	unsigned char* des = (unsigned char*)&res;
	des[0] = bytes[3];
	des[1] = bytes[2];
	des[2] = bytes[1];
	des[3] = bytes[0];
	return res;
}

/**
 * @unsigned char *b the variable to store the converted bytes (length=4)
 * @unsigned int num
 * */
INLINE void intToBytes_bigEndian(unsigned char *b, unsigned int num)
{
	unsigned char* sou =(unsigned char*)&num;
	b[0] = sou[3];
	b[1] = sou[2];
	b[2] = sou[1];
	b[3] = sou[0];
}

/**
 * @endianType: refers to the endian_type of unsigned char* b.
 * */
INLINE long bytesToLong_bigEndian(unsigned char* b) {

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

INLINE void longToBytes_bigEndian(unsigned char *b, long num) 
{
	unsigned char* sou = (unsigned char*)&num;
  if(sizeof(num) == 8) {
    // 8 bytes
	b[7] = sou[0];
	b[6] = sou[1];
	b[5] = sou[2];
	b[4] = sou[3];
	b[3] = sou[4];
	b[2] = sou[5];
	b[1] = sou[6];
	b[0] = sou[7];
  } else {
	memset(b, 0, 4);
	b[7] = sou[0];
	b[6] = sou[1];
	b[5] = sou[2];
	b[4] = sou[3];
  }
}

//TODO: debug: lfBuf.lvalue could be actually little_endian....
INLINE short getExponent_float(float value)
{
	//int ivalue = floatToBigEndianInt(value);

	lfloat lbuf;
	lbuf.value = value;
	int ivalue = lbuf.ivalue;
	
	int expValue = (ivalue & 0x7F800000) >> 23;
	expValue -= 127;
	return (short)expValue;
}

INLINE short getPrecisionReqLength_float(float precision)
{
	lfloat lbuf;
	lbuf.value = precision;
	int ivalue = lbuf.ivalue;
	
	int expValue = (ivalue & 0x7F800000) >> 23;
	expValue -= 127;
	return (short)expValue;
}

INLINE short getExponent_double(double value)
{
	//long lvalue = doubleToBigEndianLong(value);
	
	ldouble lbuf;
	lbuf.value = value;
	long lvalue = lbuf.lvalue;
	
	int expValue = (int)((lvalue & 0x7FF0000000000000) >> 52);
	expValue -= 1023;
	return (short)expValue;
}

INLINE short getPrecisionReqLength_double(double precision)
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


//the byte to input is in the big-endian format
INLINE float bytesToFloat(unsigned char* bytes)
{
	lfloat buf;
	memcpy(buf.byte, bytes, 4);
	if(sysEndianType==LITTLE_ENDIAN_SYSTEM)
		symTransform_4bytes(buf.byte);	
	return buf.value;
}

INLINE void floatToBytes(unsigned char *b, float num)
{
	lfloat buf;
	buf.value = num;
	memcpy(b, buf.byte, 4);
	if(sysEndianType==LITTLE_ENDIAN_SYSTEM)
		symTransform_4bytes(b);		
}

//the byte to input is in the big-endian format
INLINE double bytesToDouble(unsigned char* bytes)
{
	ldouble buf;
	memcpy(buf.byte, bytes, 8);
	if(sysEndianType==LITTLE_ENDIAN_SYSTEM)
		symTransform_8bytes(buf.byte);
	return buf.value;
}

INLINE void doubleToBytes(unsigned char *b, double num)
{
	ldouble buf;
	buf.value = num;
	memcpy(b, buf.byte, 8);
	if(sysEndianType==LITTLE_ENDIAN_SYSTEM)
		symTransform_8bytes(b);
}


INLINE int getMaskRightCode(int m) {
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

INLINE int getLeftMovingCode(int kMod8)
{
	return getMaskRightCode(8 - kMod8);
}

INLINE int getRightMovingSteps(int kMod8, int resiBitLength) {
	return 8 - kMod8 - resiBitLength;
}

INLINE int getRightMovingCode(int kMod8, int resiBitLength)
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

INLINE size_t bytesToSize(unsigned char* bytes, int size_type)
{
	size_t result = 0;
	if(size_type == 4)	
		result = bytesToInt_bigEndian(bytes);//4		
	else
		result = bytesToLong_bigEndian(bytes);//8	
	return result;
}

INLINE void sizeToBytes(unsigned char* outBytes, size_t size, int size_type)
{
	if(size_type == 4)
		intToBytes_bigEndian(outBytes, (unsigned int)size);//4
	else
		longToBytes_bigEndian(outBytes, (unsigned long)size);//8
}

void convertSZParamsToBytes(sz_params* params, unsigned char* result, char optQuantMode)
{
	//unsigned char* result = (unsigned char*)malloc(16);
	unsigned char buf = 0;
	buf = optQuantMode;
	buf = (buf << 1) | dataEndianType;
	buf = (buf << 1) | sysEndianType;
	buf = (buf << 2) | params->szMode;
	
	result[0] = buf;
}

void convertBytesToSZParams(unsigned char* bytes, sz_params* params, sz_exedata* pde_exe)
{
	unsigned char flag1 = bytes[0];
	pde_exe->optQuantMode = (flag1 & 0x40) >> 6;
	dataEndianType = (flag1 & 0x20) >> 5;	
	params->szMode = (flag1 & 0x0c) >> 2;
}
