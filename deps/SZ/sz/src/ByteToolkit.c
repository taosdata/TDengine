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
#include "zlib.h"


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
// arm32
#if defined(_TD_LINUX_64) || defined(_TD_MIPS_64) || defined(_TD_ARM_64)  || defined(_TD_DARWIN_64)
	b[0] = (unsigned char)(num>>56);
	b[1] = (unsigned char)(num>>48);
	b[2] = (unsigned char)(num>>40);
	b[3] = (unsigned char)(num>>32);
#else
	memset(b, 0, 4);
#endif
	b[4] = (unsigned char)(num>>24);
	b[5] = (unsigned char)(num>>16);
	b[6] = (unsigned char)(num>>8);
	b[7] = (unsigned char)(num);
//	if(dataEndianType==LITTLE_ENDIAN_DATA)
//		symTransform_8bytes(*b);
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
}

void convertBytesToSZParams(unsigned char* bytes, sz_params* params, sz_exedata* pde_exe)
{
	unsigned char flag1 = bytes[0];
	pde_exe->optQuantMode = (flag1 & 0x40) >> 6;
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
}
