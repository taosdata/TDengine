#include "sm4.h"

#define SM4_ROUND            32

static unsigned int FK[4]={
    0xA3B1BAC6,0x56AA3350,0x677D9197,0xB27022DC
};

static unsigned int CK[SM4_ROUND]={
    0x00070e15, 0x1c232a31, 0x383f464d, 0x545b6269,
    0x70777e85, 0x8c939aa1, 0xa8afb6bd, 0xc4cbd2d9,
    0xe0e7eef5, 0xfc030a11, 0x181f262d, 0x343b4249,
    0x50575e65, 0x6c737a81, 0x888f969d, 0xa4abb2b9,
    0xc0c7ced5, 0xdce3eaf1, 0xf8ff060d, 0x141b2229,
    0x30373e45, 0x4c535a61, 0x686f767d, 0x848b9299,
    0xa0a7aeb5, 0xbcc3cad1, 0xd8dfe6ed, 0xf4fb0209,
    0x10171e25, 0x2c333a41, 0x484f565d, 0x646b7279
};

static unsigned char Sbox[256]={
    0xd6,0x90,0xe9,0xfe,0xcc,0xe1,0x3d,0xb7,0x16,0xb6,0x14,0xc2,0x28,0xfb,0x2c,0x05,
    0x2b,0x67,0x9a,0x76,0x2a,0xbe,0x04,0xc3,0xaa,0x44,0x13,0x26,0x49,0x86,0x06,0x99,
    0x9c,0x42,0x50,0xf4,0x91,0xef,0x98,0x7a,0x33,0x54,0x0b,0x43,0xed,0xcf,0xac,0x62,
    0xe4,0xb3,0x1c,0xa9,0xc9,0x08,0xe8,0x95,0x80,0xdf,0x94,0xfa,0x75,0x8f,0x3f,0xa6,
    0x47,0x07,0xa7,0xfc,0xf3,0x73,0x17,0xba,0x83,0x59,0x3c,0x19,0xe6,0x85,0x4f,0xa8,
    0x68,0x6b,0x81,0xb2,0x71,0x64,0xda,0x8b,0xf8,0xeb,0x0f,0x4b,0x70,0x56,0x9d,0x35,
    0x1e,0x24,0x0e,0x5e,0x63,0x58,0xd1,0xa2,0x25,0x22,0x7c,0x3b,0x01,0x21,0x78,0x87,
    0xd4,0x00,0x46,0x57,0x9f,0xd3,0x27,0x52,0x4c,0x36,0x02,0xe7,0xa0,0xc4,0xc8,0x9e,
    0xea,0xbf,0x8a,0xd2,0x40,0xc7,0x38,0xb5,0xa3,0xf7,0xf2,0xce,0xf9,0x61,0x15,0xa1,
    0xe0,0xae,0x5d,0xa4,0x9b,0x34,0x1a,0x55,0xad,0x93,0x32,0x30,0xf5,0x8c,0xb1,0xe3,
    0x1d,0xf6,0xe2,0x2e,0x82,0x66,0xca,0x60,0xc0,0x29,0x23,0xab,0x0d,0x53,0x4e,0x6f,
    0xd5,0xdb,0x37,0x45,0xde,0xfd,0x8e,0x2f,0x03,0xff,0x6a,0x72,0x6d,0x6c,0x5b,0x51,
    0x8d,0x1b,0xaf,0x92,0xbb,0xdd,0xbc,0x7f,0x11,0xd9,0x5c,0x41,0x1f,0x10,0x5a,0xd8,
    0x0a,0xc1,0x31,0x88,0xa5,0xcd,0x7b,0xbd,0x2d,0x74,0xd0,0x12,0xb8,0xe5,0xb4,0xb0,
    0x89,0x69,0x97,0x4a,0x0c,0x96,0x77,0x7e,0x65,0xb9,0xf1,0x09,0xc5,0x6e,0xc6,0x84,
    0x18,0xf0,0x7d,0xec,0x3a,0xdc,0x4d,0x20,0x79,0xee,0x5f,0x3e,0xd7,0xcb,0x39,0x48
};

#define ROL(x,y)    ((x)<<(y) |    (x)>>(32-(y)))

unsigned int SMS4_T1(unsigned int    dwA)
{
    unsigned char    a0[4]={0};
    unsigned char    b0[4]={0};
    unsigned int    dwB=0;
    unsigned int    dwC=0;
    int                i=0;
/*
    for (i=0;i<4;i++)
    {
        a0[i] = (unsigned char)((dwA>>(i*8)) & 0xff);
        b0[i] = Sbox[a0[i]];
        dwB  |= (b0[i]<<(i*8));
    }
*/
		a0[0] = (unsigned char)((dwA) & 0xff);
    b0[0] = Sbox[a0[0]];
    dwB  |= (b0[0]);
    
    a0[1] = (unsigned char)((dwA>>(8)) & 0xff);
    b0[1] = Sbox[a0[1]];
    dwB  |= (b0[1]<<(8));

    a0[2] = (unsigned char)((dwA>>(16)) & 0xff);
    b0[2] = Sbox[a0[2]];
    dwB  |= (b0[2]<<(16));
    
    a0[3] = (unsigned char)((dwA>>(24)) & 0xff);
    b0[3] = Sbox[a0[3]]; 
    dwB  |= (b0[3]<<(24));          // 2013-08-13
    
    dwC=dwB^ROL(dwB,2)^ROL(dwB,10)^ROL(dwB,18)^ROL(dwB,24);

    return dwC;
}

unsigned int SMS4_T2(unsigned int    dwA)
{
    unsigned char    a0[4]={0};
    unsigned char    b0[4]={0};
    unsigned int    dwB=0;
    unsigned int    dwC=0;
    int        i=0;
/*
    for (i=0;i<4;i++)
    {
        a0[i] = (unsigned char)((dwA>>(i*8)) & 0xff);
        b0[i] = Sbox[a0[i]];
        dwB  |= (b0[i]<<(i*8));
    }
*/
		a0[0] = (unsigned char)((dwA) & 0xff);
    b0[0] = Sbox[a0[0]];
    dwB  |= (b0[0]);
    
    a0[1] = (unsigned char)((dwA>>(8)) & 0xff);
    b0[1] = Sbox[a0[1]];
    dwB  |= (b0[1]<<(8));

    a0[2] = (unsigned char)((dwA>>(16)) & 0xff);
    b0[2] = Sbox[a0[2]];
    dwB  |= (b0[2]<<(16));
    
    a0[3] = (unsigned char)((dwA>>(24)) & 0xff);
    b0[3] = Sbox[a0[3]]; 
    dwB  |= (b0[3]<<(24));          // 2013-08-13    
    
    dwC=dwB^ROL(dwB,13)^ROL(dwB,23);

    return dwC;
}

/* MK[4] is the Encrypt Key, rk[32] is Round Key */
void SMS4_Key_Expansion(unsigned int MK[],    unsigned int rk[])
{
    unsigned int    K[4]={0};
    int        i=0;

    for (i=0;i<4;i++)
    {
        K[i]    =    MK[i]    ^    FK[i];
    }

    for (i=0;i<SM4_ROUND;i++)
    {
        K[i%4]^=SMS4_T2(K[(i+1)%4]^K[(i+2)%4]^K[(i+3)%4]^CK[i]);
        rk[i]=K[i%4];
    }
}

/* X[4] is PlainText, rk[32] is round Key, Y[4] is CipherText */
void SMS4_ECB_Encryption_Core(unsigned int X[], unsigned int rk[], unsigned int Y[])
{
    unsigned int    tempX[4]={0};
    int                i=0;
/*    
    for (i=0;i<4;i++)
    {
        tempX[i]=X[i];
    }
*/
		tempX[0]=X[0];    
    tempX[1]=X[1];        
    tempX[2]=X[2];    
    tempX[3]=X[3];  
/*    
    for (i=0;i<SM4_ROUND;i++)
    {
        tempX[i%4]^=SMS4_T1(tempX[(i+1)%4]^tempX[(i+2)%4]^tempX[(i+3)%4]^rk[i]);
    }
*/
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[0]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[1]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[2]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[3]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[4]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[5]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[6]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[7]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[8]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[9]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[10]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[11]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[12]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[13]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[14]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[15]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[16]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[17]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[18]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[19]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[20]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[21]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[22]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[23]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[24]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[25]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[26]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[27]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[28]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[29]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[30]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[31]);
/*    for (i=0;i<4;i++)
    {
        Y[i]=tempX[3-i];
    }*/
    Y[0]=tempX[3];
    Y[1]=tempX[2];
    Y[2]=tempX[1];
    Y[3]=tempX[0]; // 2013-08-19 
}

/* X[4] is PlainText, rk[32] is round Key, Y[4] is CipherText */
void SMS4_ECB_Decryption_Core(unsigned int X[], unsigned int rk[], unsigned int Y[])
{
    unsigned int    tempX[4]={0};
    int                i=0;
    
    /*    for (i=0;i<4;i++)
    {
        tempX[i]=X[i];
    }
		*/
		tempX[0]=X[0];    
    tempX[1]=X[1];        
    tempX[2]=X[2];    
    tempX[3]=X[3]; // 2013-08-19 
   
/*    for (i=0;i<SM4_ROUND;i++)
    {
        tempX[i%4]^=SMS4_T1(tempX[(i+1)%4]^tempX[(i+2)%4]^tempX[(i+3)%4]^rk[(31-i)]);
    }
*/
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[31]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[30]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[29]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[28]); 

		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[27]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[26]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[25]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[24]); 

		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[23]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[22]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[21]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[20]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[19]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[18]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[17]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[16]); 

		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[15]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[14]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[13]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[12]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[11]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[10]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[9]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[8]); 

		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[7]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[6]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[5]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[4]); 
		
		tempX[0]^=SMS4_T1(tempX[1]^tempX[2]^tempX[3]^rk[3]);    
		tempX[1]^=SMS4_T1(tempX[2]^tempX[3]^tempX[0]^rk[2]); 
		tempX[2]^=SMS4_T1(tempX[3]^tempX[0]^tempX[1]^rk[1]);     
		tempX[3]^=SMS4_T1(tempX[0]^tempX[1]^tempX[2]^rk[0]);// 2013-08-19 
/*    for (i=0;i<4;i++)
    {
        Y[i]=tempX[3-i];
    }*/
    Y[0]=tempX[3];
    Y[1]=tempX[2];
    Y[2]=tempX[1];
    Y[3]=tempX[0]; // 2013-08-19 
}

void SMS4_convert_to_network_order(unsigned int* src,unsigned int* dst,int count)
{
	int i=0;

	for ( ; i<count; i++ )
	{
		unsigned char* ps = (unsigned char*)(src+i);
		unsigned char* pd = (unsigned char*)(dst+i);
		
		pd[0] = ps[3];
		pd[1] = ps[2];
		pd[2] = ps[1];
		pd[3] = ps[0];
	}
}

void SMS4_convert_to_host_order(unsigned int* src,unsigned int* dst,int count)
{
	SMS4_convert_to_network_order(src,dst,count);
}

void SMS4_ECB_Encryption(unsigned char plaintext[16], unsigned char key[16], unsigned char ciphertext[16])
{
	unsigned int _pt[4];
	unsigned int _ky[4];
	unsigned int _ct[4];
	unsigned int _rk[32];

	SMS4_convert_to_network_order((unsigned int*)plaintext,_pt,4);
	SMS4_convert_to_network_order((unsigned int*)key,_ky,4);

	SMS4_Key_Expansion(_ky,_rk);
	SMS4_ECB_Encryption_Core(_pt,_rk,_ct);

	SMS4_convert_to_host_order(_ct,(unsigned int*)ciphertext,4);
}

void Key_Expansion_init( unsigned char key[16], unsigned int rk[32])
{
	unsigned int _ky[4];

	SMS4_convert_to_network_order((unsigned int*)key,_ky,4);
	SMS4_Key_Expansion (_ky,rk);
}

void SMS4_ECB_EncryptionEx(unsigned char plaintext[16], unsigned int key[32], unsigned char ciphertext[16])
{
	unsigned int _pt[4];
	unsigned int _ct[4];
	
	SMS4_convert_to_network_order((unsigned int*)plaintext,_pt,4);
	SMS4_ECB_Encryption_Core(_pt,key,_ct);
	SMS4_convert_to_host_order(_ct,(unsigned int*)ciphertext,4);
}

void SMS4_ECB_Decryption(unsigned char ciphertext[16], unsigned char key[16], unsigned char plaintext[16])
{
	unsigned int _ct[4];
	unsigned int _ky[4];
	unsigned int _pt[4];
	unsigned int _rk[32];

	SMS4_convert_to_network_order((unsigned int*)ciphertext,_ct,4);
	SMS4_convert_to_network_order((unsigned int*)key,_ky,4);

	SMS4_Key_Expansion(_ky,_rk);
	SMS4_ECB_Decryption_Core(_ct,_rk,_pt);

	SMS4_convert_to_host_order(_pt,(unsigned int*)plaintext,4);
}

void SMS4_ECB_DecryptionEx(unsigned char ciphertext[16], unsigned int key[32], unsigned char plaintext[16])
{
	unsigned int _ct[4];
	unsigned int _pt[4];
	
	SMS4_convert_to_network_order((unsigned int*)ciphertext,_ct,4);
	SMS4_ECB_Decryption_Core(_ct,key,_pt);
	SMS4_convert_to_host_order(_pt,(unsigned int*)plaintext,4);
}

void SMS4_CBC_Encryption(unsigned char plaintext[16], unsigned char key[16], unsigned char iv[16], unsigned char ciphertext[16])
{
    unsigned char plaintextNew[16];
    int i = 0;
    for (i = 0; i < 16; i ++)
    {
        plaintextNew[i] = plaintext[i] ^ iv[i];
    }

    SMS4_ECB_Encryption(plaintextNew, key, ciphertext);
}

void SMS4_CBC_Decryption(unsigned char ciphertext[16], unsigned char key[16], unsigned char iv[16], unsigned char plaintext[16])
{
    unsigned char plaintextTemp[16];
    int i = 0;
    SMS4_ECB_Decryption(ciphertext, key, plaintextTemp);
    for (i = 0; i < 16; i ++)
    {
        plaintext[i] = plaintextTemp[i] ^ iv[i];
    }
}

void SMS4_CBC_EncryptionEx(unsigned char plaintext[16], unsigned int key[32], unsigned char iv[16], unsigned char ciphertext[16])
{
    unsigned char plaintextNew[16];
    int i = 0;
    for (i = 0; i < 16; i ++)
    {
        plaintextNew[i] = plaintext[i] ^ iv[i];
    }

    SMS4_ECB_EncryptionEx(plaintextNew, key, ciphertext);
}

void SMS4_CBC_DecryptionEx(unsigned char ciphertext[16], unsigned int key[32], unsigned char iv[16], unsigned char plaintext[16])
{
    unsigned char plaintextTemp[16];
    int i = 0;
    SMS4_ECB_DecryptionEx(ciphertext, key, plaintextTemp);
    for (i = 0; i < 16; i ++)
    {
        plaintext[i] = plaintextTemp[i] ^ iv[i];
    }
}

/*
int SM4_ECB_Encrypt( unsigned char *pKey, 
                      unsigned int KeyLen, 
                      unsigned char *pInData,
                      unsigned int inDataLen,
                      unsigned char *pOutData, 
                      unsigned int *pOutDataLen) 
{
    int i = 0;
    //int rv = 0;
    int loop = 0;
    unsigned int rk[32];	
	
    *pOutDataLen = 0;
	
    if(KeyLen != 16)
    {
        return 1;
    }
    if(inDataLen % 16 != 0)
    {
        return 1;
    }

    Key_Expansion_init(pKey,rk);
    loop = inDataLen / 16;
    for (i = 0; i < loop; i ++)
    {
        //SMS4_ECB_Encryption(pInData + i * 16, pKey, pOutData + i * 16);
        SMS4_ECB_EncryptionEx(pInData + i * 16, rk, pOutData + i * 16);
    }
    *pOutDataLen = inDataLen;
    return 0;
}

int SM4_ECB_Decrypt(  unsigned char *pKey, 
                      unsigned int KeyLen, 
                      unsigned char *pInData, 
                      unsigned int inDataLen,
                      unsigned char *pOutData,
                      unsigned int *pOutDataLen) 
{
    int i = 0;
    // int rv = 0;
    int loop = 0;
    unsigned int rk[32];
	
    *pOutDataLen = 0;
    if(KeyLen != 16)
    {
        return 1;
    }
    if(inDataLen % 16 != 0)
    {
        return 1;
    }

    Key_Expansion_init(pKey,rk);		
    loop = inDataLen / 16;
    for (i = 0; i < loop; i ++)
    {
        //SMS4_ECB_Decryption(pInData + i * 16, pKey, pOutData + i * 16);
	 SMS4_ECB_DecryptionEx(pInData + i * 16, rk, pOutData + i * 16);
    }
    *pOutDataLen = inDataLen;
    return 0;
}
*/
int SM4_CBC_Encrypt( unsigned char *pKey, 
                     unsigned int KeyLen,
                     unsigned char *pIV, 
                     unsigned int ivLen,
                     unsigned char *pInData, 
                     unsigned int inDataLen,
                     unsigned char *pOutData, 
                     unsigned int *pOutDataLen) 
{
    int i = 0;
    // int rv = 0;
    int loop = 0;
    unsigned char *pIVTemp = NULL;
    unsigned int rk[32];
	
    *pOutDataLen = 0;
    if(KeyLen != 16)
    {
        return 1;
    }
    if(inDataLen % 16 != 0)
    {
        return 1;
    }
    if(ivLen != 16)
    {
        return 1;
    }
    Key_Expansion_init(pKey,rk);		
    loop = inDataLen / 16;
    pIVTemp = pIV;
    for (i = 0; i < loop; i ++)
    {
        SMS4_CBC_EncryptionEx(pInData + i * 16, rk, pIVTemp, pOutData + i * 16);
        pIVTemp = pOutData + i * 16;
    }
    *pOutDataLen = inDataLen;
    return 0;
}

int SM4_CBC_Decrypt(unsigned char *pKey, 
                    unsigned int KeyLen, 
                    unsigned char *pIV, 
                    unsigned int ivLen,
                    unsigned char *pInData,
                    unsigned int inDataLen,
                    unsigned char *pOutData, 
                    unsigned int *pOutDataLen) 
{
    int i = 0;
    // int rv = 0;
    int loop = 0;
    unsigned char *pIVTemp = NULL;
    unsigned int rk[32];
	
    *pOutDataLen = 0;
    if(KeyLen != 16)
    {
        return 1;
    }
    if(inDataLen % 16 != 0)
    {
        return 1;
    }
    if(ivLen != 16)
    {
        return 1;
    }
    Key_Expansion_init(pKey,rk);		
    loop = inDataLen / 16;
    pIVTemp = pIV;
    for (i = 0; i < loop; i ++)
    {
        SMS4_CBC_DecryptionEx(pInData + i * 16, rk, pIVTemp, pOutData + i * 16);
        pIVTemp = pInData + i * 16;
    }
    *pOutDataLen = inDataLen;
    return 0;
}
