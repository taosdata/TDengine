/*! 
*  @file ss_error.h
*  @brief     SS对外声明的类似定义。
*  
*/
#ifndef __SS_DEFINE_H__
#define __SS_DEFINE_H__

typedef unsigned long long  SS_UINT64;
typedef unsigned int    SS_UINT32;
typedef unsigned short  SS_UINT16;
typedef unsigned char   SS_UINT8;

typedef long long       SS_INT64;
typedef int             SS_INT32;
typedef short           SS_INT16;
typedef char            SS_INT8;

typedef unsigned char   SS_BYTE;
typedef char            SS_CHAR;
typedef int             SS_BOOL;


#ifndef   IN
#define   IN
#endif

#ifndef   OUT
#define   OUT
#endif

#ifndef  INOUT
#define  INOUT
#endif

#ifndef SSAPI
  #if defined WIN32 || defined _WIN32 || defined _WIN64
    #define SSAPI __stdcall
  #else
    #define SSAPI
  #endif
#endif

/** 加解密最大缓冲区最大长度（字节）*/
#define SLM_MAX_USER_CRYPT_SIZE     1520

/** 用户GUID最大长度（字符串） */
#define SLM_CLOUD_MAX_USER_GUID_SIZE 	        128	

/** 用户数据区最大长度（字节）*/
#define SLM_MAX_USER_DATA_SIZE      2048

/** 用户数据区写入最大长度（字节）*/
#define SLM_MAX_WRITE_SIZE          1904

/** 请求硬件锁设备私钥签名的数据大小，见slm_sign_by_device*/
#define SLM_VERIFY_DATA_SIZE        41

/** 请求硬件锁设备私钥签名的数据前缀，见slm_sign_by_device*/
#define SLM_VERIFY_DEVICE_PREFIX    "SENSELOCK"

/** 参数格式枚举  */
typedef enum _INFO_FORMAT_TYPE {

    /** JSON格式  */
    JSON = 2,
    /** 结构体格式  */
    STRUCT = 3,
    /**  字符串模式,遵行Key=value  */
    STRING_KV = 4,
    /** 加密二进制格式*/
    CIPHER = 5,
} INFO_FORMAT_TYPE;

/** 设备证书类型*/
typedef enum _CERT_TYPE{
    /** 证书类型：根证书  */
    CERT_TYPE_ROOT_CA = 0,

    /** 证书类型：设备子CA  */
    CERT_TYPE_DEVICE_CA = 1,
    
    /** 证书类型：设备证书  */
    CERT_TYPE_DEVICE_CERT = 2,

    /** 证书类型：深思设备证书  */
    CERT_TYPE_SENSE_DEVICE_CERT = 3,

} CERT_TYPE;

/** 硬件锁闪灯控制结构*/
typedef struct _ST_LED_CONTROL {
	/**  0表示蓝色LED，1表示红色LED，参考宏：LEX_COLOR_XXX  */
	SS_UINT32   index;
	/**  0代表关闭，1代表打开， 2代表闪烁，参考宏：LED_STATE_XXX */
	SS_UINT32   state;
	/**  LED灯闪烁时间间隔（毫秒）*/
	SS_UINT32   interval;   
} ST_LED_CONTROL;

#endif //__SS_DEFINE_H__
