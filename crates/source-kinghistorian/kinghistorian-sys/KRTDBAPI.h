//==============================================================================
//
// 项目名 ：工业实时数据库
// 文件名 ：KRTDBAPI.h
// 作  者 ：DLJ(jack)
// 用  途 ：工业实时数据库API开发接口定义
// 
//==============================================================================
// 版本记录	
//==============================================================================
//
// V0.9	- DLJ 2004/09/30 初始发布版本。
// V1.0 - DLJ 2005/02/20 简化接口，移除不太重要和暂时无法确定的接口，后续再修订。			
// V1.1 - DLJ 2005/04/22 更改了性能监视相关接口。
// V1.2 - DLJ 2005/05/09 修改了回调接口及存储管理、安全部分的结构和接口。
// V1.3	- DLJ 2005/10/26 修改少量接口参数，删除少量无用的接口函数。
// V1.4	- DLJ 2006/06/08 调整了变量组相关函数，增加了用户/角色回调；修改了文件管理相关接口。
// V1.5 - DLJ 2006/07/13 修改了SQL相关接口的定义及实现。
// V1.6 - DLJ 2006/07/19 增加了变量数据和变量配置订阅的扩展版本。
// V1.7 - DLJ 2006/07/28 增加了数据回写接口及相应采集器回调函数。
// V1.8 - DLJ 2006/08/08 增加了枚举服务器端文件路径的函数。
// V1.9 - DLJ 2006/09/05 增加了测试计算脚本的函数和相应的回调函数，以及批量修改变量配置函数。
// V2.0 - DLJ 2006/09/07 增加了从采集器层次化浏览变量的接口及相应的回调函数。
// V3.0 - DLJ 2007/11/06 修改变量属性、采集器属性，增加Digital数据类型支持，变量数据查询优化。
//
//==============================================================================

//==============================================================================
#ifndef __KRTDBAPI__H__INCLUDED__
#define __KRTDBAPI__H__INCLUDED__
//==============================================================================
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <tchar.h>
#include <wtypes.h>

//==============================================================================
#pragma pack( push , BEFOREKRTDBAPI )
#pragma pack( 1 )
//==============================================================================
#ifdef  __cplusplus
extern "C" {
#endif
//==============================================================================




//==============================================================================
// 
// 宏定义
// 
//==============================================================================
#define KDBAPI			_stdcall
#define KWSTR(str)		L##str
#define KOK(err)		( ((KDB_INT32)(err)) ==  0 )
#define KER(err)		( ((KDB_INT32)(err)) !=  0 )


//==============================================================================
// 
// 数据类型
// 
//==============================================================================
typedef char						KDB_CHAR;
typedef wchar_t						KDB_WCHAR;
typedef unsigned char				KDB_BYTE;
typedef unsigned char				KDB_OCTET;
typedef unsigned char				KDB_BOOLEAN;
typedef float						KDB_FLOAT32;
typedef double						KDB_FLOAT64;
typedef signed char					KDB_INT8;
typedef unsigned char				KDB_UINT8;
typedef short						KDB_INT16;
typedef unsigned short				KDB_UINT16;
typedef long						KDB_INT32;
typedef unsigned long				KDB_UINT32;
typedef __int64						KDB_INT64;
typedef unsigned __int64			KDB_UINT64;
typedef void						KDB_VOID;
typedef void*						KDB_PTR;
typedef KDB_BYTE*					KDB_BINARY;
typedef KDB_CHAR*					KDB_STR;
typedef const KDB_CHAR*				KDB_CSTR;
typedef KDB_WCHAR*					KDB_WSTR;
typedef const KDB_WCHAR*			KDB_CWSTR;
typedef KDB_STR*					KDB_STR_ARRAY;
typedef KDB_WSTR*					KDB_WSTR_ARRAY;
typedef long						KDB_RET;					/// API函数调用返回码
typedef KDB_PTR						KDB_HANDLE;					/// 连接句柄
typedef KDB_PTR						KDB_RESULT;					/// 查询结果
typedef KDB_PTR						KDB_MULTIPLE_RESULT;		/// 多结果集
typedef struct KDBValue*			KDB_VARIANT;				/// 可变数据类型
typedef KDB_PTR						KDB_DATA_RECORDSET_HANDLE;	/// 变量数据结果集句柄
typedef KDB_PTR						KDB_TAG_RECORDSET_HANDLE;	/// 变量属性结果集句柄



//==============================================================================
// 
// 公共常量定义
// 
//==============================================================================

/// <summary> 
/// 用于订阅新增及所有数据项的特殊标志名。
/// </summary> 
/// <seealso cref="KDBTagSubscribeProperties"/>
/// <seealso cref="KDBCollectorSubscribeProperties"/>
#define KDB_NEW_ITEM	KWSTR( "$NewItem" )
#define KDB_ALL_ITEM	KWSTR( "$AllItem" )
#define KDB_NEW_GROUP	0

/// <summary> 
///	标准的布尔常量
/// </summary> 
enum KDBBoolean
{
	KDB_FALSE = 0,		/// 布尔假值 
	KDB_TRUE  = 1,		/// 布尔真值
};


/// <summary> 
/// 错误代码
/// </summary> 
typedef enum KDBErrorCode
{
	KERR_OK						=  0 ,		/// 没有错误
	KERR_FAIL					= -1 ,		/// 一般性错误
	KERR_TIMEOUT				= -2 ,		/// 超时错误
	KERR_OUT_OF_MEMORY			= -3 ,		/// 内存错误
	KERR_NOT_CONNECTED			= -4 ,		/// 没有或无法建立连接
	KERR_ACCESS_DENIED			= -5 ,		/// 访问被拒绝
	KERR_NOT_SUPPORTED			= -6 ,		/// 不支持的属性或方法
	KERR_INVALID_USER			= -7 ,		/// 无效用户或密码
	KERR_INVALID_TAGNAME		= -8 ,		/// 无效变量名
	KERR_INVALID_ARG			= -9,		/// 无效参数
	KERR_NO_DATA				= -10,		/// 数据不存在
	KERR_LIC_INVALID			= -11,		/// 无效授权
	KERR_LIC_TOO_MANY_TAGS		= -12,		/// 超过变量授权计数
	KERR_LIC_TOO_MANY_USERS		= -13,		/// 超过用户授权计数
	KERR_INVALID_COMMAND		= -14,		/// 无效命令
	KERR_INVALID_DATA			= -15,		/// 无效数据
	KERR_NETWORK_ERROR			= -16,		/// 网络错误
	KERR_SYSTEM_ERROR			= -17,		/// 系统错误
	KERR_NOT_FOUND 				= -18,		/// 无法找到对象
	KERR_SERVICE_NOT_RUNNING	= -21,		/// 服务不在运行状态
	KERR_IO_PENDING	 			= -22,		/// IO未完成
	KERR_BUFFER_TOO_SMALL		= -23,		/// 缓冲区太小
	KERR_NO_MORE_DATA 			= -24,		/// 没有更多的数据
	KERR_DEADLOCK				= -25,		/// 发生了死锁
	KERR_OUT_OF_SPACE 			= -26,		/// 磁盘空间不足
	KERR_ALREADY_EXIST 			= -27,		/// 对象已经存在
	KERR_DUPLICATE	 			= -28,		/// 发现重复项
	KERR_OVERFLOW		 		= -30,		/// 发生了溢出
	KERR_ABORTED		 		= -31,		/// 已经退出
	KERR_RECORD_TOO_LONG		= -32,		/// 记录太长
	KERR_KEY_TOO_LONG			= -33,		///键值太长
	KERR_LOAD_RESOURCE_FAIL		= -34,		/// 装载资源失败
	KERR_SERVICE_STATUS_ERROR	= -35,		/// 服务状态错误
	KERR_TOO_MANY_DATA_VERSIONS	= -36,		/// 超过允许的数据版本数
	KERR_SQL_SYNTAX_ERROR		= -37,		/// SQL语法错误
	KERR_DATA_OUT_OF_TIME_RANGE	= -39,		/// 数据超出时间范围
	KERR_DATA_TYPE_ERROR		= -40,		/// 数据类型错误
	KERR_CALLER_CANCEL			= -41,		/// 调用方取消
}KDB_ERROR_CODE;

/// <summary> 
/// 连接控制选项
/// </summary> 
typedef enum KDBConnectionFlags
{
	// 网络传输协议
	KCOF_PROTOCOL_AUTO			= 0x00000001,	/// 自动选择通讯协议（保留）
	KCOF_PROTOCOL_TCPIP			= 0x00000002,	/// 使用TCP/IP协议（默认）
	KCOF_PROTOCOL_SSL			= 0x00000004,	/// 使用SSL加密传输协议（保留）
	KCOF_PROTOCOL_NAMED_PIPE	= 0x00000008,	/// 命名管道（保留）
	KCOF_PROTOCOL_SHARED_MEMORY = 0x00000010,	/// 共享内存，只适合本机的进程间通讯（保留）
	KCOF_IS_COLLECTOR			= 0x00000100,	/// 是数据采集器
	KCOF_CACHE_MODE_DEFAULT		= 0x00000000,	/// 默认模式（使用应用程序配置或全局配置）
	KCOF_CACHE_MODE_DISABLE		= 0x10000000,	/// 禁用客户端缓存
	KCOF_CACHE_MODE_DATAONLY	= 0x20000000,	/// 仅查询变量数据使用缓存
	KCOF_CACHE_MODE_GATEWAY		= 0x30000000,	/// 带缓存的代理网关
	KCOF_CACHE_MODE_MASK		= 0x30000000,	/// 缓存模式掩码
	KCOF_NETWORK_GAP_NONE		= 0x00000000,	/// 正常非网闸模式
	KCOF_NETWORK_GAP_1BIT		= 0x40000000,	/// 1BIT模式的单向网闸
	KCOF_NETWORK_GAP_1BYTE		= 0x80000000,	/// 1BYTE模式的单向网闸(7B
	KCOF_NETWORK_GAP_4BYTE		= 0xC0000000,	/// 4BYTE模式的单向网闸
	KCOF_NETWORK_GAP_MASK		= 0xC0000000,	/// 网闸模式掩码
} KDB_CONNECTION_FLAGS;


//==============================================================================
// 
// 历史数据相关常量定义
// 
//==============================================================================

/// <summary> 
/// 时区设置
/// </summary> 
typedef enum KDBTimeZone		
{
	KTMZ_CIENT		= 0,	/// 客户端时区
	KTMZ_SERVER		= 1,	/// 服务器端时区
	KTMZ_EXPLICIT	= 2,	/// 指定时区(与UTC的偏差)
} KDB_TIMEZONE;

/// <summary> 
/// 压缩方式选择。
/// </summary> 
typedef enum KDBCompressionMode
{
	KCPM_CHANGE				= 0,	/// 只进行压缩超时及相同值检测(变化压缩)
	KCPM_SWINGINGDOOR		= 1,	/// 旋转门
	KCPM_DEADBANDING		= 2,	/// 死区压缩
	KCPM_DEADBANDING_SPIKE	= 3,	/// 死区压缩（带尖峰逻辑处理）
	
}KDB_COMPRESSION_MODE;

/// <summary> 
/// 数据类型
/// </summary> 
/// <seealso cref="KDBHistoryDataType"/> 
/// <seealso cref="KDBSqlDataType"/> 
typedef enum KDBValueDataType
 {
	KVDT_EMPTY		= 0 ,		/// 没有指定数据类型
	KVDT_BOOL		= 1 ,		/// 布尔数据类型
	KVDT_I1			= 2 ,		/// 8位整数(有符号)
	KVDT_UI1		= 3	,		/// 8位整数(无符号)
	KVDT_I2			= 4	,		/// 16位整数(有符号)
	KVDT_UI2		= 5 ,		/// 16位整数(无符号)
	KVDT_I4			= 6	,		/// 32位整数(有符号)
	KVDT_UI4		= 7	,		///	32位整数(无符号)
	KVDT_I8			= 8	,		/// 64位整数(有符号)
	KVDT_UI8		= 9	,		/// 64位整数(无符号)
	KVDT_R4			= 10,		/// 32位浮点数
	KVDT_R8			= 11,		/// 64位浮点数
	KVDT_STR		= 12,		/// ANSI字符串
	KVDT_WSTR		= 13,		/// Unicode字符串
	KVDT_TIMESTAMP	= 14,		/// 时间戳数据类型
	KVDT_BLOB		= 15,		/// 二进制数据对象
	KVDT_VARIANT	= 16,		/// 不定数据类型
	KVDT_FILETIME	= 17,		/// 文件时间结构类型
	KVDT_DECIMAL	= 18,		/// 精确的数值类型(保留)
} KDB_VALUE_DATA_TYPE;

/// <summary> 
/// SQL中用到的数据类型
/// </summary> 
/// <seealso cref="KDBValueDataType"/> 
/// <seealso cref="KDBHistoryDataType"/> 
typedef enum KDBSqlDataType
{
	KSDT_EMPTY		= 0,	/// 未知类型
	KSDT_BOOLEAN	= 1,	/// BIT数据类型
	KSDT_TINYINT	= 2,	/// TinyInt数据类型
	KSDT_SMALLINT	= 3,	/// SmallInt数据类型
	KSDT_INT		= 4,	/// INTEGER数据类型
	KSDT_BIGINT		= 5,	/// BigInt数据类型
	KSDT_REAL		= 6,	/// Real数据类型
	KSDT_FLOAT		= 7,	/// Float数据类型
	KSDT_DOUBLE		= 7,	/// Double Precision数据类型
	KSDT_VARIANT	= 8,	/// 可变数据类型
	KSDT_DECIMAL	= 9,	/// 精确的数值类型
	KSDT_NUMERIC	= 9,	/// 精确的数值类型
	KSDT_CHAR		= 10,	/// 固定长度字符串
	KSDT_VARCHAR	= 11,	/// 变长字符串(<8000)
	KSDT_CLOB		= 12,	/// 超长字符串
	KSDT_TEXT		= 12,	/// 文本数据类型
	KSDT_NCHAR		= 13,	/// 定长Unicode字符串
	KSDT_NVARCHAR	= 14,	/// 变长Unicode字符串
	KSDT_NCLOB		= 15,	/// 超长Unicode字符串
	KSDT_NTEXT		= 15,	/// 超长Unicode文本数据类型
	KSDT_DATE		= 16,	/// 日期类型
	KSDT_TIME		= 17,	/// 时间类型
	KSDT_DATETIME	= 18,	/// 日期时间类型
	KSDT_TIMESTAMP	= 18,	/// 日期时间类型
	KSDT_BINARY		= 19,	/// 定长二进制数据类型
	KSDT_VARBINARY	= 20,	/// 不定长的二进制数据类型
	KSDT_BLOB		= 21,	/// 超长的二进制数据类型
	KSDT_IMAGE		= 21,	/// 超长的二进制数据类型
} KDB_SQL_DATA_TYPE;

/// <summary> 
/// 历史数据存储所支持的数据类型
/// </summary> 
/// <seealso cref="KDBSqlDataType"/> 
/// <seealso cref="KDBValueDataType"/> 
typedef enum KDBHistoryDataType	
{
	KHDT_EMPTY			= KSDT_EMPTY,		/// 未知数据类型
	KHDT_BOOLEAN		= KSDT_BOOLEAN,		/// 开关量（布尔类型） 
	KHDT_INT8			= KSDT_TINYINT,		/// 单字节整数(有符号)
	KHDT_INT16			= KSDT_SMALLINT,	/// 双字节整数(有符号)
	KHDT_INT32			= KSDT_INT,			/// 四字节整数(有符号)
	KHDT_INT64			= KSDT_BIGINT,		/// 八字节整数(有符号)
	KHDT_FLOAT32		= KSDT_REAL,		/// 单精度浮点数
	KHDT_FLOAT64		= KSDT_DOUBLE,		/// 双精度浮点数
	KHDT_CHAR			= KSDT_CHAR,		/// 固定长度字符串
	KHDT_VARCHAR		= KSDT_VARCHAR,		/// 变长字符串
	KHDT_NCHAR			= KSDT_NCHAR,		/// 固定长度Unicode字符串
	KHDT_NVARCHAR		= KSDT_NVARCHAR,	/// 变长Unicode字符串
	KHDT_TIMESTAMP		= KSDT_TIMESTAMP,	/// 时间戳(精确到毫秒)
	KHDT_BINARY			= KSDT_BINARY,		/// 定长二进制数据
	KHDT_VARBINARY		= KSDT_VARBINARY,	/// 变长二进制数据
	KHDT_DECIMAL		= KSDT_DECIMAL,		/// 精确小数类型（暂不支持）
	KHDT_DIGITAL		= 101,				/// 数字状态量
	KHDT_FLOAT16		= 102,				/// ScaledFloat16（暂不支持）
} KDB_HISTORY_DATA_TYPE;


/// <summary> 
/// 支持的导入/导出文件类型(文件数据采集器)
/// </summary> 
typedef enum KDBFileFormat		
{
	KFFM_UNKNOWN	= 0,		/// 未知的文件类型
	KFFM_CSV		= 1,		/// CSV文件
	KFFM_XML		= 2,		/// XML文件
	KFFM_XSL		= 3,		/// Excel文件
	KFFM_REPORT		= 4,			/// Report文件
} KDB_FILE_FORMAT;

/// <summary> 
/// 数据采集方式
/// </summary> 
typedef enum KDBCollectionMode	
{
	KCLM_UNKNOWN		= 0,	/// 未知或不支持的数据采集方式
	KCLM_UNSOLICITED	= 1,	/// 主动提供（主动上报）
	KCLM_POLLED			= 2,	/// 周期性获取
} KDB_COLLECTION_MODE;

/// <summary> 
/// 采样方式
/// </summary> 
typedef enum KDBSamplingMode	
{
	KSAM_UNKNOWN		= 0,	/// 未知的采样方式
	KSAM_CURRENT_VALUE	= 1,	/// 当前值(实时值，时间戳最新的变量值，数据质量不论)
	KSAM_INTERPOLATED	= 2,	/// 线性插值采样(根据时间区间、采样点数或采样周期等时间间隔线性插值)
	KSAM_RAW_BY_TIME	= 3,	/// 根据时间区间获取原始值(时间区间为前闭后开)
	KSAM_RAW_BY_NUMBER	= 4,	///	根据指定的起始或终止时间或时间范围，最多获取指定数量的原始值
	KSAM_CALCULATED		= 5,	/// 统计计算(必须指定CalculationMode)
	KSAM_STEPPED		= 6,	/// 步进插值（非线性插值，取前一值，类似于组态王6.5）
	KSAM_TREND			= 7,	/// 趋势采样(指定时间区间、采样点数或采样周期返回每个采样间隔的最大和
								/// 最小原始值及区间的起始和终止值，用于画趋势曲线)
}KDB_SAMPLING_MODE;

/// <summary> 
/// 支持的统计计算(采样模式= KSAM_CALCULATED时)
/// </summary> 
typedef enum KDBCalculationMode	
{
	/// MAXIMUM_TIME和MINIMUM_TIME的返回值为时间戳
	/// 其他均为数值类型
	KCCM_UNKNOWN			= 0,	/// 未知的计算方式
	KCCM_COUNT				= 1,	/// 计数(每个采样区间的原始值的个数，数据质量不论)
	KCCM_AVERAGE			= 2,	/// 时间加权的原始平均值
	KCCM_TOTAL				= 3,	/// 时间加权的原始值求和
	KCCM_STDEV				= 4,	/// 时间加权的标准方差
	KCCM_RAW_TOTAL			= 5,	/// 求和(每个采样区间的原始值的求和)
	KCCM_RAW_AVERAGE		= 6,	/// 平均值(每个采样区间的原始值的算术平均值)
	KCCM_RAW_STDEV			= 7,	/// 标准方差(原始值的算术标准方差)
	KCCM_MINIMUM			= 8,	/// 最小值(每个采样区间内的最小值，可能是原始值，也可能是插值，但数据质量必须是好的)
	KCCM_MAXIMUM			= 9,	/// 最大值(每个采样区间内的最大值，可能是原始值，也可能是插值，但数据质量必须是好的)
	KCCM_MINIMUM_TIME		= 10,	/// 最小值对应的时间
	KCCM_MAXIMUM_TIME		= 11,	/// 最大值对应的时间
	KCCM_DURATION_GOOD		= 12,	/// 好数据的时间总长(ms)
	KCCM_DURATION_BAD		= 13,	/// 坏数据的时间总长(ms)
	KCCM_MAXIMUM_ACTUAL_TIME= 14,	/// 最大值（取数据实际时间）
	KCCM_MINIMUM_ACTUAL_TIME= 15,	/// 最小值（取数据时间时间）
	KCCM_START				= 16,	/// 区间内的第一个原始数据（质量戳可能是好的，也可能是坏的）
	KCCM_END				= 17,	/// 区间内的最后一个原始数据（质量戳可能是好的，也可能是坏的）
	KCCM_DELTA				= 18,	/// 区间内的第一个和最后一个好的原始数据之差
	KCCM_RANGE				= 19,	/// 区间内最大/最小值的绝对差值（只计算好数据） 
	KCCM_PERCENT_GOOD		= 20,	/// 好数据总长/区间长度
	KCCM_PERCENT_BAD		= 21,	/// 坏数据总长/区间长度
} KDB_CALCULATION_MODE;

/// <summary> 
/// 支持的过滤模式
/// </summary> 
typedef enum KDBFilterMode		
{
	KFLM_UNKNOWN				= 0,	/// 未知的方式
	KFLM_EXACT_TIME				= 1,	/// 过滤条件为真的确切时间点
	KFLM_BEFORE_TIME			= 2,	/// 从过滤条件为假一直到过滤条件为真的时间段
	KFLM_AFTER_TIME				= 3,	/// 从过滤条件为真到下一次过滤条件为假之间的时间段
	KFLM_BEFORE_AND_AFTER_TIME	= 4,	/// 从过滤条件为假到下次过滤条件为假的时间段
} KDB_FILTER_MODE;

/// <summary> 
/// 过滤比较操作
/// </summary> 
typedef enum KDBFilterComparisonMode	
{
	KFCM_UNKNOWN		= 0,	/// 未知的操作
	KFCM_EQUAL			= 1,	/// 相等		(FilterTag = FilterValue时条件为真)
	KFCM_NOT_EQUAL		= 2,	/// 不相等		(FilterTag <> FilterValue时条件为真)
	KFCM_LESS			= 3,	/// 小于		(FilterTag <  FilterValue时条件为真)
	KFCM_GREATER		= 4,	/// 大于		(FilterTag  > FilterValue时条件为真)
	KFCM_LESS_EQUAL		= 5,	/// 小于或者等于(FilterTag <= FilterValue时条件为真)
	KFCM_GREATER_EQUAL	= 6,	/// 大于或者等于(FilterTag >= FilterValue时条件为真)
}KDB_FILTER_COMPARISON_MODE;


/// <summary> 
/// 数据版本
/// </summary> 
typedef enum KDBDataVersion
{
	KDAV_ORIGINAL		=  0,	/// 原始版本
	KDAV_MODIFIED		= -3,	/// 修改版本
    KDAV_LATEST			= -2,	/// 最近的版本(可能是原始版本，也可能是新版本)
	KDAV_ALL			= -1,	/// 全部版本
} KDB_DATA_VERSION;

/// <summary>
/// 数据质量模式
/// </summary>
typedef enum KDBDataQualityMode
{
	KDQM_ALL			= 0,	/// 使用全部质量戳的数据
	KDQM_GOOD			= 1,	/// 只使用好质量戳的数据
} KDB_DATA_QUALITY_MODE;

/// <summary> 
/// 数据质量
/// </summary> 
typedef enum KDBDataQuality
{
	// QQSSSSLL ( OPC 3.0:低八位由质量Q、子状态S和限制状态L构成)
	KDAQ_OPC_QUALITY_MASK			= 0xC0,		/// Quality BITMASK
	KDAQ_OPC_STATUS_MASK			= 0xFC,		/// Quality & Substatus mask
	KDAQ_OPC_LIMIT_MASK				= 0x03,		/// Limit BITMask

	// Quality(低字节的高两位：第6位和第7位 BITMASK )
	KDAQ_OPC_BAD					= 0x00,		/// 坏数据
	KDAQ_OPC_UNCERTAIN				= 0x40,		/// 不可靠数据
	KDAQ_OPC_NA						= 0x80,		/// N/A
	KDAQ_OPC_GOOD					= 0xC0,		/// 好的数据

	// Substatus(低字节的中间四位：第2位－第5位)
	// Substatus for BAD Quality
	KDAQ_OPC_CONFIG_ERROR				= 0x04,		/// 配置错误
	KDAQ_OPC_NOT_CONNECTED				= 0x08,		/// 没有连接设备
	KDAQ_OPC_DEVICE_FAILURE				= 0x0c,		/// 设备失败
	KDAQ_OPC_SENSOR_FAILURE				= 0x10,		/// 传感器失败(limit域能够提供附加信息)
	KDAQ_OPC_LAST_KNOWN					= 0x14,		/// 上一次采集的值(通讯失败)
	KDAQ_OPC_COMM_FAILURE				= 0x18,		/// 通讯失败(且无上一次采集值可用)
	KDAQ_OPC_OUT_OF_SERVICE				= 0x1C,		/// 设备停机
	KDAQ_OPC_WAITING_FOR_INITIAL_DATA	= 0x20,		/// 尚未取得设备数据

	// Substatus for UNCERTAIN Quality
	KDAQ_OPC_LAST_USABLE			= 0x44,		/// 上一个可用值
	KDAQ_OPC_SENSOR_CAL				= 0x50,		/// 传感器值不精确
	KDAQ_OPC_EGU_EXCEEDED			= 0x54,		/// 超量程
	KDAQ_OPC_SUB_NORMAL				= 0x58,		/// 值从多个数据源得到，但缺少足够多的好数据

	// Substatus for GOOD Quality
	KDAQ_OPC_LOCAL_OVERRIDE			= 0xD8,		/// 值被覆盖(GOOD)
	
	// Limit(低字节的低两位：第0位、第1位)
	KDAQ_OPC_LIMIT_OK				= 0x00,		/// 上下限OK
	KDAQ_OPC_LIMIT_LOW				= 0x01,		/// 下限
	KDAQ_OPC_LIMIT_HIGH				= 0x02,		/// 上限
	KDAQ_OPC_LIMIT_CONST			= 0x03,		/// 常量
	
	// OPC HDA Quality( OPC HDA V1.2，高16位 )
	KDAQ_OPCHDA_EXTRADATA		= 0x00010000,	/// 同一时间戳可能存在多个数据 (Good/Bad/Uncertain)
	KDAQ_OPCHDA_INTERPOLATED	= 0x00020000,	/// 插值数据 (Good/Bad/Uncertain)
	KDAQ_OPCHDA_RAW				= 0x00040000,	/// 原始数据 (Good/Bad/Uncertain)
	KDAQ_OPCHDA_CALCULATED		= 0x00080000,	/// 计算值	(Good/Bad/Uncertain)	
	KDAQ_OPCHDA_NOBOUND			= 0x00100000,	/// 没有上界或下界(时间超出范围)	(Bad)
	KDAQ_OPCHDA_NODATA			= 0x00200000,	/// 没有数据						(Bad)
	KDAQ_OPCHDA_DATALOST		= 0x00400000,	/// 数据丢失(采集停止 off-line )	(Bad)
	KDAQ_OPCHDA_CONVERSION		= 0x00800000,	/// 标定或转换错误				(Bad/Uncertain)
	KDAQ_OPCHDA_PARTIAL			= 0x01000000,	/// 不完整时间区间的统计值		(Good/Bad/Uncertain)

} KDB_DATA_QUALITY;


#define IsDataBad( dataQuality )		(((dataQuality) & KDAQ_OPC_QUALITY_MASK) == KDAQ_OPC_BAD )
#define IsDataGood( dataQuality )		(((dataQuality) & KDAQ_OPC_QUALITY_MASK) == KDAQ_OPC_GOOD )
#define IsDataUncertain( dataQuality )	(((dataQuality) & KDAQ_OPC_QUALITY_MASK) == KDAQ_OPC_UNCERTAIN )
#define GetQualityLimit( dataQuality )	((dataQuality)  & KDAQ_OPC_LIMIT_MASK  )
#define GetQualityStatus(dataQuality)	((dataQuality)  & KDAQ_OPC_STATUS_MASK )

/// <summary> 
/// 时间戳方式
/// </summary> 
typedef enum KDBTimestampType
{
	KTST_SOURCE		= 0x01 ,					/// 时间戳由数据源提供
	KTST_COLLECTOR	= 0x02 ,					/// 时间戳由数据采集器提供
} KDB_TIMESTAMP_TYPE;

/// <summary> 
/// 支持的数据采集器
/// </summary> 
typedef enum  KDBCollectorType	
{
	KCLT_UNKNOWN			= 0,	/// 未知的数据采集器
	KCLT_CALCULATION_ENGINE	= 1,	/// 计算引擎
	KCLT_OPC				= 2,	/// OPC数据采集器
	KCLT_ALARM_SERVER		= 3,	/// 报警服务器
	KCLT_KINGVIEW			= 4,	/// 组态王数据采集器(从组态王运行系统采集数据)
	KCLT_FILE				= 5,	/// 文件采集器
	KCLT_SERVER_TO_SERVER	= 6,	/// 服务器到服务器数据采集器	
	KCLT_KINGVIEW_LAB		= 7,	/// 组态王历史数据采集器(从组态王的历史数据库文件中导入数据)
	KCLT_SIMULATION			= 8,	/// 模拟采集器
	KCLT_MANUAL				= 9,	/// 手工输入
	KCLT_OTHER				= 10,	/// 其他数据采集器
	KCLT_PI_COLLECTOR		= 11,	/// PI采集器
	KCLT_PI_DISTRIBUTOR		= 12,	/// PI分发器
	KCLT_KINGIOSERVER       = 13,   /// KingIOServer采集器
	KCLT_IFIX_COLLECTOR		= 14,   /// IFIX采集器
} KDB_COLLECTOR_TYPE;


/// <summary> 
/// 控制采集器动作
/// </summary> 
typedef enum KDBCollectorControl	
{
	KCLC_START				= 1,	/// 启动采集(注意：不是启动服务)
	KCLC_STOP				= 2,	/// 停止采集(注意：不是停止服务）
	KCLC_FAILOVER_ACTIVATE	= 3,	/// 激活冗余采集器(开始向服务器发送数据)
	KCLC_FAILOVER_DEACTIVATE= 4,	/// 取消激活冗余采集器(停止向服务器发送数据)
	KCLC_RESTART_SERVICE	= 5,	/// 重启采集器服务程序
	KCLC_STOP_SERVICE		= 6,	/// 停止采集器服务程序
	KCLC_RELOAD_SCRIPTLIB	= 7,	/// 重新装载脚本库（只对计算引擎有效）
} KDB_COLLECTOR_CONTROL;

/// <summary> 
/// 采集器状态
/// </summary> 
typedef enum KDBCollectorStatus		
{
	KCLS_UNKNOWN			= 0,	/// 未知的状态
	KCLS_STARTING			= 1,	/// 正在启动采集
	KCLS_RUNNING			= 2,	/// 正在运行
	KCLS_STOPPING			= 3,	/// 正在停止采集
	KCLS_STOPPED			= 4,	/// 已经停止采集
	KCLS_SERVICE_STARTING	= 5,	/// 服务正在启动
    KCLS_SERVICE_STOPPED	= 6,	/// 服务已经停止
	KCLS_SOURCE_STARTING	= 7,	/// 正在启动/连接数据源
	KCLS_SOURCE_STOPPED		= 8,	/// 数据源已经停止或连接中断
	KCLS_FAILOVER_STANDBY	= 9,	/// 冗余待机
} KDB_COLLECTOR_STATUS;

//==============================================================================
// 
// 变量相关常量
// 
//==============================================================================

/// <summary> 
/// 输入转换
/// </summary> 
typedef enum KDBInputConversion
{
	KICV_NO_CONVERSION	= 0,		/// 不转换
	KICV_LINEAR			= 1,		/// 线性转换
	KICV_SQRT			= 2,		/// 开方转换

} KDB_INPUT_CONVERSION;

/// <summary> 
/// 属性更改类型。
/// </summary> 
typedef enum KDBItemChangeType
{
	KICT_NO_CHANGE	= 0,			/// 没有改变
	KICT_ADDED		= 1,			/// 增加
	KICT_DELETED	= 2,			/// 删除
	KICT_MODIFIED	= 3,			/// 修改
} KDB_ITEM_CHANGE_TYPE;

//==============================================================================
//
// 计算相关定义
//
//==============================================================================

/// <summary>
/// 变量类型
/// </summary>
typedef enum KDBTagType
{
	KTTP_HISTORY		= 0,		/// 历史变量
	KTTP_CALCULATION	= 1,		/// 计算变量
} KDB_TAG_TYPE;

/// <summary>
/// 计算函数（没有特别说明，以下函数的计算值的时间戳均置为计算区间的起始时间）
/// </summary>
#define KDB_CALCFUNCTION_MAX		KWSTR("$Max")		/// 最大值
#define KDB_CALCFUNCTION_MIN		KWSTR("$Min")		/// 最小值
#define KDB_CALCFUNCTION_AVG		KWSTR("$Avg")		/// 平均值
#define KDB_CALCFUNCTION_SUM		KWSTR("$Sum")		/// 累加值
#define KDB_CALCFUNCTION_COUNT		KWSTR("$Count")		/// 数据个数（不论数据质量）
#define KDB_CALCFUNCTION_TREND		KWSTR("$Trend")		/// 趋势值（提取采样区间内的最大最小值数据 、起始数据和最后数据，时间戳为原始数据的时间戳）
#define KDB_CALCFUNCTION_PERIOD		KWSTR("$Period")	/// 整点值（该时刻没有数据，则提取前一个时刻的数据）
#define KDB_CALCFUNCTION_CTOF		KWSTR("$CtoF")		/// 变为FALSE的次数
#define KDB_CALCFUNCTION_CTOT		KWSTR("$CtoT")		/// 变为TRUE的次数

//==============================================================================
// 
// 公共结构定义
// 
//==============================================================================

/// <summary> 
/// 连接参数
/// </summary> 
typedef struct KDBConnectionOption
{
	KDB_WSTR		ServerName;			/// 服务器计算机名或地址
	KDB_WSTR		ServerPort;			/// 服务器端口或其他协议的附加地址信息
	KDB_WSTR		UserName;			/// 用户名
	KDB_WSTR		Password;			/// 密码
	KDB_WSTR		ApplicationName;	/// 客户端程序名 
	KDB_WSTR		ClientName;			/// 客户端机器名
	KDB_WSTR		CollectorName;		/// 采集器名称（只对采集器有效）
	KDB_UINT32		NetworkTimeout;		/// 网络超时（毫秒）
	KDB_UINT32		ConnectionFlags;	/// 协议、安全等选项
	KDB_UINT32		Reserved1;			/// 保留字段
	KDB_UINT32		Reserved2;			/// 保留字段
	KDB_WSTR		SessionId;			/// 用于永久的唯一标识此连接会话的ID
	KDB_WSTR		Reserved4;			/// 保留字段
}KDB_CONNECTION_OPTION , *PKDB_CONNECTION_OPTION;


/// <summary> 
/// 时间戳
/// </summary> 
typedef struct KDBTimeStamp				
{
	KDB_UINT32			Seconds;		/// 自1970/01/01 00:00:00(UTC)以来的秒数
	KDB_UINT16			Millisecs;		/// 毫秒数
} KDB_TIMESTAMP , *PKDB_TIMESTAMP;

/// <summary> 
/// BLOB数据类型
/// </summary> 
typedef struct KDBBlob					
{
	KDB_UINT32			Len;			/// 数据长度
	KDB_BINARY			Data;			/// 数据
} KDB_BLOB, *PKDB_BLOB;

/// <summary> 
///	可变类型
/// </summary> 
typedef struct KDBValue
{
	KDB_UINT16				DataType;	/// 数据类型(KDB_VALUE_DATA_TYPE)
	union
	{
		KDB_BOOLEAN				bitVal;		/// 布尔类型
		KDB_INT8				i1Val;		/// 单字节整数
		KDB_INT16				i2Val;		/// 双字节整数
		KDB_INT32				i4Val;		/// 四字节整数
		KDB_INT64				i8Val;		/// 八字节整数
		KDB_UINT8				ui1Val;		/// 单字节整数(无符号)	
		KDB_UINT16				ui2Val;		/// 双字节整数(无符号)
		KDB_UINT32				ui4Val;		/// 四字节整数(无符号)
		KDB_UINT64				ui8Val;		/// 八字节整数(无符号)
		KDB_FLOAT32				r4Val;		/// 单精度浮点数
		KDB_FLOAT64				r8Val;		/// 双精度浮点数
		KDB_STR					strVal;		/// ANSI字符串
		KDB_WSTR				wstrVal;	/// Unicode字符串
		KDB_BLOB				blobVal;	/// BLOB
		FILETIME				ftVal;		/// FILETIME类型
		KDB_TIMESTAMP			tsVal;		/// TimeStamp类型
		KDB_VARIANT				varVal;		/// 不定类型
		DECIMAL*				decVal;		/// 精确十进制数类型			
	};//DataValue;							/// 数据值
} KDB_VALUE , *PKDB_VALUE;



/// <summary> 
/// 字符串数组。
/// </summary> 
typedef struct KDBStringArray
{
	KDB_UINT32				SizeOfArray;		/// 数组大小
	KDB_WSTR_ARRAY			StringArray;		/// 字符串数组
}KDB_STRING_ARRAY,*PKDB_STRING_ARRAY;

/// <summary> 
/// 整数数组。
/// </summary>
typedef struct KDBIntArray
{
	KDB_UINT32				SizeOfArray;		/// 数组大小
	KDB_UINT32*				IntArray;			/// 整数数组
}KDB_INT_ARRAY,*PKDB_INT_ARRAY;

//==============================================================================
// 
// 变量数据相关的数据结构
// 
//==============================================================================


/// <summary> 
/// 数据检索条件
/// </summary> 
typedef struct KDBDataCriteria	
{
	KDB_UINT32					NumberOfTags;				/// 变量个数
	KDB_WSTR_ARRAY				TagNames;   				/// 变量名称
	KDB_TIMESTAMP				StartTime;					/// 起始时间(闭)
	KDB_TIMESTAMP				EndTime;					/// 终止时间(开)
	KDB_DATA_VERSION			DataVersion;				/// 数据版本(可以是latest/original/all/newcopy)
	KDB_SAMPLING_MODE			SamplingMode;				/// 采样方式
	KDB_UINT32					SamplingNumber;				/// 采样点数
	KDB_UINT64					SamplingInterval;			/// 采样间隔(毫秒数)
	KDB_CALCULATION_MODE		CalculationMode;			/// 计算方式
	KDB_WSTR					FilterTag;					/// 用作过滤条件的变量名
	KDB_FILTER_MODE				FilterMode;					/// 过滤模式
	KDB_FILTER_COMPARISON_MODE	FilterComparisonMode;		/// 过滤比较模式
	KDB_VALUE					FilterComparisonValue;		/// 过滤比较值
	KDB_UINT32					RowCount;					/// 最多返回的记录条数
	KDB_BOOLEAN					DigitalAsString;			/// 以字符串方式返回数字状态量（否则以整数方式返回）

} KDB_DATA_CRITERIA,*PKDB_DATA_CRITERIA;

/// <summary> 
/// 数据检索条件
/// </summary> 
typedef struct KDBDataCriteria2	
{
	KDB_UINT32					NumberOfTags;				/// 变量个数
	KDB_WSTR_ARRAY				TagNames;   				/// 变量名称
	KDB_TIMESTAMP				StartTime;					/// 起始时间(闭)
	KDB_TIMESTAMP				EndTime;					/// 终止时间(开)
	KDB_DATA_VERSION			DataVersion;				/// 数据版本(可以是latest/original/all/newcopy)
	KDB_SAMPLING_MODE			SamplingMode;				/// 采样方式
	KDB_UINT32					SamplingNumber;				/// 采样点数
	KDB_UINT64					SamplingInterval;			/// 采样间隔(毫秒数)
	KDB_CALCULATION_MODE		CalculationMode;			/// 计算方式
	KDB_WSTR					FilterTag;					/// 用作过滤条件的变量名
	KDB_FILTER_MODE				FilterMode;					/// 过滤模式
	KDB_FILTER_COMPARISON_MODE	FilterComparisonMode;		/// 过滤比较模式
	KDB_VALUE					FilterComparisonValue;		/// 过滤比较值
	KDB_UINT32					RowCount;					/// 最多返回的记录条数
	KDB_BOOLEAN					DigitalAsString;			/// 以字符串方式返回数字状态量（否则以整数方式返回）
	KDB_DATA_QUALITY_MODE		DataQuality;				/// 数据质量模式（默认使用全部数据，也可以只查询好质量戳的数据）
} KDB_DATA_CRITERIA2,*PKDB_DATA_CRITERIA2;

/// <summary> 
/// 数据属性
/// </summary> 
typedef struct KDBDataProperties
{
	KDB_TIMESTAMP				TimeStamp;					/// 时间戳
	KDB_INT16					Version;					/// 数据版本
	KDB_UINT32					Quality;					/// 数据质量
	KDB_VALUE					Value;						/// 数据值
} KDB_DATA_PROPERTIES , *PKDB_DATA_PROPERTIES;


/// <summary> 
/// 变量数据结果集(单个变量)。
/// </summary> 
typedef struct KDBDataRecordset 
{
	KDB_WSTR					TagName;   					/// 变量名	
	KDB_RET						ErrorStatus;				/// 错误码（插入或查询返回时设置）
	KDB_INT16					DigitalSetId;				/// 数字状态量（查询返回时设置）	
	KDB_INT16					DataType;					/// 变量类型（查询返回时设置）	
	KDB_UINT32 					NumberOfRecords;			/// 数据记录条数
	PKDB_DATA_PROPERTIES		DataRecords;  				/// 数据记录数组
} KDB_DATA_RECORDSET , *PKDB_DATA_RECORDSET;

/// <summary> 
/// 变量数据结果集集合（多个变量）。
/// </summary> 
typedef struct KDBDataRecordsets
{
	KDB_UINT32					NumberOfTags;				/// 变量数目
	PKDB_DATA_RECORDSET			DataRecordset;				/// 记录集数组
}KDB_DATA_RECORDSETS,*PKDB_DATA_RECORDSETS;



//==============================================================================
// 
// 变量相关数据结构
// 
//==============================================================================

/// <summary> 
/// 变量检索条件。
/// </summary> 
typedef struct KDBTagCriteria	
{
	KDB_WSTR						TagNameMask;				/// 变量名掩码
	KDB_UINT32						NumberOfTags;				/// 变量数目
	KDB_WSTR_ARRAY					TagNames;					/// 变量名数组
	KDB_WSTR						DescriptionMask;			/// 变量描述掩码
	KDB_WSTR						CollectorName;				/// 数据采集器名称
	KDB_WSTR						SourceAddress;				/// 数据源地址
} KDB_TAG_CRITERIA,*PKDB_TAG_CRITERIA;

/// <summary> 
/// 变量属性
/// </summary> 
typedef struct KDBTagProperties
{
	// 常规属性(General)
	KDB_WSTR						TagName;						/// 变量名称
	KDB_WSTR						EngineeringUnit;				/// 变量工程单位
	KDB_WSTR						Description;					/// 变量描述						
	KDB_INT32						TagId;							/// 变量ID（只读属性）
	KDB_INT16						DigitalSetId;					/// 变量默认的状态集ID（Digital类型的变量支持）

	// 数据采集相关属性(Collection)
	// 数据源
	KDB_WSTR						CollectorName;					/// 数据采集器名称
	KDB_COLLECTOR_TYPE				CollectorType;					/// 数据采集器类型(不显示)				
	KDB_WSTR						SourceAddress;					/// 数据源地址
	KDB_HISTORY_DATA_TYPE			DataType;						/// 变量数据类型
	KDB_INT32						DataLength;						/// 变量数据长度(字符串/BLOB类型，以字节为单位)
	
	// 采集选项
	KDB_BOOLEAN						CollectionControl;				/// 采集控制(是否采集标志)
	KDB_COLLECTION_MODE 			CollectionMode;					/// 采集方式
	KDB_INT32 						CollectionInterval;				/// 采集间隔(采样周期:单位ms)			
	KDB_INT32 						CollectionOffset;				/// 采集偏移(相位:单位ms)
	KDB_TIMESTAMP_TYPE				TimestampType;					/// 时间戳由谁提供
	KDB_INT32						TimeZoneBias;					/// 与UTC标准时间的差值(单位：分钟)(保留扩展用)
	KDB_INT32						TimeAdjustment;					/// 时间校正(单位:毫秒)

	// 数据转换(Scaling/Conversion)
	// 值域(Engineering Unit Range)
	KDB_FLOAT64 					MaxValue;						/// 变量最大值					
	KDB_FLOAT64 					MinValue;						/// 变量最小值
	
	// 输入转换(Input Conversion)
	KDB_INPUT_CONVERSION			InputConversion;				/// 输入转换方式（保留扩展用，目前暂未实现）
	KDB_FLOAT64 					MaxRaw;							/// 变量原始最大值 			
	KDB_FLOAT64 					MinRaw;							/// 变量原始最小值

	// 压缩选项(Compression)
	// 采集器压缩(Collector Compression)
	KDB_BOOLEAN						CollectorCompression;			/// 采集器压缩			
	KDB_INT8						CollectorCompressionMode;		/// 采集器压缩模式
	KDB_BOOLEAN						CollectorAbsoluteDeadbanding;	/// 死区是否为绝对值表示（还是百分比）
	KDB_FLOAT32						CollectorDeadbandPercent;		/// 采集器死区(百分比)
	KDB_FLOAT64						CollectorAbsoluteDeadband;		/// 压缩死区（绝对值）
	KDB_INT32						CollectorCompressionTimeout;	/// 压缩超时(最大时间值:单位ms)
	KDB_INT32						CollectorCompressionTimeoutMin;	/// 最小时间间隔（毫秒）	
	
	
	// 存储压缩(Archive Compression)
	KDB_BOOLEAN						ArchiveControl;					/// 存储控制(是否记录历史数据标志)
	KDB_BOOLEAN						ArchiveVersionSupport;			/// 是否支持多版本数据
	KDB_BOOLEAN						ArchiveShutdown;				/// 存储服务器关机状态
	KDB_BOOLEAN						ArchiveStepValue;				/// 压缩或者查询时不进行线性插值，而是以步进的方式
	KDB_INT8						ArchiveStoreMode;				/// 存储方式（保留属性，未来扩展用）
	KDB_BOOLEAN						ArchiveCompression;				/// 存储压缩控制			
	KDB_BOOLEAN						ArchiveAbsoluteDeadbanding;		/// 死区是否为绝对值表示（还是百分比）
	KDB_INT8						ArchiveCompressionMode;			/// 压缩方式
	KDB_FLOAT32 					ArchiveDeadbandPercent;			/// 存储死区(百分比)	
	KDB_FLOAT64						ArchiveAbsoluteDeadband;		/// 存储死区（绝对值）
	KDB_INT32						ArchiveCompressionTimeout;		/// 存储压缩超时(最大时间值:单位ms)
	KDB_INT32						ArchiveCompressionTimeoutMin;	/// 最小时间间隔（毫秒）	
	
	// 安全性(Security)
	KDB_WSTR						SecurityReadRole;				/// 具有读权限的角色			
	KDB_WSTR						SecurityWriteRole;				/// 具有写权限的角色			
	KDB_WSTR						SecurityAdminRole;				/// 具有修改变量配置权限的角色

	// 审计(Audit)
	KDB_TIMESTAMP					CreateTime;						/// 创建时间（只读属性）
	KDB_TIMESTAMP					LastModified;					/// 上次修改变量配置的时间（只读属性）
	KDB_WSTR						CreateUser;						/// 创建变量的用户（只读属性）
	KDB_WSTR						LastModifiedUser;				/// 上次修改变量配置的用户（只读属性）
	KDB_INT32						ElectronicRecord;				/// 记录电子记录（保留扩展用）

	// 计算属性(Calculation)
	KDB_WSTR						Calculation;					/// 计算公式或脚本
	KDB_UINT16						NumberOfCalculationTriggers;	/// 触发计算的变量个数
	KDB_WSTR_ARRAY					CalculationTriggers;			/// 触发计算的变量(CalculationDependencies)

	// 为采集器保留的属性域(每个属性的具体含义由各个采集器确定，不同采集器含义不同)
	KDB_INT32						TagGeneral1;					/// 保留的额外属性
	KDB_INT32						TagGeneral2;					/// 保留的额外属性
	KDB_INT32						TagGeneral3;					/// 保留的额外属性
	KDB_INT32						TagGeneral4;					/// 保留的额外属性
	KDB_WSTR						TagGeneral5;					/// 保留的额外属性
	KDB_WSTR						TagGeneral6;					/// 保留的额外属性
	KDB_WSTR						TagGeneral7;					/// 保留的额外属性
	KDB_WSTR						TagGeneral8;					/// 保留的额外属性
	KDB_FLOAT64						TagGeneral9;					/// 保留的额外属性
	KDB_FLOAT64						TagGeneral10;					/// 保留的额外属性
	KDB_FLOAT64						TagGeneral11;					/// 保留的额外属性
	KDB_FLOAT64						TagGeneral12;					/// 保留的额外属性
	KDB_INT32						TagGeneral13;					/// 保留的额外属性
	KDB_INT32						TagGeneral14;					/// 保留的额外属性
	KDB_INT32						TagGeneral15;					/// 保留的额外属性
	KDB_INT32						TagGeneral16;					/// 保留的额外属性
	KDB_WSTR						TagGeneral17;					/// 保留的额外属性
	KDB_WSTR						TagGeneral18;					/// 保留的额外属性
	KDB_WSTR						TagGeneral19;					/// 保留的额外属性
	KDB_WSTR						TagGeneral20;					/// 保留的额外属性

	// 为系统未来扩展保留的属性域(采集器不能使用)
	KDB_INT32						SystemGeneral1;					/// 保留的额外属性
	KDB_INT32						SystemGeneral2;					/// 保留的额外属性
	KDB_INT32						SystemGeneral3;					/// 保留的额外属性
	KDB_INT32						SystemGeneral4;					/// 保留的额外属性
	KDB_WSTR						SystemGeneral5;					/// 保留的额外属性
	KDB_WSTR						SystemGeneral6;					/// 保留的额外属性
	KDB_WSTR						SystemGeneral7;					/// 保留的额外属性
	KDB_WSTR						SystemGeneral8;					/// 保留的额外属性
	KDB_FLOAT64						SystemGeneral9;					/// 保留的额外属性
	KDB_FLOAT64						SystemGeneral10;				/// 保留的额外属性
	KDB_FLOAT64						SystemGeneral11;				/// 保留的额外属性
	KDB_FLOAT64						SystemGeneral12;				/// 保留的额外属性
	KDB_INT32						SystemGeneral13;				/// 保留的额外属性
	KDB_INT32						SystemGeneral14;				/// 保留的额外属性
	KDB_INT32						SystemGeneral15;				/// 保留的额外属性
	KDB_INT32						SystemGeneral16;				/// 保留的额外属性
	KDB_WSTR						SystemGeneral17;				/// 保留的额外属性
	KDB_WSTR						SystemGeneral18;				/// 保留的额外属性
	KDB_WSTR						SystemGeneral19;				/// 保留的额外属性
	KDB_WSTR						SystemGeneral20;				/// 保留的额外属性

	// 专为用户保留的属性(用于用户自定义扩展属性域，采集器和系统不使用)
	KDB_INT32						UserGeneral1;					/// 保留的额外属性
	KDB_INT32						UserGeneral2;					/// 保留的额外属性
	KDB_INT32						UserGeneral3;					/// 保留的额外属性
	KDB_INT32						UserGeneral4;					/// 保留的额外属性
	KDB_WSTR						UserGeneral5;					/// 保留的额外属性
	KDB_WSTR						UserGeneral6;					/// 保留的额外属性
	KDB_WSTR						UserGeneral7;					/// 保留的额外属性
	KDB_WSTR						UserGeneral8;					/// 保留的额外属性
	KDB_FLOAT64						UserGeneral9;					/// 保留的额外属性
	KDB_FLOAT64						UserGeneral10;					/// 保留的额外属性	

} KDB_TAG_PROPERTIES,*PKDB_TAG_PROPERTIES;

/// <summary> 
/// 变量域。
/// </summary> 
typedef struct KDBTagFields
{
	KDB_BOOLEAN						AllFields;						/// 所有域
	
	// 常规属性(General)
	KDB_BOOLEAN						TagName;						/// 变量名称
	KDB_BOOLEAN						EngineeringUnit;				/// 变量工程单位
	KDB_BOOLEAN						Description;					/// 变量描述						
	KDB_BOOLEAN						TagId;							/// 变量ID（只读属性）
	KDB_BOOLEAN						DigitalSetId;					/// 变量默认的状态集ID（Digital类型的变量支持）

	// 数据采集相关属性(Collection)
	// 数据源
	KDB_BOOLEAN						CollectorName;					/// 数据采集器名称
	KDB_BOOLEAN						CollectorType;					/// 数据采集器类型(不显示)				
	KDB_BOOLEAN						SourceAddress;					/// 数据源地址
	KDB_BOOLEAN						DataType;						/// 变量数据类型
	KDB_BOOLEAN						DataLength;						/// 变量数据长度(字符串/BLOB类型，以字节为单位)

	// 采集选项
	KDB_BOOLEAN						CollectionControl;				/// 采集控制(是否采集标志)
	KDB_BOOLEAN			 			CollectionMode;					/// 采集方式
	KDB_BOOLEAN 					CollectionInterval;				/// 采集间隔(采样周期:单位ms)			
	KDB_BOOLEAN 					CollectionOffset;				/// 采集偏移(相位:单位ms)
	KDB_BOOLEAN						TimestampType;					/// 时间戳由谁提供
	KDB_BOOLEAN						TimeZoneBias;					/// 与UTC标准时间的差值(单位：分钟)
	KDB_BOOLEAN						TimeAdjustment;					/// 时间校正(单位:毫秒)

	// 数据转换(Scaling/Conversion)
	// 值域(Engineering Unit Range)
	KDB_BOOLEAN 					MaxValue;						/// 变量最大值					
	KDB_BOOLEAN 					MinValue;						/// 变量最小值

	// 输入转换(Input Conversion)
	KDB_BOOLEAN						InputConversion;				/// 输入转换方式(保留扩展用，目前暂未实现)
	KDB_BOOLEAN 					MaxRaw;							/// 变量原始最大值 			
	KDB_BOOLEAN 					MinRaw;							/// 变量原始最小值

	// 压缩选项(Compression)
	// 采集器压缩(Collector Compression)
	KDB_BOOLEAN						CollectorCompression;			/// 采集器压缩			
	KDB_BOOLEAN						CollectorCompressionMode;		/// 采集器压缩模式
	KDB_BOOLEAN						CollectorAbsoluteDeadbanding;	/// 死区是否为绝对值表示（还是百分比）
	KDB_BOOLEAN						CollectorDeadbandPercent;		/// 采集器死区(百分比)
	KDB_BOOLEAN						CollectorAbsoluteDeadband;		/// 压缩死区（绝对值）
	KDB_BOOLEAN						CollectorCompressionTimeout;	/// 压缩超时(最大时间值:单位ms)
	KDB_BOOLEAN						CollectorCompressionTimeoutMin;	/// 最小时间间隔（毫秒）	
	

	// 存储压缩(Archive Compression)
	KDB_BOOLEAN						ArchiveControl;					/// 存储控制(是否记录历史数据标志)
	KDB_BOOLEAN						ArchiveVersionSupport;			/// 是否支持多版本数据
	KDB_BOOLEAN						ArchiveShutdown;				/// 存储服务器关机状态
	KDB_BOOLEAN						ArchiveStepValue;				/// 压缩或者查询时不进行线性插值，而是以步进的方式
	KDB_BOOLEAN						ArchiveStoreMode;				/// 存储方式（保留属性，未来扩展用）
	KDB_BOOLEAN						ArchiveCompression;				/// 存储压缩控制			
	KDB_BOOLEAN						ArchiveAbsoluteDeadbanding;		/// 死区是否为绝对值表示（还是百分比）
	KDB_BOOLEAN						ArchiveCompressionMode;			/// 压缩方式（目前只支持旋转门压缩，保留扩展用）
	KDB_BOOLEAN 					ArchiveDeadbandPercent;			/// 存储死区(百分比)	
	KDB_BOOLEAN						ArchiveAbsoluteDeadband;		/// 存储死区（绝对值）
	KDB_BOOLEAN						ArchiveCompressionTimeout;		/// 存储压缩超时(最大时间值:单位ms)
	KDB_BOOLEAN						ArchiveCompressionTimeoutMin;	/// 最小时间间隔（毫秒）	

	// 安全性(Security)
	KDB_BOOLEAN						SecurityReadRole;				/// 具有读权限的角色			
	KDB_BOOLEAN						SecurityWriteRole;				/// 具有写权限的角色			
	KDB_BOOLEAN						SecurityAdminRole;				/// 具有修改变量配置权限的角色

	// 审计(Audit)
	KDB_BOOLEAN						CreateTime;						/// 创建时间
	KDB_BOOLEAN						LastModified;					/// 上次修改变量配置的时间(只读属性)
	KDB_BOOLEAN						CreateUser;						/// 创建用户
	KDB_BOOLEAN						LastModifiedUser;				/// 上次修改变量配置的用户(只读属性)
	KDB_BOOLEAN						ElectronicRecord;				/// 记录电子记录(保留扩展用)

	// 计算属性(Calculation)
	KDB_BOOLEAN						Calculation;					/// 计算公式或脚本
	KDB_BOOLEAN						CalculationTriggers;			/// 触发计算的变量(CalculationDependencies)

	// 为采集器保留的属性域(每个属性的具体含义由各个采集器确定，不同采集器含义不同)
	KDB_BOOLEAN						TagGeneral1;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral2;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral3;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral4;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral5;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral6;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral7;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral8;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral9;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral10;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral11;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral12;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral13;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral14;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral15;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral16;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral17;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral18;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral19;					/// 保留的额外属性
	KDB_BOOLEAN						TagGeneral20;					/// 保留的额外属性

	// 为系统未来扩展保留的属性域(采集器不能使用)
	KDB_BOOLEAN						SystemGeneral1;					/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral2;					/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral3;					/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral4;					/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral5;					/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral6;					/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral7;					/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral8;					/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral9;					/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral10;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral11;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral12;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral13;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral14;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral15;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral16;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral17;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral18;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral19;				/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral20;				/// 保留的额外属性

	// 专为用户保留的属性(用于用户自定义扩展属性域，采集器和系统不使用)
	KDB_BOOLEAN						UserGeneral1;					/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral2;					/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral3;					/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral4;					/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral5;					/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral6;					/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral7;					/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral8;					/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral9;					/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral10;					/// 保留的额外属性	

}KDB_TAG_FIELDS,*PKDB_TAG_FIELDS;


/// <summary> 
/// 变量记录集
/// </summary> 
typedef struct KDBTagRecordset
{
	KDB_UINT32						NumberOfRecords;				/// 记录数目
	PKDB_TAG_PROPERTIES				TagRecords;						/// 变量记录
} KDB_TAG_RECORDSET, *PKDB_TAG_RECORDSET;


/// <summary> 
/// 变量组属性
/// </summary> 
typedef struct KDBTagGroupProperties
{
	// 变量组本身属性
	KDB_UINT32					GroupID;							/// 组标识符
	KDB_UINT32					ParentID;							/// 父组标识符
	KDB_WSTR					GroupName;							/// 组名
	KDB_WSTR					Description;						/// 变量组描述
}KDB_TAG_GROUP_PROPERTIES , *PKDB_TAG_GROUP_PROPERTIES;


//==============================================================================
//
// 数字状态集相关数据结构
//
//==============================================================================

/// <summary> 
/// 数据状态记录（描述一个数据状态集或一个数据状态）。
/// </summary> 
typedef struct KDBDigitalRecord
{
	KDB_INT16	ID;							// ID
	KDB_WSTR	Name;						// 名字
}KDB_DIGITAL_RECORD,*PKDB_DIGITAL_RECORD;

/// <summary> 
/// 数据状态记录集（描述数据状态集集合或数据状态集合）。
/// </summary> 
typedef struct KDBDigitalRecordset
{
	KDB_UINT32			NumberOfRecords;	// 记录数目
	PKDB_DIGITAL_RECORD	DigitalRecords;		// 记录数组
}KDB_DIGITAL_RECORDSET,*PKDB_DIGITAL_RECORDSET;


//==============================================================================
// 
// 数据采集器相关的结构
// 
//==============================================================================

/// <summary> 
/// 数据采集器属性域
/// </summary> 
typedef struct KDBCollectorProperties
{
	// 常规选项(General)
	KDB_WSTR						CollectorName;							/// 采集器名称(在系统内必须唯一)
	KDB_WSTR						CollectorDescription;					/// 采集器描述
	KDB_WSTR						ComputerName;							/// 计算机名(采集器所在的，由采集器自己设置)
	KDB_COLLECTOR_STATUS			CollectorStatus;						/// 采集器状态
	KDB_COLLECTOR_TYPE				CollectorType;							/// 采集器类型
	KDB_INT32						CollectorId;							/// 采集器ID（只读属性）
	
	// 缓存配置
	KDB_BOOLEAN						BufferAndForward;						/// 是否启用缓存功能(当Historian不在线时，采集器将数据缓存到本地)
	KDB_BOOLEAN						DisconnectStartup;						/// 是否启用在非连接Historian状态下也可启动采集
	KDB_INT32						MinimumDiskFreeBufferSize;				/// 采集器最小缓存磁盘空间（以M为单位，废弃未使用）
	KDB_INT32						MaximumMemoryBufferSize;				/// 采集器最大缓存内存空间（以M为单位）
	KDB_INT32						BufferFileMaxSize;						/// 单个缓存文件的最大尺寸（以M为单位）
	KDB_WSTR						BufferFileLocation;						/// 缓存文件路径(为空则由采集器自己决定)
	
	// 服务器相关属性
	KDB_WSTR						HistorianServerAddress;					/// 数据库服务器地址
	KDB_WSTR						HistorianServerPort;					/// 数据库服务器端口
	KDB_WSTR						HistorianUserName;						/// 用于登录服务器的名称
	KDB_WSTR						HistorianPassword;						/// 用于登录服务器的密码

	// 网络相关配置
	KDB_INT32						NetworkTimeout;							/// 网络连接超时(毫秒)
	KDB_INT32						TryConnectInterval;						/// 尝试重连间隔(毫秒)
	KDB_INT32						HeartbeatInterval;						/// 发送心跳包间隔（毫秒）
	KDB_INT32						StatisticsInterval;						/// 统计数据的间隔（毫秒）
	
	// 采集器冗余相关属性(Collector-Level Failover)
	KDB_BOOLEAN						FailoverControl;						/// 是否启用采集器级别的冗余切换
	KDB_BOOLEAN						FailoverIsActiveCollector;				/// 采集器是否处于激活状态（发送数据）
	KDB_BOOLEAN						FailoverAutoActiveWhenLostConnection;	/// 当丢失到服务器连接时，非激活采集器自动激活
	KDB_BOOLEAN						FailoverOnCollectorStatus;				/// 采集器状态不正常时(未知或服务停止时)切换
	KDB_BOOLEAN						FailoverOnWatchdogTagBadQuality;		/// 监控变量的质量戳为坏时切换
	KDB_BOOLEAN						FailoverOnWatchdogTagNonZeroValue;		/// 当监控变量的值为非0时切换(切换开关变量)
	KDB_BOOLEAN						FailoverOnWatchdogTagZeroValue;			/// 当监控变量的值为0时切换（与非0时切换互斥，两者最多启用一个）
	KDB_BOOLEAN						FailoverOnWatchdogTagNoNewValue;		/// 当监控变量在一段时间内没有新的值到达时切换
	KDB_INT32						FailoverWatchdogTagNoNewValueTimeout;	/// 在切换之前允许监控变量没有新值的最长持续时间（毫秒）
	KDB_INT32						FailoverInactiveCachePeriod;			/// 当采集器处于于非激活状态下时缓存多长时间范围内的数据（毫秒，为0时不采集、不缓存数据）
	KDB_WSTR						FailoverMasterCollector;				/// 以当前采集器为后备的主采集器名称
	KDB_WSTR						FailoverWatchdogTag;					/// 用于检测当前采集器状态是否正常的监控变量

	// 变量相关属性(Tags)
	KDB_WSTR						DefaultTagPrefix;						/// 缺省变量前缀
	KDB_INT32						DefaultCollectionInterval;				/// 缺省采集周期（毫秒）
	KDB_COLLECTION_MODE				DefaultCollectionType;					/// 缺省采集方式
	KDB_TIMESTAMP_TYPE				DefaultTimestampType;					/// 缺省时间戳类型

	// 采集器默认压缩属性
	KDB_BOOLEAN						DefaultCompression;						/// 缺省压缩使能
	KDB_INT8						DefaultCompressionMode;					/// 缺省压缩模式
	KDB_BOOLEAN						DefaultCompressionAbsoluteDeadbanding;	/// 缺省压缩死区是否为绝对值表示（还是百分比）
	KDB_FLOAT32						DefaultCompressionDeadbandPercent;		/// 缺省压缩死区（百分比）
	KDB_FLOAT64						DefaultCompressionAbsoluteDeadband;		/// 缺省压缩死区压缩死区（绝对值）
	KDB_INT32						DefaultCompressionTimeout;				/// 缺省压缩超时（毫秒）
	KDB_INT32						DefaultCompressionTimeoutMin;			/// 缺省最小时间间隔（毫秒）
		
	// 高级选项(Advanced)
	KDB_BOOLEAN						OnlineTagConfigurationChanges;			/// 变量配置更改立即生效
	KDB_BOOLEAN						CanBrowseSource;						/// 能够浏览数据源
	KDB_BOOLEAN						CanSourceTimestamp;						/// 数据源能否提供时间戳
	KDB_BOOLEAN						SourceTimeInLocalTime;					/// UTC时间或本地时间
	KDB_BOOLEAN						ShouldAdjustTime;						/// 是否需要调整时间(只在数据时间戳是由采集器提供时有效，根据服务器时间调整)
	KDB_BOOLEAN						ShouldQueueWrites;						/// 是否使用延迟写(提高网络IO性能和吞吐量，但会影响数据实时性)
	KDB_INT32						QueueWritesMaxDelay;					/// 发送数据之前的最大缓存延迟（以毫秒为单位）
	KDB_INT32						QueueWritesMaxRecordNumber;				/// 发送数据之前的最大缓存记录条数
	KDB_INT32						CollectionDelay;						/// 启动延迟（在开始采集数据之前的延迟，单位：毫秒）


	// 采集器状态输出
	KDB_WSTR						StatusOutputAddress;					/// 采集状态输出地址(将采集器状态作为一个变量)
	KDB_WSTR						RateOutputAddress;						/// 采集速率输出地址(将采集器速率作为一个变量)		
	KDB_WSTR						HeartbeatOutputAddress;					/// 心跳信息输出地址(将心跳信息作为一个变量）

	// 安全审计
	KDB_TIMESTAMP 					LastModified;							/// 最后修改时间
	KDB_WSTR						LastModifiedUser;						/// 最后修改用户

	// 配置(根据不同的数据采集器选项不同)
	KDB_INT32						CollectorGeneral1;						/// 采集器专有属性1
	KDB_INT32						CollectorGeneral2;						/// 采集器专有属性2
	KDB_INT32						CollectorGeneral3;						/// 采集器专有属性3
	KDB_INT32						CollectorGeneral4;						/// 采集器专有属性4
	KDB_WSTR						CollectorGeneral5;						/// 采集器专有属性5
	KDB_WSTR						CollectorGeneral6;						/// 采集器专有属性6
	KDB_WSTR						CollectorGeneral7;						/// 采集器专有属性7
	KDB_WSTR						CollectorGeneral8;						/// 采集器专有属性8
	KDB_FLOAT64						CollectorGeneral9;						/// 采集器专有属性9
	KDB_FLOAT64						CollectorGeneral10;						/// 采集器专有属性10
	KDB_FLOAT64						CollectorGeneral11;						/// 采集器专有属性11
	KDB_FLOAT64						CollectorGeneral12;						/// 采集器专有属性12
	KDB_INT32						CollectorGeneral13;						/// 采集器专有属性13
	KDB_INT32						CollectorGeneral14;						/// 采集器专有属性14
	KDB_INT32						CollectorGeneral15;						/// 采集器专有属性15
	KDB_INT32						CollectorGeneral16;						/// 采集器专有属性16
	KDB_WSTR						CollectorGeneral17;						/// 采集器专有属性17
	KDB_WSTR						CollectorGeneral18;						/// 采集器专有属性18
	KDB_WSTR						CollectorGeneral19;						/// 采集器专有属性19
	KDB_WSTR						CollectorGeneral20;						/// 采集器专有属性20

	// 为系统未来扩展保留的属性域（未来版本升级时使用）
	KDB_INT32						SystemGeneral1;							/// 保留的额外属性
	KDB_INT32						SystemGeneral2;							/// 保留的额外属性
	KDB_INT32						SystemGeneral3;							/// 保留的额外属性
	KDB_INT32						SystemGeneral4;							/// 保留的额外属性
	KDB_WSTR						SystemGeneral5;							/// 保留的额外属性
	KDB_WSTR						SystemGeneral6;							/// 保留的额外属性
	KDB_WSTR						SystemGeneral7;							/// 保留的额外属性
	KDB_WSTR						SystemGeneral8;							/// 保留的额外属性
	KDB_FLOAT64						SystemGeneral9;							/// 保留的额外属性
	KDB_FLOAT64						SystemGeneral10;						/// 保留的额外属性
	KDB_FLOAT64						SystemGeneral11;						/// 保留的额外属性
	KDB_FLOAT64						SystemGeneral12;						/// 保留的额外属性
	KDB_INT32						SystemGeneral13;						/// 保留的额外属性
	KDB_INT32						SystemGeneral14;						/// 保留的额外属性
	KDB_INT32						SystemGeneral15;						/// 保留的额外属性
	KDB_INT32						SystemGeneral16;						/// 保留的额外属性
	KDB_WSTR						SystemGeneral17;						/// 保留的额外属性
	KDB_WSTR						SystemGeneral18;						/// 保留的额外属性
	KDB_WSTR						SystemGeneral19;						/// 保留的额外属性
	KDB_WSTR						SystemGeneral20;						/// 保留的额外属性

	// 专为用户保留的属性(用于用户自定义扩展属性域，采集器和系统不使用)
	KDB_INT32						UserGeneral1;							/// 保留的额外属性
	KDB_INT32						UserGeneral2;							/// 保留的额外属性
	KDB_INT32						UserGeneral3;							/// 保留的额外属性
	KDB_INT32						UserGeneral4;							/// 保留的额外属性
	KDB_WSTR						UserGeneral5;							/// 保留的额外属性
	KDB_WSTR						UserGeneral6;							/// 保留的额外属性
	KDB_WSTR						UserGeneral7;							/// 保留的额外属性
	KDB_WSTR						UserGeneral8;							/// 保留的额外属性
	KDB_FLOAT64						UserGeneral9;							/// 保留的额外属性
	KDB_FLOAT64						UserGeneral10;							/// 保留的额外属性	

}KDB_COLLECTOR_PROPERTIES , *PKDB_COLLECTOR_PROPERTIES;


/// <summary> 
/// 采集器属性域
/// </summary> 
typedef struct KDBCollectorFields
{
	KDB_BOOLEAN						AllFields;								/// 所有域
	
	// 常规选项(General)
	KDB_BOOLEAN						CollectorName;							/// 采集器名称(在系统内必须唯一)
	KDB_BOOLEAN						CollectorDescription;					/// 采集器描述
	KDB_BOOLEAN						ComputerName;							/// 计算机名(采集器所在的，由采集器自己设置)
	KDB_BOOLEAN						CollectorStatus;						/// 采集器状态
	KDB_BOOLEAN						CollectorType;							/// 采集器类型
	KDB_BOOLEAN						CollectorId;							/// 采集器ID
	
	// 缓存配置
	KDB_BOOLEAN						BufferAndForward;						/// 是否启用缓存功能(当Historian不在线时，采集器将数据缓存到本地)
	KDB_BOOLEAN						DisconnectStartup;						/// 是否启用在非连接Historian状态下也可启动采集
	KDB_BOOLEAN						MinimumDiskFreeBufferSize;				/// 采集器最小缓存磁盘空间（以M为单位，废弃未使用）
	KDB_BOOLEAN						MaximumMemoryBufferSize;				/// 采集器最大缓存内存空间（以M为单位）
	KDB_BOOLEAN						BufferFileMaxSize;						/// 单个缓存文件的最大尺寸（以M为单位）
	KDB_BOOLEAN						BufferFileLocation;						/// 缓存文件路径(为空则由采集器自己决定)
	
	// 服务器相关属性
	KDB_BOOLEAN						HistorianServerAddress;					/// 数据库服务器地址
	KDB_BOOLEAN						HistorianServerPort;					/// 数据库服务器端口
	KDB_BOOLEAN						HistorianUserName;						/// 用于登录服务器的名称
	KDB_BOOLEAN						HistorianPassword;						/// 用于登录服务器的密码

	// 网络相关配置
	KDB_BOOLEAN						NetworkTimeout;							/// 网络连接超时(毫秒)
	KDB_BOOLEAN						TryConnectInterval;						/// 尝试重连间隔(毫秒)
	KDB_BOOLEAN						HeartbeatInterval;						/// 发送心跳包间隔（毫秒）
	KDB_BOOLEAN						StatisticsInterval;						/// 统计数据的间隔（毫秒）
	
	// 采集器冗余相关属性(Collector-Level Failover)
	KDB_BOOLEAN						FailoverControl;						/// 是否启用采集器级别的冗余切换
	KDB_BOOLEAN						FailoverIsActiveCollector;				/// 采集器是否处于激活状态（发送数据）
	KDB_BOOLEAN						FailoverAutoActiveWhenLostConnection;	/// 当丢失到服务器连接时，非激活采集器自动激活
	KDB_BOOLEAN						FailoverOnCollectorStatus;				/// 采集器状态不正常时(未知或服务停止时)切换
	KDB_BOOLEAN						FailoverOnWatchdogTagBadQuality;		/// 监控变量的质量戳为坏时切换
	KDB_BOOLEAN						FailoverOnWatchdogTagNonZeroValue;		/// 当监控变量的值为非0时切换(切换开关变量)
	KDB_BOOLEAN						FailoverOnWatchdogTagZeroValue;			/// 当监控变量的值为0时切换（与非0时切换互斥，两者最多启用一个）
	KDB_BOOLEAN						FailoverOnWatchdogTagNoNewValue;		/// 当监控变量在一段时间内没有新的值到达时切换
	KDB_BOOLEAN						FailoverWatchdogTagNoNewValueTimeout;	/// 在切换之前允许监控变量没有新值的最长持续时间（毫秒）
	KDB_BOOLEAN						FailoverInactiveCachePeriod;			/// 当采集器处于于非激活状态下时缓存多长时间范围内的数据（毫秒）
	KDB_BOOLEAN						FailoverMasterCollector;				/// 以当前采集器为后备的主采集器名称
	KDB_BOOLEAN						FailoverWatchdogTag;					/// 用于检测当前采集器状态是否正常的监控变量

	// 变量相关属性(Tags)
	KDB_BOOLEAN						DefaultTagPrefix;						/// 缺省变量前缀
	KDB_BOOLEAN						DefaultCollectionInterval;				/// 缺省采集周期（毫秒）
	KDB_BOOLEAN						DefaultCollectionType;					/// 缺省采集方式
	KDB_BOOLEAN						DefaultTimestampType;					/// 缺省时间戳类型

	// 采集器默认压缩属性
	KDB_BOOLEAN						DefaultCompression;						/// 缺省压缩使能
	KDB_BOOLEAN						DefaultCompressionMode;					/// 缺省压缩模式
	KDB_BOOLEAN						DefaultCompressionAbsoluteDeadbanding;	/// 缺省压缩死区是否为绝对值表示（还是百分比）
	KDB_BOOLEAN						DefaultCompressionDeadbandPercent;		/// 缺省压缩死区（百分比）
	KDB_BOOLEAN						DefaultCompressionAbsoluteDeadband;		/// 缺省压缩死区压缩死区（绝对值）
	KDB_BOOLEAN						DefaultCompressionTimeout;				/// 缺省压缩超时（毫秒）
	KDB_BOOLEAN						DefaultCompressionTimeoutMin;			/// 缺省最小时间间隔（毫秒）

	// 高级选项(Advanced)
	KDB_BOOLEAN						OnlineTagConfigurationChanges;			/// 变量配置更改立即生效
	KDB_BOOLEAN						CanBrowseSource;						/// 能够浏览数据源
	KDB_BOOLEAN						CanSourceTimestamp;						/// 数据源能否提供时间戳
	KDB_BOOLEAN						SourceTimeInLocalTime;					/// UTC时间或本地时间
	KDB_BOOLEAN						ShouldAdjustTime;						/// 是否需要调整时间(只在数据时间戳是由采集器提供时有效，根据服务器时间调整)
	KDB_BOOLEAN						ShouldQueueWrites;						/// 是否使用延迟写(提高网络IO性能和吞吐量，但会影响数据实时性)
	KDB_BOOLEAN						QueueWritesMaxDelay;					/// 发送数据之前的最大缓存延迟（以毫秒为单位）
	KDB_BOOLEAN						QueueWritesMaxRecordNumber;				/// 发送数据之前的最大缓存记录条数
	KDB_BOOLEAN						CollectionDelay;						/// 启动延迟（在开始采集数据之前的延迟，单位：毫秒）


	// 采集器状态输出
	KDB_BOOLEAN						StatusOutputAddress;					/// 采集状态输出地址(将采集器状态作为一个变量)
	KDB_BOOLEAN						RateOutputAddress;						/// 采集速率输出地址(将采集器速率作为一个变量)		
	KDB_BOOLEAN						HeartbeatOutputAddress;					/// 心跳信息输出地址(将心跳信息作为一个变量）

	// 安全审计
	KDB_BOOLEAN 					LastModified;							/// 最后修改时间
	KDB_BOOLEAN						LastModifiedUser;						/// 最后修改用户

	// 配置(根据不同的数据采集器选项不同)
	KDB_BOOLEAN						CollectorGeneral1;						/// 采集器专有属性1
	KDB_BOOLEAN						CollectorGeneral2;						/// 采集器专有属性2
	KDB_BOOLEAN						CollectorGeneral3;						/// 采集器专有属性3
	KDB_BOOLEAN						CollectorGeneral4;						/// 采集器专有属性4
	KDB_BOOLEAN						CollectorGeneral5;						/// 采集器专有属性5
	KDB_BOOLEAN						CollectorGeneral6;						/// 采集器专有属性6
	KDB_BOOLEAN						CollectorGeneral7;						/// 采集器专有属性7
	KDB_BOOLEAN						CollectorGeneral8;						/// 采集器专有属性8
	KDB_BOOLEAN						CollectorGeneral9;						/// 采集器专有属性9
	KDB_BOOLEAN						CollectorGeneral10;						/// 采集器专有属性10
	KDB_BOOLEAN						CollectorGeneral11;						/// 采集器专有属性11
	KDB_BOOLEAN						CollectorGeneral12;						/// 采集器专有属性12
	KDB_BOOLEAN						CollectorGeneral13;						/// 采集器专有属性13
	KDB_BOOLEAN						CollectorGeneral14;						/// 采集器专有属性14
	KDB_BOOLEAN						CollectorGeneral15;						/// 采集器专有属性15
	KDB_BOOLEAN						CollectorGeneral16;						/// 采集器专有属性16
	KDB_BOOLEAN						CollectorGeneral17;						/// 采集器专有属性17
	KDB_BOOLEAN						CollectorGeneral18;						/// 采集器专有属性18
	KDB_BOOLEAN						CollectorGeneral19;						/// 采集器专有属性19
	KDB_BOOLEAN						CollectorGeneral20;						/// 采集器专有属性20

	// 为系统未来扩展保留的属性域（未来版本升级时使用）
	KDB_BOOLEAN						SystemGeneral1;							/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral2;							/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral3;							/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral4;							/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral5;							/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral6;							/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral7;							/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral8;							/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral9;							/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral10;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral11;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral12;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral13;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral14;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral15;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral16;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral17;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral18;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral19;						/// 保留的额外属性
	KDB_BOOLEAN						SystemGeneral20;						/// 保留的额外属性

	// 专为用户保留的属性(用于用户自定义扩展属性域，采集器和系统不使用)
	KDB_BOOLEAN						UserGeneral1;							/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral2;							/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral3;							/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral4;							/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral5;							/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral6;							/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral7;							/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral8;							/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral9;							/// 保留的额外属性
	KDB_BOOLEAN						UserGeneral10;							/// 保留的额外属性	

}KDB_COLLECTOR_FIELDS,*PKDB_COLLECTOR_FIELDS;


/// <summary> 
/// 采集器记录集
/// </summary> 
typedef struct KDBCollectorRecordset
{
	KDB_UINT32						NumberOfRecords;				/// 采集器数目
	PKDB_COLLECTOR_PROPERTIES		CollectorRecords;				/// 采集器记录
} KDB_COLLECTOR_RECORDSET, *PKDB_COLLECTOR_RECORDSET;

/// <summary> 
/// 采集器性能统计结构。
/// </summary> 
typedef struct KDBCollectorStatistics
{
	KDB_UINT64						TotalRecordsCollected;			/// 采集的数据记录总数
	KDB_UINT64						TotalRecordsReported;			/// 发送到服务器的记录总数
	KDB_FLOAT64						AverageRecordRate;				/// 当前周期内每秒发送的记录数
	KDB_FLOAT64						MaximumRecordRate;				/// 最大的每秒发送记录数
	KDB_FLOAT64						MinimumRecordRate;				/// 最小的每秒发送记录数
}KDB_COLLECTOR_STATISTICS,*PKDB_COLLECTOR_STATISTICS;



//==============================================================================
// 
// 安全和用户相关的数据结构
// 
//==============================================================================

/// <summary> 
/// 用户属性
/// </summary> 
typedef struct KDBUserProperties
{
	KDB_WSTR			UserName;			/// 用户名
	KDB_WSTR			UserFullName;		/// 用户全称
	KDB_WSTR			UserDescription;	/// 用户描述
	KDB_WSTR			UserContactInfo;	/// 用户联系信息
	KDB_UINT32			UserFlags;			/// 用户标志(只读属性)
	KDB_TIMESTAMP		CreateTime;			/// 创建时间
	KDB_TIMESTAMP		LastModifiedTime;	/// 上次修改时间
	KDB_WSTR			LastModifiedUser;	/// 上次修改用户
} KDB_USER_PROPERTIES,*PKDB_USER_PROPERTIES;


/// <summary> 
/// 用户记录集
/// </summary> 
typedef struct KDBUserRecordset
{	
	KDB_UINT32				NumberOfRecords;	/// 用户数目
	PKDB_USER_PROPERTIES	UserRecords;		/// 用户属性
} KDB_USER_RECORDSET , *PKDB_USER_RECORDSET;


/// <summary> 
/// 角色
/// </summary> 
typedef struct KDBRoleProperties
{
	KDB_WSTR			RoleName;			/// 角色名称
	KDB_WSTR			RoleDescription;	/// 角色描述
	KDB_UINT32			RoleFlags;			/// 角色标志(只读属性)
} KDB_ROLE_PROPERTIES,*PKDB_ROLE_PROPERTIES;


/// <summary> 
/// 角色记录集
/// </summary> 
typedef struct KDBRoleRecordset
{
	KDB_UINT32				NumberOfRecords;		/// 角色数目
	PKDB_ROLE_PROPERTIES	RoleRecords;		/// 角色记录
} KDB_ROLE_RECORDSET, *PKDB_ROLE_RECORDSET;


//==============================================================================
// 
// 历史数据存储文件选项
// 
//==============================================================================

/// <summary> 
/// 存储设备属性。
/// </summary> 
typedef struct KDBArchiveStoreProperties  
{
	KDB_WSTR						StoreName;				/// 逻辑名称
	KDB_WSTR						StorePath;				/// 主文件路径
	KDB_INT32						StoreType;				/// 类型
	KDB_INT32						StoreId;				/// 标识符
	KDB_UINT32						StoreSize;				/// 当前大小(以MB为单位)
	KDB_UINT32						MaxFileSize;			/// 文件的最大大小(以MB为单位)
	KDB_UINT32						Growth;					/// 文件尺寸增长增量(单位为M)
	KDB_UINT32						StoreFlags;				/// 属性
	KDB_INT32						FileNumber;				/// 文件个数
	KDB_INT32						ActiveFileId;			/// 当前的激活文件编号
	KDB_TIMESTAMP					StartTime;				/// 历史数据的起始时间
	KDB_TIMESTAMP					EndTime;				/// 历史数据的终止时间
	KDB_TIMESTAMP					LastBackup;				/// 上次备份时间
	KDB_WSTR						LastBackupUser;			/// 上次备份的用户
	KDB_TIMESTAMP					LastModified;			/// 上次修改的时间
	KDB_WSTR						LastModifiedUser;		/// 上次修改的用户
} KDB_ARCHIVE_STORE_PROPERTIES, *PKDB_ARCHIVE_STORE_PROPERTIES;

/// <summary> 
/// 存储设备记录集
/// </summary> 
typedef struct KDBArchiveStoreRecordset
{
	KDB_UINT32						NumberOfRecords;		/// 存储文件数目
	PKDB_ARCHIVE_STORE_PROPERTIES	ArchiveRecords;			/// 存储文件记录
} KDB_ARCHIVE_STORE_RECORDSET , *PKDB_ARCHIVE_STORE_RECORDSET;


/// <summary> 
/// 存储文件属性。
/// </summary> 
typedef struct KDBArchiveFileProperties
{
	KDB_INT32			FileId;					/// 文件编号(从1开始)
	KDB_INT32			StoreId;				/// 文件所属的表空间编号
	KDB_UINT32			FileSize;				/// 文件当前尺寸(以M为单位)
	KDB_UINT32			BasePage;				/// 该文件的起始页面编号
	KDB_UINT32			FileFlags;				/// 文件属性
	KDB_WSTR			FileName;				/// 文件逻辑名称
	KDB_WSTR			FilePath;				/// 文件路径
} KDB_ARCHIVE_FILE_PROPERTIES,*PKDB_ARCHIVE_FILE_PROPERTIES;

/// <summary> 
/// 存储文件记录集。
/// </summary> 
typedef struct KDBArchiveFileRecordset
{
	KDB_UINT32						NumberOfRecords;	// 记录条数
	PKDB_ARCHIVE_FILE_PROPERTIES	FileRecords;		// 文件记录信息
} KDB_ARCHIVE_FILE_RECORDSET,*PKDB_ARCHIVE_FILE_RECORDSET;


//==============================================================================
// 
// 回调函数定义
// 
//==============================================================================


/// <summary> 
/// 	数据更新通知回调函数。
/// </summary> 
/// <param name="DBHandle">
/// 	连接句柄。
/// </param>
/// <param name="UserParameter">
/// 	自定义参数。
/// </param>
/// <param name="ChangedValues">
/// 	被修改的数据记录集，<see cref="KDB_DATA_RECORDSET"/> 。
/// </param>
/// <seealso cref="KDBDataRegisterCallback "/> 
typedef KDB_RET ( CALLBACK *KDB_DATA_CALLBACK_FUNCTION ) (
								KDB_HANDLE						DBHandle, 
								KDB_PTR							UserParameter,
								PKDB_DATA_RECORDSET				ChangedValues );

/// <summary> 
/// 	变量属性改变通知回调函数。
/// </summary> 
/// <param name="DBHandle">
/// 	连接句柄。
/// </param>
/// <param name="UserParameter">
/// 	自定义参数。
/// </param>
/// <param name="TagProperties">
/// 	被改变的变量属性，<see cref="KDB_TAG_PROPERTIES"/> 。
/// </param>
/// <param name="ChangeType">
/// 	更改类型，<see cref="KDB_ITEM_CHANGE_TYPE"/> 。
/// </param>
/// <seealso cref="KDBTagRegisterPropertiesCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_TAG_PROPERTIES_CALLBACK_FUNCTION  ) (
								KDB_HANDLE				DBHandle, 
								KDB_PTR					UserParameter,
								PKDB_TAG_PROPERTIES		TagProperties, 
								KDB_ITEM_CHANGE_TYPE	ChangeType );


/// <summary> 
///		变量组属性更新回调通知函数。
/// </summary> 
/// <param name="DBHandle">
///		连接句柄。
/// </param>
/// <param name="UserParameter">
///		自定义参数。
/// </param>
/// <param name="GroupProperties">
///		被更新的变量组属性。
/// </param>
/// <param name="ChangeType">
///		更改类型，<see cref="KDB_ITEM_CHANGE_TYPE"/> 。
/// </param>
/// <returns>
///		成功时返回KERR_OK，失败时返回相应的错误代码。
/// </returns>
typedef KDB_RET ( CALLBACK *KDB_TAG_GROUP_PROPERTIES_CALLBACK_FUNCTION )(
								KDB_HANDLE					DBHandle,
								KDB_PTR						UserParameter,
								PKDB_TAG_GROUP_PROPERTIES	GroupProperties,
								KDB_ITEM_CHANGE_TYPE		ChangeType);



/// <summary> 
///		变量组信息更新通知回调函数。
/// </summary> 
/// <param name="DBHandle">
///		连接句柄。
/// </param>
/// <param name="UserParameter">
///		自定义参数。
/// </param>
/// <param name="GroupId">
///		被更新的变量组编号。
/// </param>
/// <param name="NumberOfTags">
///		变量数目。
/// </param>
/// <param name="TagNames">
///		变量名称数组。
/// </param>
/// <param name="ChangeType">
///		更改类型，<see cref="KDB_ITEM_CHANGE_TYPE"/> 。
/// </param>
/// <returns>
///		成功时返回KERR_OK，失败时返回相应的错误代码。
/// </returns>
typedef KDB_RET ( CALLBACK *KDB_TAG_GROUP_INFO_CALLBACK_FUNCTION)(
								KDB_HANDLE					DBHandle,
								KDB_PTR						UserParameter,
								KDB_UINT32					GroupId,
								KDB_UINT32					NumberOfTags,
								KDB_WSTR_ARRAY				TagNames,
								KDB_ITEM_CHANGE_TYPE		ChangeType);

/// <summary> 
/// 	采集器属性改变通知回调函数。
/// </summary> 
/// <param name="DBHandle">
/// 	连接句柄。
/// </param>
/// <param name="UserParameter">
/// 	自定义参数。
/// </param>
/// <param name="CollectorProperties">
/// 	被修改的采集器属性，<see cref="KDB_COLLECTOR_PROPERTIES"/> 。
/// </param>
/// <param name="ChangeType">
/// 	更改类型，<see cref="KDB_ITEM_CHANGE_TYPE"/> 。
/// </param>
/// <seealso cref="KDBCollectorRegisterPropertiesCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_COLLECTOR_PROPERTIES_CALLBACK_FUNCTION) (
								KDB_HANDLE					DBHandle, 
								KDB_PTR						UserParameter, 
								PKDB_COLLECTOR_PROPERTIES 	CollectorProperties,
								KDB_ITEM_CHANGE_TYPE		ChangeType );


/// <summary> 
/// 	采集器状态改变通知回调函数。
/// </summary> 
/// <param name="DBHandle">
/// 	连接句柄。
/// </param>
/// <param name="UserParameter">
/// 	自定义参数。
/// </param>
/// <param name="CollectorName">
/// 	采集器名称。
/// </param>
/// <param name="CollectorStatus">
/// 	采集器状态，<see cref="KDB_COLLECTOR_STATUS"/> 。
/// </param>
/// <seealso cref="KDBCollectorRegisterStatusCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_COLLECTOR_STATUS_CALLBACK_FUNCTION) (
								KDB_HANDLE					DBHandle, 
								KDB_PTR						UserParameter, 
								KDB_CWSTR					CollectorName,
								KDB_COLLECTOR_STATUS		CollectorStatus );		


/// <summary> 
/// 	采集器变量浏览回调函数。
/// </summary> 
/// <param name="DBHandle">
/// 	连接句柄。
/// </param>
/// <param name="UserParameter">
/// 	自定义参数。
/// </param>
/// <param name="TagSourceAddressMask">
/// 	变量源地址，可以带通配符。
/// </param>
/// <param name="TagDescriptionMask">
/// 	变量描述，可以带通配符。
/// </param>
/// <param name="TagRecordset">
/// 	用于保存返回的变量属性记录集。
/// </param>
/// <seealso cref="KDBCollectorRegisterBrowseCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_COLLECTOR_BROWSE_CALLBACK_FUNCTION )(
								KDB_HANDLE				DBHandle, 
								KDB_PTR					UserParameter, 
								KDB_CWSTR				TagSourceAddressMask, 
								KDB_CWSTR				TagDescriptionMask, 
								PKDB_TAG_RECORDSET		TagRecordset);




/// <summary> 
///		层次化浏览采集器中的变量信息回调函数。
/// </summary> 
/// <param name="DBHandle">
///		连接句柄。
/// </param>
/// <param name="UserParameter">
/// 	自定义参数。
/// </param>
/// <param name="BrowsePosition">
///		浏览节点位置，如果为空则表示根节点。
/// </param>
/// <param name="BrowseRecursive">
///		是否递归浏览所有子节点，如果为真，则浏览所有子节点的变量配置，此时不返回子节点信息。
/// </param>
/// <param name="BranchFilterMask">
///		分枝过滤掩码（只有当BrowseRecursive为假时，此条件才会被应用到被浏览节点的次级分枝上）。
/// </param>
/// <param name="TagSourceAddressMask">
///		变量源地址（即采集器中的变量名），可以带通配符，也可以为空（NULL）。
/// </param>
/// <param name="TagDescriptionMask">
///		变量记录集，<see cref="KDB_TAG_RECORDSET"/> 。
/// </param>
/// <param name="ChildNodeNames">
///		用于保存返回的子节点名称（只有当BrowseRecursive为假时）。
/// </param>
/// <param name="ChildNodeIds">
///		用于保存返回的子节点标识符（只有当BrowseRecursive为假时）。
/// </param>
/// <param name="TagRecordset">
///		用于保存返回的变量配置信息。
/// </param>
/// <returns>
///		成功时返回KERR_OK，失败时返回相应的错误代码。
/// </returns>
/// <seealso cref="KDBCollectorRegisterBrowseHierarchicalCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_COLLECTOR_BROWSE_HIERARCHICAL_CALLBACK_FUNCTION)(
	KDB_HANDLE								DBHandle, 
	KDB_PTR									UserParameter, 
	KDB_CWSTR								BrowsePosition,
	KDB_BOOLEAN								BrowseRecursive,
	KDB_CWSTR								BranchFilterMask,
	KDB_CWSTR								TagSourceAddressMask,
	KDB_CWSTR								TagDescriptionMask,
	PKDB_STRING_ARRAY						ChildNodeNames,
	PKDB_STRING_ARRAY						ChildNodeIds,
	PKDB_TAG_RECORDSET						TagRecordset );


/// <summary> 
/// 	采集器控制回调函数。
/// </summary> 
/// <param name="DBHandle">
/// 	连接句柄。
/// </param>
/// <param name="UserParameter">
/// 	自定义参数。
/// </param>
/// <param name="CollectorControl">
/// 	采集器控制动作，<see cref="KDB_COLLECTOR_CONTROL"/> 。
/// </param>
/// <seealso cref="KDBCollectorRegisterControlCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_COLLECTOR_CONTROL_CALLBACK_FUNCTION ) (
								KDB_HANDLE				DBHandle, 
								KDB_PTR					UserParameter, 
								KDB_COLLECTOR_CONTROL	CollectorControl );


/// <summary> 
/// 	采集器重新装载数据回调函数。
/// </summary> 
/// <param name="DBHandle">
/// 	数据库连接句柄。
/// </param>
/// <param name="UserParameter">
/// 	自定义参数。
/// </param>
/// <param name="StartTime">
/// 	起始时间，<see cref="KDB_TIMESTAMP"/> 。
/// </param>
/// <param name="EndTime">
/// 	中止时间，<see cref="KDB_TIMESTAMP"/> 。
/// </param>
/// <param name="NumberOfTags">
/// 	变量个数。
/// </param>
/// <param name="TagNames">
/// 	变量名称数组。
/// </param>
/// <seealso cref="KDBCollectorRegisterReloadCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_COLLECTOR_RELOAD_CALLBACK_FUNCTION )(
								KDB_HANDLE				DBHandle, 
								KDB_PTR					UserParameter, 
								PKDB_TIMESTAMP			StartTime,
								PKDB_TIMESTAMP			EndTime,
								KDB_UINT32				NumberOfTags,
								KDB_WSTR_ARRAY			TagNames );


/// <summary> 
/// 	从数据采集器获得变量实时数据回调函数。
/// </summary> 
/// <param name="DBHandle">
/// 	数据库连接句柄。
/// </param>
/// <param name="UserParameter">
/// 	自定义参数。
/// </param>
/// <param name="NumberOfTags">
/// 	变量个数。
/// </param>
/// <param name="TagNames">
/// 	变量名称数组。
/// </param>
/// <param name="DataRecords">
/// 	用于保存返回的变量数值结果集。
/// </param>
/// <param name="ErrorStatuses">
/// 	用于保存返回的状态结果集。
/// </param>
/// <seealso cref="KDBCollectorRegisterCurrentValueCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_COLLECTOR_CURRENT_VALUE_CALLBACK_FUNCTION )(
								KDB_HANDLE				DBHandle, 
								KDB_PTR					UserParameter,
								KDB_UINT32				NumberOfTags, 
								KDB_WSTR_ARRAY			TagNames,
								PKDB_DATA_PROPERTIES	DataRecords,
								KDB_RET*				ErrorStatuses);

/// <summary> 
///		更新数据源的变量数据（数据回写）。
/// </summary> 
/// <param name="DBHandle">
///		数据库连接句柄。
/// </param>
/// <param name="UserParameter">
///		自定义参数。
/// </param>
/// <param name="NumberOfTags">
///		变量数目。
/// </param>
/// <param name="TagNames">
///		变量名称数组。
/// </param>
/// <param name="TagValues">
///		变量值数组（每个变量一个值）。
/// </param>
/// <param name="ErrorStatuses">
///		用于保存返回码的数组（每个变量一个返回码）。
/// </param>
/// <returns>
///		成功时返回KERR_OK，失败时返回相应的错误代码。
/// </returns>
/// <seealso cref="KDBCollectorRegisterWriteBackCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_COLLECTOR_WRITE_BACK_CALLBACK_FUNCTION)(
		KDB_HANDLE				DBHandle,
		KDB_PTR					UserParameter,
        KDB_UINT32				NumberOfTags,
		KDB_WSTR_ARRAY			TagNames,
		PKDB_VALUE				TagValues,
		KDB_RET*				ErrorStatuses );


/// <summary> 
///		测试计算脚本回调。
/// </summary> 
/// <param name="DBHandle">
///		数据库连接句柄。
/// </param>
/// <param name="UserParameter">
///		自定义参数。
/// </param>
/// <param name="ScriptClass">
///		脚本类别（Jvascript/VbScript）。
/// </param>
/// <param name="ScriptText">
///		计算脚本字符串。
/// </param>
/// <param name="ErrorLength">
///		用于保存错误信息的缓冲区大小。
/// </param>
/// <param name="ErrorDescription">
///		用于保存错误信息的缓冲区。
/// </param>
/// <param name="ScriptOK">
///		用于保存返回脚本是否正确标志。
/// </param>
/// <param name="ScriptResult">
///		用于保存计算的结果。
/// </param>
/// <returns>
///		成功时返回KERR_OK，失败时返回相应的错误代码。
/// </returns>
/// <seealso cref="KDBCollectorRegisterCalculationTestCallback"/> 
typedef KDB_RET ( CALLBACK *KDB_COLLECTOR_CALCULATION_TEST_CALLBACK_FUNCTION) (
		KDB_HANDLE				DBHandle,
		KDB_PTR					UserParameter,
		KDB_CWSTR				ScriptClass,
		KDB_CWSTR				ScriptText,
		KDB_UINT32				ErrorLength,
		KDB_WSTR				ErrorDescription,
		KDB_BOOLEAN*			ScriptOK,
		PKDB_DATA_PROPERTIES	ScriptResult );


/// <summary> 
///		用户属性更新通知回调函数。
/// </summary> 
/// <param name="DBHandle">
///		连接句柄。
/// </param>
/// <param name="UserParameter">
///		自定义参数。
/// </param>
/// <param name="UserProperties">
/// 	被修改的用户属性，<see cref="KDB_USER_PROPERTIES"/> 。
/// </param>
/// <param name="ChangeType">
/// 	更改类型，<see cref="KDB_ITEM_CHANGE_TYPE"/> 。
/// </param>
/// <returns>
///		成功时返回KERR_OK，失败时返回相应的错误代码。
/// </returns>
typedef KDB_RET ( CALLBACK * KDB_USER_PROPERTIES_CALLBACK_FUNCTION)(
	KDB_HANDLE				DBHandle, 
	KDB_PTR					UserParameter,
	PKDB_USER_PROPERTIES	UserProperties,
	KDB_ITEM_CHANGE_TYPE	ChangeType );


/// <summary> 
///		角色属性更新通知回调函数。
/// </summary> 
/// <param name="DBHandle">
///		连接句柄。
/// </param>
/// <param name="UserParameter">
///		自定义参数。
/// </param>
/// <param name="RoleProperties">
/// 	被修改的角色属性，<see cref="KDB_ROLE_PROPERTIES"/> 。
/// </param>
/// <param name="ChangeType">
/// 	更改类型，<see cref="KDB_ITEM_CHANGE_TYPE"/> 。
/// </param>
/// <returns>
///		成功时返回KERR_OK，失败时返回相应的错误代码。
/// </returns>
typedef KDB_RET ( CALLBACK * KDB_ROLE_PROPERTIES_CALLBACK_FUNCTION)(
	KDB_HANDLE				DBHandle, 
	KDB_PTR					UserParameter,
	PKDB_ROLE_PROPERTIES	RoleProperties,
	KDB_ITEM_CHANGE_TYPE	ChangeType );


/// <summary> 
///		用户角色所属关系更新通知回调函数。
/// </summary> 
/// <param name="DBHandle">
///		连接句柄。
/// </param>
/// <param name="UserParameter">
///		自定义参数。
/// </param>
/// <param name="RoleName">
///		角色名称。
/// </param>
/// <param name="NumberOfUsers">
///		用户数目。
/// </param>
/// <param name="UserNames">
///		用户名称数组。
/// </param>
/// <param name="ChangeType">
///		更改类型，<see cref="KDB_ITEM_CHANGE_TYPE"/> 。
/// </param>
/// <returns>
///		成功时返回KERR_OK，失败时返回相应的错误代码。
/// </returns>
typedef KDB_RET (CALLBACK *KDB_USER_ROLE_INFO_CALLBACK_FUNCTION)(
	KDB_HANDLE				DBHandle, 
	KDB_PTR					UserParameter,
	KDB_CWSTR				RoleName,
	KDB_UINT32				NumberOfUsers,
	KDB_WSTR_ARRAY			UserNames,
	KDB_ITEM_CHANGE_TYPE	ChangeType);


//==============================================================================
// 
// 公共接口函数定义
// 
//==============================================================================

	/// <summary> 
	/// 	初始化API库。
	/// </summary> 
	/// <param name="Flags">
	/// 	初始化选项（保留，应始终为0）。
	/// </param>
	/// <returns>
	/// 	KERR_OK表示初始化成功，其他值表示初始化失败。
	/// </returns>
	/// <seealso cref="KDBAPICleanup"/> 
	KDB_RET KDBAPI KDBAPIStartup( KDB_UINT32 Flags );

	/// <summary> 
	///		关闭客户端与服务器的所有连接，并释放相关的资源。
	/// </summary> 
	/// <returns>
	///		KERR_OK表示成功，其他值表示出现错误。
	/// </returns>
	/// <seealso cref="KDBAPIStartup"/> 
	KDB_RET KDBAPI KDBAPICleanup();


	/// <summary> 
	/// 	启用/关闭跟踪调试功能。
	/// </summary> 
	/// <param name="EnableTrace">
	/// 	启用或者关闭调试功能标志。
	/// </param>
	/// <param name="LogFileName">
	/// 	启用跟踪调试功能时，调试信息输出的目标文件路径；如果为NULL，则使用默认的
	///	文件名及路径。默认的日志文件路径与可执行文件位于同一目录下，默认日志文件名为
	/// 可执行文件名_KRTDBAPI.log。
	/// </param>
	KDB_VOID KDBAPI KDBAPITrace( KDB_BOOLEAN EnableTrace , KDB_CWSTR LogFileName );	


	/// <summary> 
	///		获得KRTDBAPI的版本信息。
	/// </summary> 
	/// <param name="Version">
	///		用于存储版本信息的缓冲区。
	/// </param>
	/// <param name="Length">
	///		缓冲区长度。
	/// </param>
	KDB_VOID KDBAPI KDBAPIVersion( KDB_WSTR Version,ULONG Length );

 
//==============================================================================
// 
// 辅助函数
// 
//==============================================================================

	/// <summary> 
	/// 	时间戳转换到文件时间。
	/// </summary> 
	/// <param name="UTCTime">
	/// 	时间戳，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <param name="FileTime">
	/// 	文件时间（UTC）。
	/// </param>
	/// <returns>
	/// 	成功时返回KDB_TRUE，否则返回KDB_FALSE。
	/// </returns>
	/// <seealso cref="KDBUtilFileTimeToTimeStamp"/> 
	KDB_BOOLEAN KDBAPI KDBUtilTimeStampToFileTime( KDB_TIMESTAMP *UTCTime, FILETIME *FileTime );


	/// <summary> 
	/// 	文件时间到时间戳的转换。
	/// </summary> 
	/// <param name="FileTime">
	/// 	文件时间（UTC）。
	/// </param>
	/// <param name="UTCTime">
	/// 	时间戳，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KDB_TRUE，否则返回KDB_FALSE。
	/// </returns>
	/// <remarks>
	///     注意：FileTime的时间范围必须位于1970-1-1至2106-2-7之间，否则将产生不正确的结果。
	/// </remarks>
	/// <seealso cref="KDBUtilTimeStampToFileTime"/> 
	KDB_BOOLEAN KDBAPI KDBUtilFileTimeToTimeStamp( FILETIME *FileTime, KDB_TIMESTAMP *UTCTime );

	
	/// <summary> 
	/// 	获得客户端系统当前的时间戳。
	/// </summary> 
	/// <param name="UTCTime">
	/// 	时间戳，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KDB_TRUE，否则返回KDB_FALSE。
	/// </returns>
	KDB_BOOLEAN KDBAPI KDBUtilGetCurrentTimeStamp( KDB_TIMESTAMP *UTCTime );


	/// <summary> 
	/// 	系统时间（客户端时区）转换到时间戳。
	/// </summary> 
	/// <param name="LocalSystemTime">
	/// 	客户端本地系统时间。
	/// </param>
	/// <param name="UTCTime">
	/// 	时间戳，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KDB_TRUE，否则返回KDB_FALSE。
	/// </returns>
	/// <remarks>
	///     注意：SystemTime的时间范围必须位于1970-1-1至2106-2-7之间，否则将产生不正确的结果。
	/// </remarks>
	/// <seealso cref="KDBUtilTimeStampToSystemTime"/> 
	KDB_BOOLEAN KDBAPI KDBUtilSystemTimeToTimeStamp( SYSTEMTIME* LocalSystemTime, KDB_TIMESTAMP *UTCTime );

	/// <summary> 
	/// 	时间戳转换到系统时间（客户端时区）。
	/// </summary> 
	/// <param name="UTCTime">
	/// 	时间戳，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <param name="LocalSystemTime">
	/// 	客户端本地系统时间。
	/// </param>
	/// <returns>
	/// 	成功时返回KDB_TRUE，否则返回KDB_FALSE。
	/// </returns>
	/// <seealso cref="KDBUtilSystemTimeToTimeStamp"/> 
	KDB_BOOLEAN KDBAPI KDBUtilTimeStampToSystemTime(  KDB_TIMESTAMP *UTCTime , SYSTEMTIME* LocalSystemTime);


	/// <summary> 
	/// 	时间戳转换为字符串形式。
	/// </summary> 
	/// <param name="UTCTime">
	/// 	时间戳，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <param name="LocalStrTime">
	/// 	用于保存结果的字符串，结果格式为YYYY-MM-DD HH:mm:SS.FFF（客户端本地时间，不少于24个字符，由调用方保证）。 
	/// </param>
	/// <returns>
	/// 	成功时返回KDB_TRUE，否则返回KDB_FALSE。
	/// </returns>
	/// <seealso cref="KDBUtilUnicodeStringToTimeStamp"/> 
	KDB_BOOLEAN KDBAPI KDBUtilTimeStampToUnicodeString( KDB_TIMESTAMP *UTCTime , KDB_WSTR LocalStrTime );

	
	/// <summary> 
	/// 	字符串转换为时间戳。
	/// </summary> 
	/// <param name="LocalStrTime">
	/// 	字符串形式表示的时间信息（客户端本地时间），日期格式：YYYY-MM-DD HH:mm:ss:ms。
	/// </param>
	/// <param name="UTCTime">
	/// 	时间戳，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KDB_TRUE，否则返回KDB_FALSE。
	/// </returns>
	/// <remarks>
	///     注意：StrTime的时间范围必须位于1970-1-1至2106-2-7之间，否则将产生不正确的结果。
	/// </remarks>
	/// <seealso cref="KDBUtilTimeStampToUnicodeString"/> 
	KDB_BOOLEAN KDBAPI KDBUtilUnicodeStringToTimeStamp( KDB_CWSTR LocalStrTime , KDB_TIMESTAMP *UTCTime );

	/// <summary> 
	/// 	ANSI字符串转换为Unicode字符串。
	/// </summary> 
	/// <param name="ANSIStr">
	/// 	ANSI字符串。
	/// </param>
	/// <param name="UnicodeStr">
	/// 	Unicode字符串。
	/// </param>
	/// <remarks>
	///     UnicodeStr必须预先分配足够的缓冲区（不少于ANSIStr长度＋1个字符）。
	/// </remarks>
	/// <seealso cref="KDBUtilUnicodeToAnsi"/> 
	KDB_VOID KDBAPI KDBUtilAnsiToUnicode( KDB_CSTR ANSIStr, KDB_WSTR UnicodeStr );


	/// <summary> 
	/// 	Unicode字符串转换为ANSI字符串。
	/// </summary> 
	/// <param name="UnicodeStr">
	/// 	Unicode字符串。
	/// </param>
	/// <param name="ANSIStr">
	/// 	ANSI字符串。
	/// </param>
	/// <remarks>
	///     ANSIStr必须预先分配足够的缓冲区（不少于UnicodeStr长度*2+2个字符）。
	/// </remarks>
	/// <seealso cref="KDBUtilAnsiToUnicode"/> 
	KDB_VOID KDBAPI KDBUtilUnicodeToAnsi( KDB_CWSTR UnicodeStr , KDB_STR ANSIStr);


	/// <summary> 
	/// 	获得错误描述。
	/// </summary> 
	/// <param name="ErrorCode">
	/// 	错误代码。
	/// </param>
	/// <param name="ErrorDescription">
	/// 	错误描述。
	/// </param>
	/// <param name="Length">
	/// 	错误描述缓冲区大小。
	/// </param>
	KDB_VOID KDBAPI KDBUtilGetErrorDescription( KDB_RET ErrorCode, KDB_WSTR ErrorDescription, KDB_UINT32 Length );


	/// <summary> 
	/// 	初始化可变类型数据值。
	/// </summary> 
	/// <param name="DataValue">
	/// 	可变类型数据值，<see cref="KDB_VALUE"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBUtilValueClear"/> 
	KDB_RET KDBAPI KDBUtilValueInit( PKDB_VALUE DataValue );

	
	/// <summary> 
	/// 	清除可变类型数据值内部分配的资源。
	/// </summary> 
	/// <param name="DataValue">
	/// 	可变类型数据值，<see cref="KDB_VALUE"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBUtilValueInit"/> 
	KDB_RET KDBAPI KDBUtilValueClear( PKDB_VALUE DataValue );

	
	/// <summary> 
	/// 	复制可变类型数据。
	/// </summary> 
	/// <param name="TargetValue">
	/// 	目标数据，<see cref="KDB_VALUE"/>。
	/// </param>
	/// <param name="SourceValue">
	/// 	源数据，<see cref="KDB_VALUE"/>。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBUtilValueChangeType"/> 
	KDB_RET KDBAPI KDBUtilValueCopy( PKDB_VALUE TargetValue , PKDB_VALUE SourceValue );

	/// <summary> 
	///		释放字符串数组。	
	/// </summary> 
	/// <param name="StringArray">
	///		待释放的字符串数组。
	/// </param>
	KDB_VOID KDBAPI KDBUtilFreeStringArray( PKDB_STRING_ARRAY StringArray );


	/// <summary> 
	///		释放整数数组。
	/// </summary> 
	/// <param name="IntArray">
	///		待释放整数数组。
	/// </param>
	KDB_VOID KDBAPI KDBUtilFreeIntArray( PKDB_INT_ARRAY IntArray );

	
	/// <summary> 
	///		复制宽字节字符串。
	/// </summary> 
	/// <param name="Source">
	///		源字符串。
	/// </param>
	/// <returns>
	///		复制后的字符串。
	/// </returns>
	/// <remarks>
	///     需要调用KDBUtilUnicodeStringFree释放结果字符串。
	/// </remarks>
	KDB_WSTR KDBAPI KDBUtilUnicodeStringDuplicate( KDB_CWSTR Source );

	/// <summary> 
	///		释放字符串。
	/// </summary> 
	/// <param name="Source">
	///		待释放字符串。
	/// </param>
	KDB_VOID KDBAPI KDBUtilUnicodeStringFree( KDB_WSTR Source );


//==============================================================================
// 
/// 服务器相关接口函数
// 
//==============================================================================
	

	/// <summary> 
	/// 	连接到数据库服务器。
	/// </summary> 
	/// <param name="ConnectionOption">
	/// 	连接参数，<see cref="KDB_CONNECTION_OPTION"/> 。
	/// </param>
	/// <param name="DBHandle">
	/// 	[out]用于保存返回的连接句柄。
	/// </param>
	/// <returns>
	/// 	如果成功连接到数据库服务器，则函数返回KERR_OK；否则，返回相应的错误码。
	/// </returns>
	/// <remarks> 
	/// 	KDBServerConnect函数是同步调用，该函数将一直等待，直到超时或者服务器
	/// 	返回登录成功或者失败的消息，超时参数可通过ConnectionOption中的网络超时
	///		来控制。
	/// </remarks> 
	/// <example>
	///		<code>
	///			KDB_CONNECTION_OPTION connOption	= { 0 };
	///			connOption.ServerName				= KWSTR( "HistorySrv" );
	///			connOption.UserName					= KWSTR( "Admin" );
	///			connOption.Password					= KWSTR( "beijing2008" );
	///			connOption.ApplicationName			= KWSTR( "SQL Assistant" );
	///			connOption.ClientName				= KWSTR( "Sales" );
	///			connOption.NetworkTimeout			= 0; // 使用缺省超时参数
	///			connOption.ConnectionFlags			= KCOF_PROTOCOL_TCPIP;	
	///
	///			KDB_HANDLE	DBHandle = NULL;
	///			KDB_RET		ret = KDBServerConnect( &connOption , &DBHandle );
	///			if( KER(ret) )
	///			{
	///				wprintf( KWSTR( "Oops!Can't connect to database server!\n") );
	///				return 0;
	///			}
	///		</code>
	/// </example>
	/// <seealso cref="KDBServerDisconnect"/> 
	KDB_RET	KDBAPI KDBServerConnect( PKDB_CONNECTION_OPTION  ConnectionOption , KDB_HANDLE* DBHandle );


	/// <summary> 
	/// 	断开客户端与服务器之间的连接，并释放相关的资源。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <returns>
	/// 	KERR_OK表示成功，其他值表示出现了错误。
	/// </returns>
	/// <remarks> 
	/// 	调用KDBServerDisconnect函数之后，DBHandle句柄不能再被使用。一种安全的做法是在调用
	/// 	KDBServerDisconnect函数之后，将DBHandle句柄置为NULL，这样可以防止被释放的DBHandle句柄
	///		被误用。
	/// </remarks> 
	/// <example>
	///		<code>
	///			KDB_RET ret = KDBServerDisconnect( DBHandle ); 			
	///			DBHandle = NULL;
	///		</code>
	/// </example>
	/// <seealso cref="KDBServerConnect"/> 
	KDB_RET		KDBAPI KDBServerDisconnect( KDB_HANDLE DBHandle );

	/// <summary> 
	/// 	判断是否已经成功连接到服务器。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	[in]连接句柄。
	/// </param>
	/// <returns>
	/// 	如果已经成功连接到服务器，则返回KDB_TRUE；否则，返回KDB_FALSE。
	/// </returns>
	/// <example>
	///		<code>
	///			KDB_BOOLEAN bIsConnected = KDBServerIsConnected( DBHandle );
	///			if( bIsConnected ) 
	///				wprintf( KWSTR( "Connected!\n" ) );
	///			else
	///				wprintf( KWSTR( "NOT Connected!\n" ) );
	///		</code>
	/// </example>
	/// <seealso cref="KDBServerConnect"/> 
	/// <seealso cref="KDBServerDisconnect"/> 
	KDB_BOOLEAN	KDBAPI KDBServerIsConnected( KDB_HANDLE DBHandle );


	/// <summary> 
	/// 	获得服务器的时间。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CurrentTime">
	/// 	用于返回服务器的时间。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBServerGetTime(
		KDB_HANDLE				DBHandle, 
		KDB_TIMESTAMP*			CurrentTime);



//==============================================================================
// 
// 历史数据相关接口定义
// 
//==============================================================================

//==============================================================================
// 
// 变量数据相关的接口函数
// 
//==============================================================================

	/// <summary> 
	/// 	向数据库服务器添加（插入）变量历史数据。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="DataRecordsets">
	/// 	变量记录集，<see cref="KDB_DATA_RECORDSETS"/> 。
	/// </param>
	/// <param name="WaitForReply">
	/// 	是否等待数据库返回确认信息。
	/// </param>
	/// <returns>
	/// 	成功返回KERR_OK，失败返回相应的错误码。
	/// </returns>
	/// <example>
	///		插入一个变量的历史数据：
	///		<code>
	///				KDB_DATA_RECORDSETS recordsets	= { 0 };
	///				KDB_DATA_RECORDSET	recordset	= { 0 };
	///				recordsets.NumberOfTags			= 1;
	///				recordsets.DataRecordset		= &recordset;
	///				recordset.TagName				= KWSTR("WATER_SWITCH");
	///				recordset.NumberOfRecords		= 1;
	///				recordset.DataRecords			= &DataProperties;
	///				KDB_RET ErrorCode = KERR_OK;
	///				KDB_RET	ret = KDBDataAdd(
	///					DBHandle,
	///					&recordsets,
	///					KDB_TRUE );
	///				if( KOK( ret ) && KOK( ErrorCode ) )
	///					wprintf( KWSTR("Insert data ok!\n") );
	///				else
	///					wprintf( KWSTR("Insert data failed,error code: %d\n") , ErrorCode );
	///		</code>
	/// </example>
	KDB_RET KDBAPI KDBDataAdd( 
		KDB_HANDLE				DBHandle, 
		PKDB_DATA_RECORDSETS	DataRecordsets,
		KDB_BOOLEAN				WaitForReply );


	/// <summary> 
	///		根据变量ID向服务器插入数据。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	///		变量数目。
	/// </param>
	/// <param name="TagIds">
	///		变量ID数组。
	/// </param>
	/// <param name="TagValues">
	///		变量记录数组（每个变量一个记录）。
	/// </param>
	/// <param name="ErrorStatuses">
	///		用于保存返回码的数组（每个变量一个返回码）。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBDataAddById(
		KDB_HANDLE				DBHandle, 
		KDB_UINT32				NumberOfTags,
		KDB_INT32*				TagIds,
		PKDB_DATA_PROPERTIES	TagValues,
		KDB_RET*				ErrorStatuses );


	/// <summary> 
	///		更新数据源的变量数据（数据回写）。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	///		变量数目。
	/// </param>
	/// <param name="TagNames">
	///		变量名称数组。
	/// </param>
	/// <param name="TagValues">
	///		变量值数组（每个变量一个值）。
	/// </param>
	/// <param name="ErrorStatuses">
	///		用于保存返回码的数组（每个变量一个返回码）。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBDataWriteBack(
		KDB_HANDLE				DBHandle,
		KDB_UINT32				NumberOfTags,
		KDB_WSTR_ARRAY			TagNames,
		PKDB_VALUE				TagValues,
		KDB_RET*				ErrorStatuses );


	/// <summary> 
	/// 	删除多个变量一段时间内的所有数据。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	数据库连接句柄。
	/// </param>
	/// <param name="StartTime">
	/// 	起始时间，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <param name="EndTime">
	/// 	终止时间，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="TagNames">
	/// 	变量名称数组。
	/// </param>
	/// <param name="ErrorStatuses">
	/// 	用于保存返回的错误码的数组。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	该接口相当于OPCHDA 1.2中的IOPCHDA_SyncUpdate::DeleteRaw接口。
	/// </remarks> 
	/// <seealso cref="KDBDataDeleteSingle"/> 
	KDB_RET KDBAPI KDBDataDelete(
		KDB_HANDLE				DBHandle, 
		PKDB_TIMESTAMP			StartTime,
		PKDB_TIMESTAMP			EndTime,
		KDB_UINT32				NumberOfTags,
		KDB_WSTR_ARRAY			TagNames,
		KDB_RET*				ErrorStatuses);


	/// <summary> 
	/// 	删除单个变量的记录。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	数据库连接句柄。
	/// </param>
	/// <param name="TagName">
	/// 	变量名称。
	/// </param>
	/// <param name="NumberOfRecords">
	/// 	待删除的记录个数。
	/// </param>
	/// <param name="DataTimestamps">
	/// 	待删除的数据记录时间戳。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	删除操作将删除对应时间戳的所有版本的数据。
	/// </remarks> 
	/// <seealso cref="KDBDataDelete"/> 
	KDB_RET KDBAPI KDBDataDeleteSingle(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				TagName,
		KDB_UINT32				NumberOfRecords,
		PKDB_TIMESTAMP			DataTimestamps );

	/// <summary> 
	/// 	检索变量的实时数据。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="TagNames">
	/// 	变量名数组。
	/// </param>
	/// <param name="DigitalAsString">
	/// 	是否以字符串方式返回Digital类型。
	/// </param>
	/// <param name="DataProperties">
	/// 	数据属性数组，<see cref="KDB_DATA_PROPERTIES"/> 。
	/// </param>
	/// <param name="ErrorStatuses">
	/// 	错误状态数组。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBDataFreeCurrentValue"/> 
	KDB_RET KDBAPI KDBDataGetCurrentValue(
		KDB_HANDLE				DBHandle, 
		KDB_UINT32				NumberOfTags, 
		KDB_WSTR_ARRAY			TagNames, 
		KDB_BOOLEAN				DigitalAsString,
		PKDB_DATA_PROPERTIES	DataProperties,	
		KDB_RET*				ErrorStatuses);

	/// <summary> 
	/// 	检索给定时间后变量的实时数据。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="DateTime">
	/// 	给定的时刻
	/// </param>
	/// <param name="DigitalAsString">
	/// 	是否以字符串方式返回Digital类型。
	/// </param>
	/// <param name="DataRecordsets">
	/// 	变量数据记录集
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBDataFreeCurrentValue"/> 
	KDB_RET KDBAPI KDBDataGetCurrentValueAfter(
		KDB_HANDLE				DBHandle, 
		PKDB_TIMESTAMP			DateTime,
		KDB_BOOLEAN				DigitalAsString,
		PKDB_DATA_RECORDSETS	DataRecordsets );
	
	/// <summary> 
	/// 	释放数据属性内部分配的内存。
	/// </summary> 
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="DataProperties">
	/// 	数据属性数组，<see cref="KDB_DATA_PROPERTIES"/> 。
	/// </param>
	/// <seealso cref="KDBDataGetCurrentValue"/> 
	KDB_VOID KDBAPI KDBDataFreeCurrentValue(
		KDB_UINT32				NumberOfTags, 
		PKDB_DATA_PROPERTIES	DataProperties );
	
	/// <summary> 
	/// 	检索变量数据。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="DataCriteria">
	/// 	数据检索条件，<see cref="KDB_DATA_CRITERIA"/> 。
	/// </param>
	/// <param name="DataRecordsets">
	/// 	数据记录集集合，<see cref="KDB_DATA_RECORDSETS"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <example>
	///		检索变量Tag1和Tag2的的数据，采样间隔为1秒，每个采样区间内取最大、最小两个值：
	///		<code>
	///				// 构造检索条件
	///				KDB_DATA_CRITERIA criteria ={ 0 };
	///				ZeroMemory( &criteria,sizeof( KDB_DATA_CRITERIA ) ) ;
	///				KDB_WSTR TagNames[] = {
	///						KWSTR("Tag00001"),KWSTR("Tag00002"),
	///						KWSTR("Tag00003"),KWSTR("Tag00004"),
	///						KWSTR("Tag00005"),
	///				};
	///				criteria.NumberOfTags = 5;
	///				criteria.TagNames     = TagNames;
	///				KDBUtilUnicodeStringToTimeStamp( KWSTR("2005-09-30 15:00:00" ) , &criteria.StartTime );	
	///				KDBUtilUnicodeStringToTimeStamp( KWSTR("2005-10-20 00:00:00" ) , &criteria.EndTime );
	///				criteria.DataVersion		= KDAV_ALL;
	///				criteria.SamplingMode		= KSAM_RAW_BY_TIME;
	///				criteria.RowCount			= 1000000;
	///
	///				// 检索数据
	///				KDB_DATA_RECORDSETS	Recordsets = { 0 } ;
	///				DWORD StartTick = GetTickCount();
	///				KDB_RET ret = KDBDataOpenRecordset( 
	///					m_hClient,
	///					&criteria,
	///					&Recordsets );
	///				DWORD EndTick = GetTickCount();
	///				if( KER(ret) )
	///				{
	///					wprintf( KWSTR("KDBDataOpenRecordset failed, error code : %d\n") , ret);
	///					return;
	///				}
	///				else 
	///				{
	///					wprintf( KWSTR( "KDBDataOpenRecordset : %d\n" ), EndTick - StartTick );
	///				}
	///
	///				// 输出检索结果
	///				for( KDB_UINT32 index = 0; index < 5; index++ )
	///				{
	///					if( KER(Recordsets.DataRecordset[index].ErrorStatus) )
	///					{
	///						WCHAR ErrorString[256] = { 0 };
	///						KDBUtilGetErrorDescription( Recordsets.DataRecordset[index].ErrorStatus,ErrorString,256 );
	///						wprintf( KWSTR("Search data of tag %d failed, error code: %d %s\n") ,
	///							index+1 ,Recordsets.DataRecordset[index].ErrorStatus,ErrorString );
	///						continue;
	///					}
	///
	///					// 显示变量记录集的最后一条记录
	///					PKDB_DATA_RECORDSET pDataRecordset = &Recordsets.DataRecordset[index] ;
	///					wprintf( KWSTR("TagName:%s %d\n") , pDataRecordset->TagName,pDataRecordset->NumberOfRecords );
	///					PKDB_DATA_PROPERTIES pDataProperties = &pDataRecordset->DataRecords[pDataRecordset->NumberOfRecords-1];
	///					WCHAR TimeString[256] = { 0 };
	///					KDBUtilTimeStampToUnicodeString( &pDataProperties->TimeStamp,TimeString );
	///					wprintf( KWSTR("Timestamp: %s\tValue: %08.3f\tQuality: %d\n") ,
	///						TimeString,
	///						pDataProperties->Value.r4Val,
	///						pDataProperties->Quality );
	///				}
	///
	///				// 关闭记录集
	///				KDBDataCloseRecordset( &Recordsets );
	///
	///		</code>
	/// </example>
	/// <seealso cref="KDBDataCloseRecordset"/> 
	KDB_RET	KDBAPI  KDBDataOpenRecordset(
		KDB_HANDLE				DBHandle, 
		PKDB_DATA_CRITERIA		DataCriteria,
        PKDB_DATA_RECORDSETS	DataRecordsets);

	
	/// <summary> 
	/// 	释放记录集内部分配的资源。
	/// </summary> 
	/// <param name="DataRecordsets">
	/// 	记录集集合，<see cref="KDB_DATA_RECORDSETS"/> 。
	/// </param>
	/// <seealso cref="KDBDataOpenRecordset"/> 
	KDB_VOID KDBAPI	KDBDataCloseRecordset(  
		PKDB_DATA_RECORDSETS	DataRecordsets);
	
	
	
	/// <summary> 
	/// 	订阅数据改变事件通知。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="TagName">
	/// 	变量名。
	/// </param>
	/// <param name="MinimumElapsedTime">
	/// 	最小间隔时间，单位为毫秒。
	/// </param>
	/// <param name="Subscribe">
	/// 	订阅或者取消订阅标志。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBDataSubscribe函数与KDBDataRegisterCallback函数配合使用。
	/// </remarks> 
	/// <seealso cref="KDBDataRegisterCallback"/> 
	KDB_RET KDBAPI  KDBDataSubscribe(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				TagName,
		KDB_UINT32				MinimumElapsedTime, 
		KDB_BOOLEAN				Subscribe);

	/// <summary> 
	/// 	订阅数据改变事件通知。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="TagNames">
	/// 	变量名数组。
	/// </param>
	/// <param name="MinimumElapsedTime">
	/// 	最小间隔时间，单位为毫秒。
	/// </param>
	/// <param name="ErrorStatuses">
	/// 	用于保存错误状态的数组，每个变量一个状态值。
	/// </param>
	/// <param name="Subscribe">
	/// 	订阅或者取消订阅标志。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBDataSubscribe函数与KDBDataRegisterCallback函数配合使用。
	/// </remarks> 
	/// <seealso cref="KDBDataRegisterCallback"/> 
	KDB_RET KDBAPI  KDBDataSubscribeEx(
		KDB_HANDLE				DBHandle,
		KDB_UINT32				NumberOfTags, 
		KDB_WSTR_ARRAY			TagNames, 
		KDB_UINT32				MinimumElapsedTime, 
		KDB_RET*				ErrorStatuses,
		KDB_BOOLEAN				Subscribe);

	
	/// <summary> 
	/// 	注册数据变化事件回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	///		数据变化事件回调函数，<see cref="KDB_DATA_CALLBACK_FUNCTION"/> 。	
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	该函数与KDBDataSubscribe函数配合使用。
	/// </remarks> 
	/// <seealso cref="KDBDataSubscribe"/> 
	KDB_RET KDBAPI KDBDataRegisterCallback (
		KDB_HANDLE					DBHandle, 
		KDB_DATA_CALLBACK_FUNCTION	CallbackFunction, 
		KDB_PTR						UserParameter);



	//==============================================================================
	//
	// 变量数据记录迭代访问接口，在大数据量情况下能够提供更好的性能和更小的工作集
	//
	//==============================================================================

	/// <summary> 
	///		检索变量数据。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DataCriteria">
	///		数据检索条件，<see cref="KDB_DATA_CRITERIA"/> 。
	/// </param>
	/// <param name="RecordsetHandle">
	///		[out]保存返回的变量数据结果集。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI  KDBDataOpenRecordsetHandle(
		KDB_HANDLE						DBHandle, 
		PKDB_DATA_CRITERIA				DataCriteria,
		KDB_DATA_RECORDSET_HANDLE*		RecordsetHandle );

	/// <summary> 
	///		释放记录集内部分配的资源。
	/// </summary> 
	/// <param name="RecordsetHandle">
	///		变量数据结果集句柄。
	/// </param>
	KDB_VOID KDBAPI	KDBDataCloseRecordsetHandle(  
		KDB_DATA_RECORDSET_HANDLE	RecordsetHandle);


	/// <summary> 
	///		获得结果集包含的记录条数和变量数目。
	/// </summary> 
	/// <param name="RecordsetHandle">
	///		变量数据结果集句柄。
	/// </param>
	/// <param name="RecordCount">
	///		用于保存返回的记录条数（如果为NULL则不返回）。
	/// </param>
	/// <param name="TagCount">
	///		用于保存返回的变量数目（如果为NULL则不返回）。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBDataRecordsetHandleGetSize(
		KDB_DATA_RECORDSET_HANDLE	RecordsetHandle,
		KDB_UINT32*					RecordCount,
		KDB_UINT32*					TagCount );



	/// <summary> 
	///		从结果集获得指定的记录信息。
	/// </summary> 
	/// <param name="RecordsetHandle">
	///		变量数据结果集句柄。
	/// </param>
	/// <param name="RecordIndex">
	///		记录索引(注意：从1开始)。
	/// </param>
	/// <param name="DataProperties">
	///		用于保存记录的属性（若为NULL则不返回记录属性）。
	/// </param>
	/// <param name="TagIndex">
	///		用于保存该记录对应的变量索引（若为NULL则不返回）。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remark>
	///     注意：需要使用KDBDataFreeCurrentValue释放DataProperties所分配的内存。
	/// </remark>
	KDB_RET KDBAPI KDBDataRecordsetHandleGetRecord(
		KDB_DATA_RECORDSET_HANDLE			RecordsetHandle,
		KDB_UINT32							RecordIndex,
		PKDB_DATA_PROPERTIES				DataProperties,
		KDB_UINT32*							TagIndex );


	/// <summary> 
	///		从结果集获得指定的变量信息。
	/// </summary> 
	/// <param name="RecordsetHandle">
	///		变量数据结果集句柄。
	/// </param>
	/// <param name="TagIndex">
	///		变量索引(注意：从1开始)。
	/// </param>
	/// <param name="NameLength">
	///		用于保存变量名的缓冲区大小（以字符为单位，若为0则不返回变量名）。
	/// </param>
	/// <param name="TagName">
	///		用于保存变量名的缓冲区（若为NULL则不返回变量名）。
	/// </param>
	/// <param name="TagDigitalSetId">
	///		用于保存变量的数字集ID（若为NULL则不返回）。
	/// </param>
	/// <param name="TagDataType">
	///		用于保存变量的数据类型（若为NULL则不返回变量类型）。
	/// </param>
	/// <param name="TagDataLength">
	///		用于保存变量的数据类型长度（若为NULL则不返回长度信息）。
	/// </param>
	/// <param name="TagRecordStart">
	///		用于保存该变量对应记录的起始索引（若为NULL则不返回）。
	/// </param>
	/// <param name="TagRecordCount">
	///		用于保存该变量的记录数目（若为NULL则不返回）。
	/// </param>
	/// <param name="TagErrorCode">
	///		用于保存该变量的对应的错误码（若为NULL则不返回）。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remark>
	///     注意：NameLength应大于64以避免变量名被截断。
	/// </remark>
	KDB_RET KDBAPI KDBDataRecordsetHandleGetTagInfo(
		KDB_DATA_RECORDSET_HANDLE			RecordsetHandle,
		KDB_UINT32							TagIndex,
		KDB_UINT32							NameLength,
		KDB_WSTR							TagName,
		KDB_INT16*							TagDigitalSetId,
		KDB_INT16*							TagDataType,
		KDB_INT32*							TagDataLength,
		KDB_UINT32*							TagRecordStart,
		KDB_UINT32*							TagRecordCount,
		KDB_INT32*							TagErrorCode );



	
//==============================================================================
// 
// 变量组相关函数
// 
//==============================================================================

	/// <summary> 
	///		添加变量组。
	/// </summary> 
	/// <param name="DBHandle">
	///		变量组名称。
	/// </param>
	/// <param name="GroupProperties">
	///		变量组属性。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBTagGroupDelete"/> 
	KDB_RET KDBAPI KDBTagGroupAdd(
		KDB_HANDLE					DBHandle,
		PKDB_TAG_GROUP_PROPERTIES	GroupProperties );


	/// <summary> 
	/// 	删除变量组。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="GroupID">
	/// 	变量组标识符。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBTagGroupAdd"/> 
	KDB_RET	KDBAPI KDBTagGroupDelete(
		KDB_HANDLE					DBHandle, 
		KDB_UINT32					GroupID );


	/// <summary> 
	///		修改变量组属性。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="GroupProperties">
	///		变量组属性。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBTagGroupGetProperties"/> 
	KDB_RET KDBAPI KDBTagGroupSetProperties(
		KDB_HANDLE					DBHandle,
		PKDB_TAG_GROUP_PROPERTIES	GroupProperties );


	/// <summary> 
	/// 	检索单个变量组的属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="GroupID">
	///		变量组标识符。
	/// </param>
	/// <param name="GroupProperties">
	/// 	变量组属性，<see cref="KDB_TAG_GROUP_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	应使用KDBTagGroupFreeProperties释放变量属性。
	///		GroupID为1时，获得根节点的变量。
	/// </remarks> 
	/// <seealso cref="KDBTagGroupSetProperties"/> 
	/// <seealso cref="KDBTagGroupFreeProperties"/> 
	KDB_RET KDBAPI KDBTagGroupGetProperties(
		KDB_HANDLE					DBHandle, 
		KDB_UINT32					GroupID,
		PKDB_TAG_GROUP_PROPERTIES	GroupProperties );

	/// <summary> 
	/// 	释放变量组属性内部分配的内存。
	/// </summary> 
	/// <param name="GroupProperties">
	/// 	变量组属性，<see cref="KDB_TAG_GROUP_PROPERTIES"/> 。
	/// </param>
	/// <seealso cref="KDBTagGroupGetProperties"/>
	KDB_VOID KDBAPI KDBTagGroupFreeProperties(
		PKDB_TAG_GROUP_PROPERTIES	GroupProperties );

	/// <summary> 
	///		获得子组标识符。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="GroupID">
	///		变量组ID。
	/// </param>
	/// <param name="ChildrenIDs">
	///		子组标识符数组，<see cref="KDB_INT_ARRAY"/> 。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBTagGroupGetChildren(
		KDB_HANDLE					DBHandle, 
		KDB_UINT32					GroupID,
		PKDB_INT_ARRAY				ChildrenIDs );

	/// <summary> 
	///		获得变量组包含的变量。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="GroupID">
	///		变量组ID。
	/// </param>
	/// <param name="TagNames">
	///		变量名称数组，<see cref="KDB_STRING_ARRAY"/> 。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBTagGroupGetTags(
		KDB_HANDLE					DBHandle, 
		KDB_UINT32					GroupID,
		PKDB_STRING_ARRAY			TagNames );

	/// <summary> 
	/// 	添加变量到变量组。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="GroupID">
	/// 	变量组标识符。
	/// </param>
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="TagNames">
	/// 	变量名称数组。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBTagGroupDeleteTags"/> 
	KDB_RET KDBAPI KDBTagGroupAddTags(
		KDB_HANDLE					DBHandle, 
		KDB_UINT32					GroupID,
		KDB_UINT32					NumberOfTags,
		KDB_WSTR_ARRAY				TagNames );

	/// <summary> 
	/// 	从变量组删除变量。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="GroupID">
	/// 	变量组标识符。
	/// </param>
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="TagNames">
	/// 	变量名称数组。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBTagGroupDeleteTags"/> 
	KDB_RET KDBAPI KDBTagGroupDeleteTags(
		KDB_HANDLE					DBHandle, 
		KDB_UINT32					GroupID,
		KDB_UINT32					NumberOfTags,
		KDB_WSTR_ARRAY				TagNames );
    
	/// <summary> 
	/// 	变量组属性变化回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	变量组属性更新回调函数指针，<see cref="KDB_TAG_GROUP_PROPERTIES_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBTagGroupRegisterPropertiesCallback (
		KDB_HANDLE									DBHandle, 
		KDB_TAG_GROUP_PROPERTIES_CALLBACK_FUNCTION	CallbackFunction, 
		KDB_PTR										UserParameter );


	/// <summary> 
	/// 	变量组信息变化回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	变量组属性更新回调函数指针，<see cref="KDB_TAG_GROUP_INFO_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBTagGroupRegisterInfoCallback (
		KDB_HANDLE									DBHandle, 
		KDB_TAG_GROUP_INFO_CALLBACK_FUNCTION		CallbackFunction, 
		KDB_PTR										UserParameter );





//==============================================================================
// 
// 变量操作
// 
//==============================================================================

	/// <summary> 
	/// 	新增变量。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="TagProperties">
	/// 	变量属性，<see cref="KDB_TAG_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBTagDelete"/> 
	KDB_RET	KDBAPI KDBTagAdd(
		KDB_HANDLE					DBHandle, 
		PKDB_TAG_PROPERTIES			TagProperties );


	/// <summary> 
	///		批量增加变量。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	///		变量数目。
	/// </param>
	/// <param name="TagProperties">
	///		变量属性数组，<see cref="KDB_TAG_PROPERTIES"/> 。
	/// </param>
	/// <param name="ErrorStatuses">
	///		用于保存错误状态的数组，每个变量一个状态值。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBTagAddEx(
		KDB_HANDLE					DBHandle, 
		KDB_UINT32					NumberOfTags,
		PKDB_TAG_PROPERTIES			TagProperties,
		KDB_RET*					ErrorStatuses );


	/// <summary> 
	/// 	删除变量。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="TagName">
	/// 	变量名称。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBTagDelete将从数据库服务器中删除变量的配置信息，这意味着已经存储在历史数据库系统中的该变
	/// 	量历史数据将不能再被访问，因此应慎用KDBTagDelete函数。
	/// </remarks> 
	KDB_RET KDBAPI KDBTagDelete(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				TagName);

	
	/// <summary> 
	///		批量删除变量。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	///		变量数目。
	/// </param>
	/// <param name="TagNames">
	///		变量名称数组。
	/// </param>
	/// <param name="ErrorStatuses">
	///		用于保存错误状态的数组，每个变量一个状态值。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBTagDeleteEx(
		KDB_HANDLE				DBHandle, 
		KDB_UINT32				NumberOfTags, 
		KDB_WSTR_ARRAY			TagNames,
		KDB_RET*				ErrorStatuses );



	/// <summary> 
	/// 	检索数据库系统中现有的变量信息。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="TagCriteria">
	/// 	变量检索条件，<see cref="KDB_TAG_CRITERIA"/>，在使用名称作为检索条件的前提下，如果TagNameMask
	///		不为空，则使用TagNameMask作为检索条件；否则，使用变量名称数组TagNames中指定的变量名称作为检索
	///		条件。
	/// </param>
	/// <param name="TagFields">
	/// 	要查询的变量域（NULL表示全部域），<see cref="KDB_TAG_FIELDS"/> 。
	/// </param>
	/// <param name="TagRecordset">
	/// 	变量记录集，<see cref="KDB_TAG_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// </remarks> 
	/// <example>
	///		如下示例检索名称以SIM为前缀的所有变量属性：
	///		<code>
	///			
	/// 		KDB_TAG_CRITERIA	criteria = { 0 };
	/// 		KDB_TAG_RECORDSET	recordset;
	/// 
	/// 		// 使用变量名作为检索条件
	/// 		criteria.TagNameMask	= KWSTR("SIM*");
	/// 
	/// 		// 检索变量属性
	/// 		KDB_RET ret = KDBTagOpenRecordset(
	/// 						DBHandle,
	/// 						&criteria,
	///							NULL,
	/// 						&recordset);
	/// 		if( KER(ret) )
	/// 		{
	/// 			wprintf( KWSTR("KDBTagOpenRecordset failed,error code:%d\n"),ret );
	/// 			return 0;
	/// 		}
	/// 
	/// 		// 显示变量属性
	/// 		for( KDB_UINT32 index =0; index < recordset.NumberOfRecords; index++ )
	/// 		{
	/// 			PKDB_TAG_PROPERTIES pTagProperties = &recordset.TagRecords[index];
	/// 			wprintf( KWSTR("TagName:%s\n Description:%s\n DataType:%d\n CollectorName:%s\n"),
	/// 				pTagProperties->TagName,
	/// 				pTagProperties->Description,
	/// 				pTagProperties->DataType,
	/// 				pTagProperties->CollectorName);
	/// 		}
	/// 
	/// 		// 关闭记录集，释放资源
	/// 		KDBTagCloseRecordset( &recordset );
	/// 
	/// 	</code>
	///		上述查询等价于以下的SQL查询：
	///		<code>
	///			SELECT TagName,Description,DataType,CollectorName FROM Tag WHERE TagName LIKE 'SIM*'
	///		</code>
	/// </example>
	/// <seealso cref="KDBTagCloseRecordset"/> 
	KDB_RET KDBAPI KDBTagOpenRecordset(
		KDB_HANDLE					DBHandle, 
		PKDB_TAG_CRITERIA			TagCriteria, 
		PKDB_TAG_FIELDS				TagFields, 
		PKDB_TAG_RECORDSET			TagRecordset);

	
	/// <summary> 
	/// 	关闭变量数据集并释放资源。
	/// </summary> 
	/// <param name="TagRecordset">
	/// 	变量记录集，<see cref="KDB_TAG_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBTagOpenRecordset"/> 
	KDB_RET KDBAPI KDBTagCloseRecordset(
		PKDB_TAG_RECORDSET			TagRecordset);

	/// <summary> 
	///		复制变量数据集。
	/// </summary> 
	/// <param name="SourceRecordset">
	///		源数据集。
	/// </param>
	/// <param name="TargetRecordset">
	///		目标数据集。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	应使用KDBTagCloseRecordset释放TargetRecordset。
	/// </remarks> 
	/// <seealso cref="KDBTagFreeProperties"/> 
	KDB_RET KDBAPI KDBTagCopyRecordset(
		PKDB_TAG_RECORDSET			SourceRecordset,
		PKDB_TAG_RECORDSET			TargetRecordset );



	/// <summary> 
	/// 	清空变量属性域。
	/// </summary> 
	/// <param name="TagFields">
	/// 	变量属性域，<see cref="KDB_TAG_FIELDS"/> 。
	/// </param>
	/// <seealso cref="KDBTagOpenRecordset"/> 
	KDB_VOID KDBAPI KDBTagClearAllFields(
		PKDB_TAG_FIELDS				TagFields );
	
	/// <summary> 
	/// 	检查变量是否存在。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="TagName">
	/// 	变量名称。
	/// </param>
	/// <returns>
	/// 	KERR_OK表示变量存在，KERR_NOT_FOUND表示变量不存在，其他值表示出现了错误。
	/// </returns>
	KDB_RET KDBAPI KDBTagExists( 
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				TagName );

	/// <summary> 
	/// 	检索单个变量属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="TagName">
	/// 	变量名称。
	/// </param>
	/// <param name="TagFields">
	/// 	要查询的变量域（NULL表示全部域），<see cref="KDB_TAG_FIELDS"/> 。
	/// </param>
	/// <param name="TagProperties">
	/// 	变量属性，<see cref="KDB_TAG_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	应使用KDBTagFreeProperties释放变量属性结构内部分配的内存。
	/// </remarks> 
	/// <seealso cref="KDBTagSetProperties"/> 
	/// <seealso cref="KDBTagFreeProperties"/> 
	KDB_RET KDBAPI KDBTagGetProperties(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				TagName, 
		PKDB_TAG_FIELDS			TagFields, 
		PKDB_TAG_PROPERTIES		TagProperties);

	/// <summary> 
	///		根据变量ID获得变量属性。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="TagId">
	///		变量ID。
	/// </param>
	/// <param name="TagFields">
	/// 	要查询的变量域（NULL表示全部域），<see cref="KDB_TAG_FIELDS"/> 。
	/// </param>
	/// <param name="TagProperties">
	///		变量属性。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBTagGetPropertiesById(
		KDB_HANDLE				DBHandle, 
		KDB_INT32				TagId,
		PKDB_TAG_FIELDS			TagFields, 
		PKDB_TAG_PROPERTIES		TagProperties);


	/// <summary> 
	///		根据变量名称获得变量ID。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据量连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	///		变量数目。
	/// </param>
	/// <param name="TagNames">
	///		变量名称数组。
	/// </param>
	/// <param name="TagIds">
	///		用于保存返回的变量ID数组，如果对应项为0，则表示该变量不存在。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBTagGetIds(
		KDB_HANDLE				DBHandle, 
		KDB_UINT32				NumberOfTags, 
		KDB_WSTR_ARRAY			TagNames,
		KDB_INT32*				TagIds );

	/// <summary> 
	///		获得服务器所有的变量名称。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="TagNames">
	///		变量名称数组，<see cref="KDB_STRING_ARRAY"/> 。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBTagGetAllNames(
		KDB_HANDLE				DBHandle,
		PKDB_STRING_ARRAY		TagNames );

	/// <summary> 
	///		获得满足条件的变量名称。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="TagNameMask">
	///		变量名掩码，可以为NULL。
	/// </param>
	/// <param name="DescriptionMask">
	///		变量描述掩码，可以为NULL。
	/// </param>
	/// <param name="CollectorName">
	///		数据采集器名称，可以为NULL。
	/// </param>
	/// <param name="SourceAddress">
	///		数据源地址，可以为NULL。
	/// </param>
	/// <param name="TagNames">
	///		变量名称数组，<see cref="KDB_STRING_ARRAY"/> 。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBTagGetNames(
		KDB_HANDLE					DBHandle, 
		KDB_CWSTR					TagNameMask,
		KDB_CWSTR					DescriptionMask,
		KDB_CWSTR					CollectorName,
		KDB_CWSTR					SourceAddress,
		PKDB_STRING_ARRAY			TagNames );


	/// <summary> 
	/// 	修改变量属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="TagFields">
	/// 	变量属性域，指明哪些域被修改了，<see cref="KDB_TAG_FIELDS"/> 。
	/// </param>
	/// <param name="TagProperties">
	/// 	变量属性，<see cref="KDB_TAG_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBTagGetProperties"/> 
	/// <seealso cref="KDBTagClearAllFields"/>
	KDB_RET KDBAPI KDBTagSetProperties(
		KDB_HANDLE					DBHandle,
		PKDB_TAG_FIELDS				TagFields, 
		PKDB_TAG_PROPERTIES			TagProperties );

	
	/// <summary> 
	///		批量修改变量配置。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	///		变量数目。
	/// </param>
	/// <param name="TagNames">
	///		变量名称数组。
	/// </param>
	/// <param name="TagFields">
	/// 	变量属性域，指明哪些域被修改了，<see cref="KDB_TAG_FIELDS"/> 。
	/// </param>
	/// <param name="TagProperties">
	/// 	变量属性，<see cref="KDB_TAG_PROPERTIES"/> 。
	/// </param>
	/// <param name="ErrorStatuses">
	/// 	用于保存错误状态的数组，每个变量一个状态值。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks>
	///     所有的变量由TagFields指定的域将被设为相同的值。
	/// </remarks>
	KDB_RET KDBAPI KDBTagSetPropertiesEx(
		KDB_HANDLE					DBHandle,
		KDB_UINT32					NumberOfTags, 
		KDB_WSTR_ARRAY				TagNames,
		PKDB_TAG_FIELDS				TagFields, 
		PKDB_TAG_PROPERTIES			TagProperties,
		KDB_RET*					ErrorStatuses );

	

	/// <summary> 
	///		复制变量属性。
	/// </summary> 
	/// <param name="SourceProperties">
	///		源属性。
	/// </param>
	/// <param name="TargetProperties">
	///		目标属性。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	应使用KDBTagFreeProperties释放TargetProperties。
	/// </remarks> 
    /// <seealso cref="KDBTagFreeProperties"/> 
	KDB_RET KDBAPI KDBTagCopyProperties(
		PKDB_TAG_PROPERTIES			SourceProperties,
		PKDB_TAG_PROPERTIES			TargetProperties );

	/// <summary> 
	///		释放变量属性。	
	/// </summary> 
	/// <param name="TagProperties">
	/// 	变量属性，<see cref="KDB_TAG_PROPERTIES"/> 。
	/// </param>
	/// <seealso cref="KDBTagGetProperties"/> 
	KDB_VOID KDBAPI KDBTagFreeProperties(
		PKDB_TAG_PROPERTIES			TagProperties);

	/// <summary> 
	/// 	订阅变量配置变化通知。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="TagName">
	/// 	变量名称。
	/// </param>
	/// <param name="Subscribe">
	/// 	订阅或取消订阅标志。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBTagSubscribeProperties函数与KDBTagRegisterPropertiesCallback函数配合使用，共同完成对变量
	/// 	配置变化的跟踪。
	/// </remarks> 
	/// <seealso cref="KDBTagRegisterPropertiesCallback"/> 
	KDB_RET KDBAPI KDBTagSubscribeProperties(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				TagName, 
		KDB_BOOLEAN				Subscribe );


	/// <summary> 
	/// 	订阅变量配置变化通知。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="TagNames">
	/// 	变量名数组。
	/// </param>
	/// <param name="ErrorStatuses">
	/// 	用于保存错误状态的数组，每个变量一个状态值。
	/// </param>
	/// <param name="Subscribe">
	/// 	订阅或取消订阅标志。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBTagSubscribeProperties函数与KDBTagRegisterPropertiesCallback函数配合使用，共同完成对变量
	/// 	配置变化的跟踪。
	/// </remarks> 
	/// <seealso cref="KDBTagRegisterPropertiesCallback"/> 
	KDB_RET KDBAPI KDBTagSubscribePropertiesEx(
		KDB_HANDLE				DBHandle, 
		KDB_UINT32				NumberOfTags, 
		KDB_WSTR_ARRAY			TagNames,
		KDB_RET*				ErrorStatuses,
		KDB_BOOLEAN				Subscribe );


	/// <summary> 
	/// 	注册变量配置变化回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	变量配置变化回调函数指针，<see cref="KDB_TAG_PROPERTIES_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	客户端程序使用KDBTagRegisterPropertiesCallback注册接收到变量配置变化的通知事件处理函数，当
	/// 	客户端接收到相应的通知时，将自动调用注册的回调函数来处理接收到的通知。
	/// </remarks> 
	/// <seealso cref="KDBTagSubscribeProperties"/> 
	KDB_RET	KDBAPI KDBTagRegisterPropertiesCallback (
		KDB_HANDLE								DBHandle, 
		KDB_TAG_PROPERTIES_CALLBACK_FUNCTION	CallbackFunction, 
		KDB_PTR									UserParameter );


//==============================================================================
// 变量属性结果集操作函数(迭代方式的访问接口)
//==============================================================================

	/// <summary> 
	///		检索变量属性。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="TagCriteria">
	///		属性检索条件，<see cref="KDB_TAG_CRITERIA"/> 。
	/// </param>
	/// <param name="TagFields">
	/// 	要查询的变量域（NULL表示全部域），<see cref="KDB_TAG_FIELDS"/> 。
	/// </param>
	/// <param name="RecordsetHandle">
	///		[out]保存返回的变量属性结果集。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI  KDBTagOpenRecordsetHandle(
		KDB_HANDLE						DBHandle, 
		PKDB_TAG_CRITERIA				TagCriteria,
		PKDB_TAG_FIELDS					TagFields,
		KDB_TAG_RECORDSET_HANDLE*		RecordsetHandle );

	/// <summary> 
	///		释放记录集内部分配的资源。
	/// </summary> 
	/// <param name="RecordsetHandle">
	///		变量属性结果集句柄。
	/// </param>
	KDB_VOID KDBAPI	KDBTagCloseRecordsetHandle(  
		KDB_TAG_RECORDSET_HANDLE	RecordsetHandle);


	/// <summary> 
	///		获得结果集包含的变量数目。
	/// </summary> 
	/// <param name="RecordsetHandle">
	///		变量属性结果集句柄。
	/// </param>
	/// <returns>
	///		成功时返回变量数目，失败时返回0.
	/// </returns>
	KDB_UINT32 KDBAPI KDBTagRecordsetHandleGetSize(
		KDB_TAG_RECORDSET_HANDLE	RecordsetHandle) ;



	/// <summary> 
	///		从结果集获得指定的记录信息。
	/// </summary> 
	/// <param name="RecordsetHandle">
	///		变量属性结果集句柄。
	/// </param>
	/// <param name="TagIndex">
	///		记录索引(注意：从1开始)。
	/// </param>
	/// <param name="TagProperties">
	///		用于保存记录的属性。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remark>
	///     注意：需要使用KDBTagFreeProperties释放TagProperties所分配的内存。
	/// </remark>
	KDB_RET KDBAPI KDBTagRecordsetHandleGetRecord(
		KDB_TAG_RECORDSET_HANDLE			RecordsetHandle,
		KDB_UINT32							TagIndex,
		PKDB_TAG_PROPERTIES					TagProperties);






//==============================================================================
//
// 数据状态集相关接口函数
//
//==============================================================================



	/// <summary> 
	///		枚举所有的数字状态集信息。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetRecordset">
	///		数字状态集记录集。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks>
	///     需要调用KDBDigitalCloseRecordset来释放获得的DigitalSetRecordset记录集。
	/// </remarks>
	KDB_RET	KDBAPI KDBDigitalSetOpenRecordset( 
		KDB_HANDLE				DBHandle, 
		PKDB_DIGITAL_RECORDSET	DigitalSetRecordset );

	/// <summary> 
	///		释放数字状态（集）记录集。
	/// </summary> 
	/// <param name="DigitalRecordset">
	///		待释放的记录集。
	/// </param>
	KDB_VOID KDBAPI KDBDigitalCloseRecordset( PKDB_DIGITAL_RECORDSET DigitalRecordset );

	/// <summary> 
	///		增加一个新的数字状态集。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID，如果为0，则由服务器自动选定，并由ReturnDigitalSetId参数返回。
	///		注意：1～64保留为系统使用，自定义状态集应使用从65开始的ID，否则将可能导致在
	///		系统升级时出现兼容性问题。
	/// </param>
	/// <param name="DigitalSetName">
	///		数字状态集名称。
	/// </param>
	/// <param name="ReturnDigitalSetId">
	///		用于保存返回的实际数字状态集ID。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalSetAdd( 
		KDB_HANDLE			DBHandle,								
		KDB_INT16			DigitalSetId, 
		KDB_CWSTR			DigitalSetName, 
		KDB_INT16*			ReturnDigitalSetId );

	/// <summary> 
	///		删除一个数字状态集。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalSetDelete( 
		KDB_HANDLE			DBHandle,
		KDB_INT16			DigitalSetId );

	/// <summary> 
	///		重命名一个数字状态集。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalSetName">
	///		数字状态集名称。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalSetRename( 
		KDB_HANDLE			DBHandle,	
		KDB_INT16			DigitalSetId, 
		KDB_CWSTR			DigitalSetName );

	/// <summary> 
	///		根据数字状态集ID获得其名称。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalSetName">
	///		数字状态集名称。
	/// </param>
	/// <param name="DigitalSetNameLength">
	///		数字状态集名称缓冲区长度（需至少为65以避免名称被截断）。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalSetGetNameById(
		KDB_HANDLE			DBHandle,
		KDB_INT16			DigitalSetId,
		KDB_WSTR			DigitalSetName,
		KDB_UINT32			DigitalSetNameLength );

	/// <summary> 
	///		根据数字状态集名称获得其ID。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetName">
	///		数字状态集名称。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalSetGetIdByName( 
		KDB_HANDLE			DBHandle,
		KDB_CWSTR			DigitalSetName,
		KDB_INT16*			DigitalSetId );


	/// <summary> 
	///		枚举指定数字状态集的所有状态。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalStateRecordset">
	///		用于保存返回的数字状态记录集。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks>
	///     需要调用KDBDigitalCloseRecordset来释放获得的DigitalStateRecordset记录集。
	/// </remarks>
	KDB_RET	KDBAPI KDBDigitalStateOpenRecordset( 
		KDB_HANDLE				DBHandle,
		KDB_INT16				DigitalSetId, 
		PKDB_DIGITAL_RECORDSET	DigitalStateRecordset );


	/// <summary> 
	///		增加一个数字状态。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalStateId">
	///		数字状态ID。
	/// </param>
	/// <param name="DigitalStateName">
	///		数字状态名称。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalStateAdd( 
		KDB_HANDLE			DBHandle,
		KDB_INT16			DigitalSetId,
		KDB_INT16			DigitalStateId, 
		KDB_CWSTR			DigitalStateName);

	/// <summary> 
	///		删除一个指定的状态。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalStateId">
	///		数字状态ID。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalStateDelete( 
		KDB_HANDLE			DBHandle,
		KDB_INT16			DigitalSetId, 
		KDB_INT16			DigitalStateId );

	/// <summary> 
	///		重命名一个数字状态。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalStateId">
	///		数字状态ID。
	/// </param>
	/// <param name="DigitalStateName">
	///		数字状态名称。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalStateRename( 
		KDB_HANDLE			DBHandle,
		KDB_INT16			DigitalSetId, 
		KDB_INT16			DigitalStateId, 
		KDB_CWSTR			DigitalStateName );

	/// <summary> 
	///		根据数字状态ID查找其名称。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalStateId">
	///		数字状态ID。
	/// </param>
	/// <param name="DigitalStateName">
	///		数字状态名称。
	/// </param>
	/// <param name="DigitalStateNameLength">
	///		数字状态名称缓冲区长度（需要至少为65以避免名称被截断）。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalStateGetNameById( 
		KDB_HANDLE			DBHandle,
		KDB_INT16			DigitalSetId, 
		KDB_INT16			DigitalStateId,
		KDB_WSTR			DigitalStateName,
		KDB_UINT32			DigitalStateNameLength );

	/// <summary> 
	///		根据数字状态名称获得其ID。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalStateName">
	///		数字状态名称。
	/// </param>
	/// <param name="DigitalStateId">
	///		数字状态ID。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBDigitalStateGetIdByName( 
		KDB_HANDLE			DBHandle,
		KDB_INT16			DigitalSetId, 
		KDB_CWSTR			DigitalStateName,
		KDB_INT16*			DigitalStateId );

	/// <summary> 
	///		根据ID得到数字状态代码。
	/// </summary> 
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalStateId">
	///		数字状态ID。
	/// </param>
	/// <returns>
	///		数字状态的32整数表示。
	/// </returns>
	KDB_INT32 KDBAPI KDBDigitalStateCodeFromId( 
		KDB_INT16			DigitalSetId, 
		KDB_INT16			DigitalStateId );

	/// <summary> 
	///		根据数字状态代码得到其状态集及状态ID。
	/// </summary> 
	/// <param name="DigitalStateCode">
	///		数字状态代码。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalStateId">
	///		数字状态ID。
	/// </param>
	KDB_VOID KDBAPI KDBDigitalStateCodeToId( 
		KDB_INT32			DigitalStateCode, 
		KDB_INT16*			DigitalSetId, 
		KDB_INT16*			DigitalStateId  );

	/// <summary> 
	///		根据数字状态代码得到其名称。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalStateCode">
	///		数字状态代码。
	/// </param>
	/// <param name="DigitalStateName">
	///		数字状态名称。
	/// </param>
	/// <param name="DigitalStateNameLength">
	///		数字状态名称缓冲区长度（需要至少为65以避免名称被截断）。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBDigitalStateCodeToName(
		KDB_HANDLE			DBHandle,
		KDB_INT32			DigitalStateCode, 
		KDB_WSTR			DigitalStateName,
		KDB_UINT32			DigitalStateNameLength );

	/// <summary> 
	///		根据数据状态名称得到其对应的状态代码。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DigitalSetId">
	///		数字状态集ID。
	/// </param>
	/// <param name="DigitalStateName">
	///		数字状态名称。
	/// </param>
	/// <param name="DigitalStateCode">
	///		用于保存返回的数字状态代码。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBDigitalStateCodeFromName(
		KDB_HANDLE			DBHandle,
		KDB_INT16			DigitalSetId, 
		KDB_CWSTR			DigitalStateName,
		KDB_INT32*			DigitalStateCode );


	/// <summary> 
	///		获得数字状态集自服务器本次启动后的最近一次修改时间。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="LastModified">
	///		返回自服务器本次启动后，数字状态集的最近一次修改时间。
	///		如果自服务器本次启动后没有修改，则返回1970-1-1。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks>
	///     根据本函数返回结果，客户端程序可检查是否需要更新本地缓存的数字状态集信息。
	/// </remarks>
	KDB_RET KDBAPI KDBDigitalGetLastModified(
		KDB_HANDLE			DBHandle,
		PKDB_TIMESTAMP		LastModified );

	
//==============================================================================
// 
// 数据采集器相关接口函数
// 
//==============================================================================

	/// <summary> 
	/// 	添加数据采集器。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorProperties">
	/// 	数据采集器属性，<see cref="KDB_COLLECTOR_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBCollectorDelete"/> 
	KDB_RET KDBAPI KDBCollectorAdd(
		KDB_HANDLE					DBHandle, 
		PKDB_COLLECTOR_PROPERTIES	CollectorProperties );

	/// <summary> 
	/// 	删除数据采集器。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器名称。
	/// </param>
	/// <param name="DeleteTags">
	/// 	是否删除与该采集器相关联的变量。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBCollectorDelete(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				CollectorName, 
		KDB_BOOLEAN				DeleteTags );

	/// <summary> 
	/// 	检索数据库系统中的数据采集器信息。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorNameMask">
	/// 	数据采集器名称，可以带通配符，也可以为空（NULL）。
	/// </param>
	/// <param name="CollectorRecordset">
	/// 	数据采集器记录集，<see cref="KDB_COLLECTOR_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <example>
	///		检索所有采集器的名称、类型和所在的计算机：
	///		<code>
	///			
	/// 		KDB_COLLECTOR_RECORDSET recordset;
	/// 		KDB_RET ret = KDBCollectorOpenRecordset(
	/// 						DBHandle,
	/// 						NULL,
	/// 						&recordset );
	/// 		if( KER(ret) )
	/// 		{
	/// 			wprintf( KWSTR("KDBCollectorOpenRecordset failed,error code:%d\n") , ret );
	/// 			return 0;
	/// 		}
	/// 		for( KDB_UINT32 index = 0; index < recordset.NumberOfRecords; index++ )
	/// 		{
	/// 			PKDB_COLLECTOR_PROPERTIES pCollectorProperties = &recordset.CollectorRecords[index];
	/// 			wprintf( KWSTR("CollectorName:%s\n CollectorType:%d\n ComputerName:%s\n"),
	/// 					 pCollectorProperties->CollectorName,
	/// 					 pCollectorProperties->CollectorType,
	/// 					 pCollectorProperties->ComputerName);
	/// 		}
	/// 		
	/// 		// 关闭记录集，释放内存
	/// 		KDBCollectorCloseRecordset( &recordset );
	///		
	///		</code>
	/// </example>
	/// <seealso cref="KDBCollectorCloseRecordset"/> 
	KDB_RET KDBAPI KDBCollectorOpenRecordset(
		KDB_HANDLE					DBHandle, 
		KDB_CWSTR					CollectorNameMask, 
		PKDB_COLLECTOR_RECORDSET	CollectorRecordset);
	
	/// <summary> 
	/// 	释放数据采集器记录集。
	/// </summary> 
	/// <param name="CollectorRecordset">
	/// 	数据采集器记录集，<see cref="KDB_COLLECTOR_RECORDSET"/> 。
	/// </param>
	/// <seealso cref="KDBCollectorOpenRecordset"/> 
	KDB_VOID KDBAPI KDBCollectorCloseRecordset(
		PKDB_COLLECTOR_RECORDSET	CollectorRecordset);


	/// <summary> 
	/// 	清除采集器属性域。
	/// </summary> 
	/// <param name="CollectorFields">
	/// 	数据采集器属性域，<see cref="KDB_COLLECTOR_FIELDS"/> 。
	/// </param>
	/// <seealso cref="KDBCollectorOpenRecordset"/> 
	KDB_VOID KDBAPI KDBCollectorClearAllFields(
		PKDB_COLLECTOR_FIELDS		CollectorFields);

	/// <summary> 
	/// 	检索单个数据采集器的属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器名称。
	/// </param>
	/// <param name="CollectorProperties">
	/// 	数据采集器属性，<see cref="KDB_COLLECTOR_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBCollectorOpenRecordset"/> 
	/// <seealso cref="KDBCollectorFreeProperties"/> 
	KDB_RET KDBAPI KDBCollectorGetProperties(
		KDB_HANDLE					DBHandle, 
		KDB_CWSTR					CollectorName, 
		PKDB_COLLECTOR_PROPERTIES	CollectorProperties );

	/// <summary> 
	/// 	修改数据采集器属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorFields">
	/// 	数据采集器属性域，<see cref="KDB_COLLECTOR_FIELDS"/> 。
	/// </param>
	/// <param name="CollectorProperties">
	/// 	数据采集器属性，<see cref="KDB_COLLECTOR_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	不能使用KDBCollectorSetProperties来修改数据采集器名称，亦不提供修改数据采集器名称的接口。
	/// </remarks> 
	/// <seealso cref="KDBCollectorClearAllFields"/> 
	/// <seealso cref="KDBCollectorGetProperties"/>
	KDB_RET KDBAPI KDBCollectorSetProperties(
		KDB_HANDLE					DBHandle, 
		PKDB_COLLECTOR_FIELDS		CollectorFields, 
		PKDB_COLLECTOR_PROPERTIES	CollectorProperties );

	/// <summary> 
	/// 	释放数据采集器属性结构内部分配的内存。
	/// </summary> 
	/// <param name="CollectorProperties">
	/// 	数据采集器属性，<see cref="KDB_COLLECTOR_PROPERTIES"/> 。
	/// </param>
	/// <seealso cref="KDBCollectorGetProperties"/> 
	KDB_VOID KDBAPI KDBCollectorFreeProperties(
		PKDB_COLLECTOR_PROPERTIES	CollectorProperties);
	

	/// <summary> 
	/// 	设置数据采集器的状态。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器的名称。
	/// </param>
	/// <param name="CollectorStatus">
	/// 	数据采集器的状态，<see cref="KDB_COLLECTOR_STATUS"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorSetStatus只应由数据采集器（计算引擎、OPC数据采集器等）调用，
	/// 	其他的客户端程序不应该调用该接口函数。
	///		没有对称的KDBCollectorGetStatus接口函数，数据采集器的状态可以通过KDBCollectorGetProperties
	///		或KDBCollectorOpenRecordset函数获得。
	/// </remarks> 
	/// <seealso cref="KDBCollectorGetProperties"/> 
	/// <seealso cref="KDBCollectorOpenRecordset"/> 
	KDB_RET KDBAPI KDBCollectorSetStatus(
		KDB_HANDLE					DBHandle, 
		KDB_CWSTR					CollectorName, 
		KDB_COLLECTOR_STATUS		CollectorStatus );

	/// <summary> 
	/// 	订阅采集器的属性改变通知。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器名称。
	/// </param>
	/// <param name="Subscribe">
	/// 	订阅或者取消订阅的标志。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	需要监控数据采集器属性变化的应用程序可以通过KDBCollectorSubscribeProperties来订阅采集器属性
	/// 	变化通知事件，当服务器检测到相应数据采集器的属性发生改变时，数据库服务器会主动通知客户端。
	///		KDBCollectorSubscribeProperties函数必须与KDBCollectorRegisterPropertiesCallback函数配合使用，
	///		客户端程序使用KDBCollectorRegisterPropertiesCallback来注册回调函数，当客户端从服务器接收到
	///		采集器属性变化通知时，KRTDBAPI自动调用客户端注册的回调函数处理相应的通知。
	/// </remarks> 
	/// <seealso cref="KDBCollectorRegisterPropertiesCallback"/> 
	KDB_RET KDBAPI  KDBCollectorSubscribeProperties(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				CollectorName, 
		KDB_BOOLEAN				Subscribe );

	/// <summary> 
	/// 	注册属性变化回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	属性变化回调函数指针，<see cref="KDB_COLLECTOR_PROPERTIES_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	用户自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorRegisterPropertiesCallback函数必须与KDBCollectorSubscribeProperties函数配合使用。
	/// </remarks> 
	/// <seealso cref="KDBCollectorSubscribeProperties"/> 
	KDB_RET KDBAPI	KDBCollectorRegisterPropertiesCallback(
		KDB_HANDLE									DBHandle,
		KDB_COLLECTOR_PROPERTIES_CALLBACK_FUNCTION	CallbackFunction,
		KDB_PTR										UserParameter );
	
	/// <summary> 
	/// 	订阅采集器的状态改变通知。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器名称。
	/// </param>
	/// <param name="Subscribe">
	/// 	订阅或者取消订阅的标志。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorSubscribeStatus函数与KDBCollectorSubscribeProperties函数类似，只是它跟踪数据采集
	/// 	器的状态变化而非属性变化，它需要与KDBCollectorRegisterStatusCallback函数配合使用。
	/// </remarks> 
	/// <seealso cref="KDBCollectorRegisterStatusCallback"/> 
	KDB_RET KDBAPI  KDBCollectorSubscribeStatus(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				CollectorName, 
		KDB_BOOLEAN				Subscribe);

	/// <summary> 
	/// 	注册数据采集器状态改变回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	数据采集器状态改变回调函数指针，<see cref="KDB_COLLECTOR_STATUS_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	用户自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorRegisterStatusCallback函数需要与KDBCollectorSubscribeStatus函数配合使用。
	/// </remarks> 
	/// <seealso cref="KDBCollectorSubscribeStatus"/> 
	KDB_RET	KDBAPI	KDBCollectorRegisterStatusCallback(
		KDB_HANDLE									DBHandle, 
		KDB_COLLECTOR_STATUS_CALLBACK_FUNCTION		CallbackFunction,
		KDB_PTR										UserParameter );


	/// <summary> 
	/// 	浏览采集器中的变量。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器名称。
	/// </param>
	/// <param name="TagSourceAddressMask">
	/// 	变量源地址（即采集器中的变量名），可以带通配符，也可以为空（NULL）。
	/// </param>
	/// <param name="TagDescriptionMask">
	/// 	变量描述，可以带通配符，也可以为空（NULL）。
	/// </param>
	/// <param name="TagRecordset">
	/// 	变量记录集，<see cref="KDB_TAG_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorBrowseRequest函数主要由管理工具调用，使用该函数可以从数据采集器中批量导入变量配
	/// 	置。
	/// </remarks> 
	/// <seealso cref="KDBCollectorRegisterBrowseCallback"/> 
	KDB_RET KDBAPI KDBCollectorBrowseRequest(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				CollectorName, 
		KDB_CWSTR				TagSourceAddressMask,
		KDB_CWSTR				TagDescriptionMask, 
		PKDB_TAG_RECORDSET		TagRecordset);
	
	/// <summary> 
	/// 	注册变量浏览回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	变量浏览回调函数指针，<see cref="KDB_COLLECTOR_BROWSE_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	用户自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorRegisterBrowseCallback函数应只由数据采集器（OPC数据采集器）调用。
	/// </remarks> 
	/// <seealso cref="KDBCollectorBrowseRequest"/> 
	KDB_RET KDBAPI	KDBCollectorRegisterBrowseCallback(
		KDB_HANDLE								DBHandle, 
		KDB_COLLECTOR_BROWSE_CALLBACK_FUNCTION	CallbackFunction,
		KDB_PTR									UserParameter);


	/// <summary> 
	///		层次化浏览采集器中的变量信息。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="CollectorName">
	///		采集器名称。
	/// </param>
	/// <param name="BrowsePosition">
	///		浏览节点位置，如果为空则表示根节点。
	/// </param>
	/// <param name="BrowseRecursive">
	///		是否递归浏览所有子节点，如果为真，则浏览所有子节点的变量配置，此时不返回子节点信息。
	/// </param>
	/// <param name="BranchFilterMask">
	///		分枝过滤掩码（只有当BrowseRecursive为假时，此条件才会被应用到被浏览节点的次级分枝上）。
	/// </param>
	/// <param name="TagSourceAddressMask">
	///		变量源地址（即采集器中的变量名），可以带通配符，也可以为空（NULL）。
	/// </param>
	/// <param name="TagDescriptionMask">
	///		变量记录集，<see cref="KDB_TAG_RECORDSET"/> 。
	/// </param>
	/// <param name="ChildNodeNames">
	///		用于保存返回的子节点名称（只有当BrowseRecursive为假时）。
	/// </param>
	/// <param name="ChildNodeIds">
	///		用于保存返回的子节点标识（只有当BrowseRecursive为假时）。
	/// </param>
	/// <param name="TagRecordset">
	///		用于保存返回的变量配置信息。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks>
	///     当BrowseRecursvie为真时，只返回变量配置信息(TagRecordset)；当BrowseRecursive为假时，同时
	///	返回子节点信息ChildNodeNames和ChildNodeIds，其中ChildNodeNames用于界面显示，而ChildNodeIds用
	/// 于唯一标识一个分支或子节点，作为后续KDBCollectorBrowseHierarchicalRequest调用的BrowsePosition
	/// 参数。
	/// </remarks>
	KDB_RET KDBAPI KDBCollectorBrowseHierarchicalRequest(
		KDB_HANDLE								DBHandle, 
		KDB_CWSTR								CollectorName, 
		KDB_CWSTR								BrowsePosition,
		KDB_BOOLEAN								BrowseRecursive,
		KDB_CWSTR								BranchFilterMask,
		KDB_CWSTR								TagSourceAddressMask,
		KDB_CWSTR								TagDescriptionMask,
		PKDB_STRING_ARRAY						ChildNodeNames,
		PKDB_STRING_ARRAY						ChildNodeIds,
		PKDB_TAG_RECORDSET						TagRecordset );

	/// <summary> 
	/// 	注册变量层次化浏览回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	变量浏览回调函数指针，<see cref="KDB_COLLECTOR_BROWSE_HIERARCHICAL_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	用户自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorRegisterBrowseHierarchicalCallback函数应只由数据采集器（OPC采集器）调用。
	/// </remarks> 
	/// <seealso cref="KDBCollectorBrowseRequest"/> 
	KDB_RET KDBAPI	KDBCollectorRegisterBrowseHierarchicalCallback(
		KDB_HANDLE											DBHandle, 
		KDB_COLLECTOR_BROWSE_HIERARCHICAL_CALLBACK_FUNCTION	CallbackFunction,
		KDB_PTR												UserParameter);

	
	/// <summary> 
	/// 	控制采集器停止或启动数据采集。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器名称。
	/// </param>
	/// <param name="CollectorControl">
	/// 	数据采集器动作控制，<see cref="KDB_COLLECTOR_CONTROL"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorControlRequest函数并不能控制数据采集器程序的启动或停止，它只是在数据采集器已经
	/// 	启动运行并连接到服务器的前提下，控制数据采集器暂停或者重新开始数据采集的动作。
	/// </remarks> 
	/// <seealso cref="KDBCollectorRegisterControlCallback"/> 
	KDB_RET KDBAPI KDBCollectorControlRequest(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				CollectorName, 
		KDB_COLLECTOR_CONTROL	CollectorControl);


	/// <summary> 
	/// 	注册数据采集器控制回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	数据采集器控制回调函数指针，<see cref="KDB_COLLECTOR_CONTROL_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	用户自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorRegisterControlCallback应只由数据采集器（OPC数据采集器、计算引擎等）调用，其
	/// 	他客户端应用程序不应使用该函数。
	/// </remarks> 
	/// <seealso cref="KDBCollectorControlRequest"/> 
	KDB_RET KDBAPI	KDBCollectorRegisterControlCallback(
		KDB_HANDLE								DBHandle, 
		KDB_COLLECTOR_CONTROL_CALLBACK_FUNCTION CallbackFunction, 
		KDB_PTR									UserParameter);


	/// <summary> 
	/// 	数据采集器重载变量数据请求。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器名称。
	/// </param>
	/// <param name="StartTime">
	/// 	起始时间，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <param name="EndTime">
	/// 	中止时间，<see cref="KDB_TIMESTAMP"/> 。
	/// </param>
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="TagNames">
	/// 	变量名称数组。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorReloadRequest函数应由管理工具调用，其他应用程序应避免调用此函数。通过使用此函数，
	/// 	管理工具能够通知计算引擎重新计算某段时间内的变量值，这在某些应用场合可能是非常有用的。
	/// </remarks> 
	/// <seealso cref="KDBCollectorRegisterReloadCallback"/> 
	KDB_RET KDBAPI KDBCollectorReloadRequest( 
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				CollectorName, 
		PKDB_TIMESTAMP			StartTime,
		PKDB_TIMESTAMP			EndTime,
		KDB_UINT32				NumberOfTags,
		KDB_WSTR_ARRAY			TagNames );


	/// <summary> 
	/// 	注册数据采集器重载变量数据回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	数据采集器重载变量数据回调函数指针，<see cref="KDB_COLLECTOR_RELOAD_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorRegisterReloadCallback函数只应由数据采集器调用，其他客户端应用程序应避免调用此
	/// 	函数。
	/// </remarks> 
	KDB_RET	KDBAPI	KDBCollectorRegisterReloadCallback(
		KDB_HANDLE								DBHandle, 
		KDB_COLLECTOR_RELOAD_CALLBACK_FUNCTION	CallbackFunction,
		KDB_PTR									UserParameter);

	
	/// <summary> 
	/// 	请求获得变量当前数据（实时数据）。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器名称。
	/// </param>
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="TagNames">
	/// 	变量名称数组。
	/// </param>
	/// <param name="DataRecords">
	/// 	变量数据记录，<see cref="KDB_DATA_PROPERTIES"/> 。
	/// </param>
	/// <param name="ErrorStatuses">
	/// 	用于保存错误状态的数组，每个变量一个状态值。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	应使用KDBDataFreeCurrentValue释放获得的数据记录。
	/// </remarks> 
	/// <seealso cref="KDBCollectorRegisterCurrentValueCallback"/> 
	/// <seealso cref="KDBDataFreeCurrentValue "/> 
	KDB_RET KDBAPI KDBCollectorCurrentValueRequest( 
		KDB_HANDLE					DBHandle, 
		KDB_CWSTR					CollectorName, 
		KDB_UINT32					NumberOfTags, 
		KDB_WSTR_ARRAY				TagNames, 
		PKDB_DATA_PROPERTIES		DataRecords,
		KDB_RET*					ErrorStatuses );
	

	/// <summary> 
	/// 	注册获取变量实时数据的回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	获取变量实时数据的回调函数指针，<see cref="KDB_COLLECTOR_CURRENT_VALUE_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorRegisterCurrentValueCallback函数只应由数据采集器调用。
	/// </remarks> 
	/// <seealso cref="KDBCollectorCurrentValueRequest"/> 
	KDB_RET	KDBAPI	KDBCollectorRegisterCurrentValueCallback(
		KDB_HANDLE										DBHandle,
		KDB_COLLECTOR_CURRENT_VALUE_CALLBACK_FUNCTION	CallbackFunction , 
		KDB_PTR											UserParameter);

	/// <summary> 
	/// 	注册变量数据回写的回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	变量数据回写的回调函数指针，<see cref="KDB_COLLECTOR_WRITE_BACK_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorRegisterWriteBackCallback函数只应由数据采集器调用。
	/// </remarks> 
	KDB_RET KDBAPI KDBCollectorRegisterWriteBackCallback(
		KDB_HANDLE										DBHandle,
		KDB_COLLECTOR_WRITE_BACK_CALLBACK_FUNCTION		CallbackFunction,
		KDB_PTR											UserParameter);



	/// <summary> 
	///		请求测试计算脚本。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="CollectorName">
	/// 	数据采集器名称。
	/// </param>
	/// <param name="ScriptClass">
	///		脚本类别（Jvascript/VbScript）。
	/// </param>
	/// <param name="ScriptText">
	///		计算脚本字符串。
	/// </param>
	/// <param name="ErrorLength">
	///		用于保存错误信息的缓冲区大小。
	/// </param>
	/// <param name="ErrorDescription">
	///		用于保存错误信息的缓冲区。
	/// </param>
	/// <param name="ScriptOK">
	///		用于保存返回脚本是否执行成功标志。
	/// </param>
	/// <param name="ScriptResult">
	///		用于保存计算的结果。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBCollectorCalculationTestRequest(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				CollectorName,
		KDB_CWSTR				ScriptClass,
		KDB_CWSTR				ScriptText,
		KDB_UINT32				ErrorLength,
		KDB_WSTR				ErrorDescription,
		KDB_BOOLEAN*			ScriptOK,
		PKDB_DATA_PROPERTIES	ScriptResult );

	/// <summary> 
	/// 	注册计算脚本测试的回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	计算脚本测试的回调函数指针，<see cref="KDB_COLLECTOR_CALCULATION_TEST_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	KDBCollectorRegisterCalculationTestCallback函数只应由计算引擎调用。
	/// </remarks> 
	KDB_RET KDBAPI KDBCollectorRegisterCalculationTestCallback(
		KDB_HANDLE											DBHandle,
		KDB_COLLECTOR_CALCULATION_TEST_CALLBACK_FUNCTION	CallbackFunction,
		KDB_PTR												UserParameter);


	/// <summary> 
	///		设置采集器的性能统计数据。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="CollectorName">
	///		采集器名称。
	/// </param>
	/// <param name="Statistics">
	///		性能统计数据。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBCollectorSetStatistics(
		KDB_HANDLE					DBHandle, 
		KDB_CWSTR					CollectorName,
		PKDB_COLLECTOR_STATISTICS	Statistics );

	
//==============================================================================
// 
// 安全性相关接口函数
// 
//==============================================================================

//==============================================================================
// 
// 用户
// 
//==============================================================================


	/// <summary> 
	/// 	检索数据库系统中的现有用户。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="UserNameMask">
	/// 	用户名，可以带通配符，也可以为空（NULL）。
	/// </param>
	/// <param name="UserRecordset">
	/// 	用户记录集，<see cref="KDB_USER_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	UserNameMask可以带通配符，也都可以为空。为空表示不使用该条件进行筛选，
	/// 	意即等价于通配符“*”。
	/// </remarks> 
	/// <example>
	///		检索数据库系统中现有的所有用户：
	///		<code>
	///		
	/// 		KDB_USER_RECORDSET recordset;
	/// 		KDB_RET ret = KDBSecurityUserOpenRecordset( 
	/// 						DBHandle,
	/// 						NULL,
	/// 						&recordset );
	/// 		if( KER(ret) )
	/// 		{
	/// 			wprintf( KWSTR( "KDBSecurityUserOpenRecordset failed,error code:%d\n") , ret );
	/// 			return 0;
	/// 		}
	/// 		for( KDB_UINT32 index =0; index < recordset.NumberOfRecords; index++ )
	/// 		{
	/// 			PKDB_USER_PROPERTIES pUserProperties = &recordset.UserRecords[index];
	/// 			wprintf( KWSTR("UserName:%s\n UserFullName:%s\n Description:%s\n"),
	/// 				pUserProperties->UserName,
	/// 				pUserProperties->UserFullName,
	/// 				pUserProperties->UserDescription);
	/// 			
	/// 		}
	/// 		
	/// 		// 释放内存
	/// 		KDBSecurityUserCloseRecordset( &recordset );
	/// 	
	///		</code>
	/// </example>
	/// <seealso cref="KDBSecurityUserCloseRecordset"/> 
	KDB_RET KDBAPI KDBSecurityUserOpenRecordset(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				UserNameMask,
		PKDB_USER_RECORDSET		UserRecordset );


	/// <summary> 
	/// 	关闭用户记录集，释放记录集内部分配的内存。
	/// </summary> 
	/// <param name="UserRecordset">
	/// 	用户记录集，<see cref="KDB_USER_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBSecurityUserOpenRecordset"/> 
	KDB_RET KDBAPI KDBSecurityUserCloseRecordset(
		PKDB_USER_RECORDSET			UserRecordset );


	/// <summary> 
	/// 	检索单个用户的属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="UserName">
	/// 	用户名。
	/// </param>
	/// <param name="UserProperties">
	/// 	用户属性，<see cref="KDB_USER_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBSecurityUserOpenRecordset"/> 
	/// <seealso cref="KDBSecurityUserSetProperties"/> 
	/// <seealso cref="KDBSecurityUserFreeProperties"/> 
	KDB_RET KDBAPI KDBSecurityUserGetProperties(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				UserName,
		PKDB_USER_PROPERTIES	UserProperties );

	/// <summary> 
	/// 	修改用户属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="UserProperties">
	/// 	用户属性，<see cref="KDB_USER_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBSecurityUserGetProperties"/> 
	/// <seealso cref="KDBSecurityUserFreeProperties"/> 
	KDB_RET KDBAPI KDBSecurityUserSetProperties(
		KDB_HANDLE					DBHandle,
		PKDB_USER_PROPERTIES		UserProperties );


	/// <summary> 
	/// 	释放用户属性结构内部分配的内存。
	/// </summary> 
	/// <param name="UserProperties">
	/// 	用户属性，<see cref="KDB_USER_PROPERTIES"/> 。
	/// </param>
	/// <seealso cref="KDBSecurityUserGetProperties"/> 
	/// <seealso cref="KDBSecurityUserSetProperties"/> 
	KDB_VOID KDBAPI KDBSecurityUserFreeProperties(
		PKDB_USER_PROPERTIES		UserProperties );

	/// <summary> 
	///		获得用户所属的角色。
	/// </summary> 
	/// <param name="DBHandle">
	///		服务器连接句柄。
	/// </param>
	/// <param name="UserName">
	///		用户名称。
	/// </param>
	/// <param name="UserRoles">
	///		用于保存发挥的用户所属的角色名数组。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks>
	///     需要使用KDBUtilFreeStringArray函数释放UserRoles。
	/// </remarks>
	KDB_RET KDBAPI KDBSecurityUserGetRoles(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				UserName,
		PKDB_STRING_ARRAY		UserRoles );

	
	/// <summary> 
	///		将用户添加到某些角色。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="UserName">
	///		用户名。
	/// </param>
	/// <param name="NumberOfRoles">
	///		待添加的角色数目。
	/// </param>
	/// <param name="RoleNames">
	///		角色名称数组。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBSecurityUserAddRoles(
		KDB_HANDLE			DBHandle,
		KDB_CWSTR			UserName,
		KDB_UINT32			NumberOfRoles,
		KDB_WSTR_ARRAY		RoleNames );

	/// <summary> 
	///		将用户添加从某些角色中删除。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="UserName">
	///		用户名。
	/// </param>
	/// <param name="NumberOfRoles">
	///		待删除的角色数目。
	/// </param>
	/// <param name="RoleNames">
	///		角色名称数组。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBSecurityUserDeleteRoles(
		KDB_HANDLE			DBHandle,
		KDB_CWSTR			UserName,
		KDB_UINT32			NumberOfRoles,
		KDB_WSTR_ARRAY		RoleNames );


	/// <summary> 
	/// 	修改用户密码。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="UserName">
	/// 	用户名。
	/// </param>
	/// <param name="OldPassword">
	/// 	旧密码。
	/// </param>
	/// <param name="NewPassword">
	/// 	新密码。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks>
	///     管理员可以修改其他用户的密码，而普通用户智能修改自己的密码。
	/// </remarks>
	/// <seealso cref="KDBSecurityUserAdd"/>
	KDB_RET	KDBAPI KDBSecurityUserChangePassword(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				UserName,
		KDB_CWSTR				OldPassword,
		KDB_CWSTR				NewPassword);
		
	/// <summary> 
	/// 	添加新用户。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="Password">
	/// 	密码。
	/// </param>
	/// <param name="UserProperties">
	/// 	用户属性，<see cref="KDB_USER_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBSecurityUserAdd(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				Password,
		PKDB_USER_PROPERTIES	UserProperties);

	/// <summary> 
	/// 	删除用户。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="UserName">
	/// 	用户名。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBSecurityUserDelete(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				UserName);


//==============================================================================
// 
// 角色
// 
//==============================================================================

	/// <summary> 
	/// 	添加自定义用户角色。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="RoleProperties">
	/// 	角色属性，<see cref="KDB_ROLE_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	只能增加用户自定义角色，不能增加系统预定义角色。
	/// </remarks> 
	KDB_RET KDBAPI KDBSecurityRoleAdd(
		KDB_HANDLE					DBHandle,
		PKDB_ROLE_PROPERTIES		RoleProperties );

	/// <summary> 
	/// 	删除用户自定义角色。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="RoleName">
	/// 	角色名称。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	系统预定义角色不能删除。
	/// </remarks> 
	KDB_RET KDBAPI KDBSecurityRoleDelete(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				RoleName );

	/// <summary> 
	/// 	添加用户到角色。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="RoleName">
	/// 	角色名称。
	/// </param>
	/// <param name="NumberOfUsers">
	/// 	用户数目。
	/// </param>
	/// <param name="UserNames">
	/// 	用户名数组。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBSecurityRoleAddUsers(
		KDB_HANDLE			DBHandle,
		KDB_CWSTR			RoleName,
		KDB_UINT32			NumberOfUsers,
		KDB_WSTR_ARRAY		UserNames );


	/// <summary> 
	/// 	删除用户具有的角色。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="RoleName">
	/// 	角色名称。
	/// </param>
	/// <param name="NumberOfUsers">
	/// 	用户数目。
	/// </param>
	/// <param name="UserNames">
	/// 	用户名数字。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBSecurityRoleDeleteUsers(
		KDB_HANDLE			DBHandle,
		KDB_CWSTR			RoleName,
		KDB_UINT32			NumberOfUsers,
		KDB_WSTR_ARRAY		UserNames );

	/// <summary> 
	/// 	检索数据库系统中现有的角色。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="RoleNameMask">
	/// 	角色名称，可以带通配符，也可以为空（NULL）。
	/// </param>
	/// <param name="RoleRecordset">
	/// 	用于保存结果记录集，<see cref="KDB_ROLE_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <example>
	///		检索数据库中的所有角色：
	///		<code>
	///			
	/// 		KDB_ROLE_RECORDSET recordset;
	/// 		KDB_RET ret = KDBSecurityRoleOpenRecordset(
	/// 						DBHandle,
	/// 						NULL,
	/// 						&recordset );
	/// 		if( KER(ret) )
	/// 		{
	/// 			wprintf( KWSTR("KDBSecurityRoleOpenRecordset failed,error code:%d\n") , ret );
	/// 			return 0;
	/// 		}
	/// 		for( KDB_UINT32 index =0 ; index < recordset.NumberOfRecords; index++ )
	/// 		{
	/// 			PKDB_ROLE_PROPERTIES pRoleProperties = &recordset.RoleRecords[index];
	/// 			wprintf( KWSTR("RoleName:%s\n Description:%s\n"),
	/// 				pRoleProperties->RoleName,
	/// 				pRoleProperties->RoleDescription );
	/// 		}
	/// 		KDBSecurityRoleCloseRecordset( &recordset );
	/// 		
	///		</code>
	/// </example>
	/// <seealso cref="KDBSecurityRoleCloseRecordset"/> 
	KDB_RET KDBAPI KDBSecurityRoleOpenRecordset(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				RoleNameMask,
		PKDB_ROLE_RECORDSET		RoleRecordset );

	/// <summary> 
	/// 	释放记录集中分配的内存。
	/// </summary> 
	/// <param name="RoleRecordset">
	/// 	角色记录集，<see cref="KDB_ROLE_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBSecurityRoleOpenRecordset"/> 
	KDB_RET KDBAPI KDBSecurityRoleCloseRecordset(
		PKDB_ROLE_RECORDSET			RoleRecordset );


	/// <summary> 
	/// 	检索单个角色的属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="RoleName">
	/// 	角色名称。
	/// </param>
	/// <param name="RoleProperties">
	/// 	角色属性，<see cref="KDB_ROLE_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBSecurityRoleOpenRecordset"/> 
	KDB_RET KDBAPI KDBSecurityRoleGetProperties(
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				RoleName,
		PKDB_ROLE_PROPERTIES	RoleProperties );

	/// <summary> 
	/// 	修改单个角色的属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="RoleProperties">
	/// 	角色属性，<see cref="KDB_ROLE_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBSecurityRoleSetProperties(
		KDB_HANDLE				DBHandle,
		PKDB_ROLE_PROPERTIES	RoleProperties );

	/// <summary> 
	/// 	释放角色属性内部分配的内存。
	/// </summary> 
	/// <param name="RoleProperties">
	/// 	角色属性，<see cref="KDB_ROLE_PROPERTIES"/> 。
	/// </param>
	/// <seealso cref="KDBSecurityRoleGetProperties"/> 
	KDB_VOID KDBAPI KDBSecurityRoleFreeProperties(
		PKDB_ROLE_PROPERTIES		RoleProperties );


	/// <summary> 
	///		获得某个角色所包含的用户。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="RoleName">
	///		角色名。
	/// </param>
	/// <param name="UserNames">
	///		用于保存用户名的数组。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBSecurityRoleGetUsers(
		KDB_HANDLE			DBHandle,
		KDB_CWSTR			RoleName,
		PKDB_STRING_ARRAY	UserNames );


	/// <summary> 
	/// 	用户信息变化回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	用户属性更新回调函数指针，<see cref="KDB_USER_PROPERTIES_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBSecurityRegisterUserPropertiesCallback (
		KDB_HANDLE									DBHandle, 
		KDB_USER_PROPERTIES_CALLBACK_FUNCTION		CallbackFunction, 
		KDB_PTR										UserParameter );

	/// <summary> 
	/// 	角色信息变化回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	角色属性更新回调函数指针，<see cref="KDB_ROLE_PROPERTIES_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBSecurityRegisterRolePropertiesCallback (
		KDB_HANDLE									DBHandle, 
		KDB_ROLE_PROPERTIES_CALLBACK_FUNCTION		CallbackFunction, 
		KDB_PTR										UserParameter );

	/// <summary> 
	/// 	用户角色信息变化回调函数。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="CallbackFunction">
	/// 	用户角色信息变化回调函数指针，<see cref="KDB_USER_ROLE_INFO_CALLBACK_FUNCTION"/> 。
	/// </param>
	/// <param name="UserParameter">
	/// 	自定义参数。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI KDBSecurityRegisterUserRoleInfoCallback (
		KDB_HANDLE									DBHandle, 
		KDB_USER_ROLE_INFO_CALLBACK_FUNCTION		CallbackFunction, 
		KDB_PTR										UserParameter );



//==============================================================================
// 
// 存储文件相关函数
// 
//==============================================================================

	/// <summary> 
	/// 	添加（新建）存储设备。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="ArchiveProperties">
	/// 	存储文件属性，<see cref="KDB_ARCHIVE_STORE_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBArchiveDeleteStore"/> 
	KDB_RET	KDBAPI KDBArchiveAddStore(
		KDB_HANDLE						DBHandle, 
		PKDB_ARCHIVE_STORE_PROPERTIES	ArchiveProperties );

	/// <summary> 
	/// 	删除存储文件。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="ArchiveName">
	/// 	存储文件逻辑名。
	/// </param>
	/// <param name="ShouldDeleteFile">
	/// 	是否物理删除文件，亦或只是从数据库系统中卸载该存储文件。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBArchiveAddStore"/> 
	KDB_RET	KDBAPI KDBArchiveDeleteStore(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				ArchiveName, 
		KDB_BOOLEAN				ShouldDeleteFile);

	/// <summary> 
	/// 	检索数据库系统中现有的存储设备信息。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="ArchiveNameMask">
	/// 	存储设备逻辑名，可以带通配符，也可以为空（NULL）。
	/// </param>
	/// <param name="ArchiveRecordset">
	/// 	存储设备记录集，<see cref="KDB_ARCHIVE_STORE_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBArchiveCloseStoreRecordset"/> 
	KDB_RET	KDBAPI KDBArchiveOpenStoreRecordset(
		KDB_HANDLE						DBHandle, 
		KDB_CWSTR						ArchiveNameMask, 
		PKDB_ARCHIVE_STORE_RECORDSET	ArchiveRecordset);

	/// <summary> 
	/// 	关闭存储设备信息记录集，释放相关资源。
	/// </summary> 
	/// <param name="ArchiveRecordset">
	/// 	存储设备信息记录集，<see cref="KDB_ARCHIVE_STORE_RECORDSET"/> 。
	/// </param>
	/// <seealso cref="KDBArchiveOpenStoreRecordset"/> 
	KDB_VOID KDBAPI KDBArchiveCloseStoreRecordset(
		PKDB_ARCHIVE_STORE_RECORDSET		ArchiveRecordset);


	/// <summary> 
	///		获得存储设备组成的文件信息。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="ArchiveName">
	///		存储设备名称。
	/// </param>
	/// <param name="FileRecordset">
	///		文件结果集，<see cref="KDB_ARCHIVE_FILE_RECORDSET"/> 。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBArchiveOpenFileRecordset(
		KDB_HANDLE						DBHandle, 
		KDB_CWSTR						ArchiveName,
		PKDB_ARCHIVE_FILE_RECORDSET		FileRecordset );

	/// <summary> 
	///		释放文件结果集。
	/// </summary> 
	/// <param name="FileRecordset">
	///		文件结果集，<see cref="KDB_ARCHIVE_FILE_RECORDSET"/> 。
	/// </param>
	KDB_VOID KDBAPI KDBArchiveCloseFileRecordset(
		PKDB_ARCHIVE_FILE_RECORDSET		FileRecordset );

	/// <summary> 
	///		加入文件到存储设备。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="ArchiveName">
	///		存储设备名称。
	/// </param>
	/// <param name="FilePath">
	///		文件路径。
	/// </param>
	/// <param name="FileSize">
	///		文件大小（以M为单位）。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBArchiveAddFileToStore(
		KDB_HANDLE						DBHandle, 
		KDB_CWSTR						ArchiveName,
		KDB_CWSTR						FilePath,
		KDB_UINT32						FileSize);

	/// <summary> 
	///		从表空间中删除文件。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="ArchiveName">
	///		存储设备名称。
	/// </param>
	/// <param name="FileName">
	///		文件名称。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks>
	///     该函数只能删除表空间中未使用的空文件。
	/// </remarks>
    KDB_RET KDBAPI KDBArchiveDeleteFileFromStore(
		KDB_HANDLE						DBHandle, 
		KDB_CWSTR						ArchiveName,
		KDB_CWSTR						FileName );


	/// <summary> 
	/// 	检索单个存储设备的属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="ArchiveName">
	/// 	存储设备逻辑名。
	/// </param>
	/// <param name="ArchiveProperties">
	/// 	存储文件属性，<see cref="KDB_ARCHIVE_STORE_PROPERTIES"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBArchiveSetStoreProperties"/> 
	/// <seealso cref="KDBArchiveFreeStoreProperties"/> 
	KDB_RET	KDBAPI KDBArchiveGetStoreProperties(
		KDB_HANDLE						DBHandle, 
		KDB_CWSTR						ArchiveName, 
		PKDB_ARCHIVE_STORE_PROPERTIES	ArchiveProperties);


	/// <summary> 
	/// 	修改存储文件属性。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="ArchiveProperties">
	/// 	存储文件属性，<see cref="KDB_ARCHIVE_STORE_PROPERTIES"/> 。	
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBArchiveGetStoreProperties"/> 
	KDB_RET	KDBAPI KDBArchiveSetStoreProperties(
		KDB_HANDLE						DBHandle,
		PKDB_ARCHIVE_STORE_PROPERTIES	ArchiveProperties);

	/// <summary> 
	/// 	释放存储文件属性内部分配的资源。
	/// </summary> 
	/// <param name="ArchiveProperties">
	///		存储文件属性，<see cref="KDB_ARCHIVE_STORE_PROPERTIES"/> 。
	/// </param>
	/// <seealso cref="KDBArchiveGetStoreProperties"/> 
	KDB_VOID KDBAPI KDBArchiveFreeStoreProperties(
		PKDB_ARCHIVE_STORE_PROPERTIES	ArchiveProperties);


	/// <summary> 
	/// 	释放存储文件属性内部分配的资源。
	/// </summary> 
	/// <param name="FileProperties">
	///		存储文件属性，<see cref="KDB_ARCHIVE_FILE_PROPERTIES"/> 。
	/// </param>
	KDB_VOID KDBAPI KDBArchiveFreeFileProperties(
		PKDB_ARCHIVE_FILE_PROPERTIES	FileProperties);


	/// <summary> 
	/// 	备份存储文件。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="ArchiveName">
	/// 	存储设备逻辑名。
	/// </param>
	/// <param name="BackupFileName">
	/// 	备份目标文件物理路径。
	/// </param>
	/// <param name="MaxFileSize">
	/// 	指定每个文件的最大大小，以M为单位；如果为0，则表示不指定。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBArchiveRestoreStore"/> 
	KDB_RET KDBAPI KDBArchiveBackupStore(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				ArchiveName, 
		KDB_CWSTR				BackupFileName,
		KDB_UINT32				MaxFileSize );


	/// <summary> 
	/// 	恢复先前备份的存储文件。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="BackupFileName">
	/// 	备份文件物理路径。
	/// </param>
	/// <param name="ArchiveName">
	/// 	存储设备逻辑名。
	/// </param>
	/// <param name="NeedRestartServer">
	/// 	需要重启服务器才能使用该存储文件。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBArchiveBackupStore"/> 
	KDB_RET KDBAPI KDBArchiveRestoreStore(
		KDB_HANDLE				DBHandle, 
		KDB_CWSTR				BackupFileName,
		KDB_CWSTR				ArchiveName,
		KDB_BOOLEAN*			NeedRestartServer);


//==============================================================================
// 
// 通用SQL访问接口
// 
//==============================================================================

//==============================================================================
// 
// SQL相关常量定义
// 
//==============================================================================



/// <summary> 
/// 列标志
/// </summary> 
typedef enum KDBSqlColumnFlags
{
	KSCF_BOOKMARK		= 0x00000001,	/// 书签列
	KSCF_CHAPTER		= 0x00000002,	/// 章节列
	KSCF_FIXED_LENGTH	= 0x00000004,	/// 固定长度列
	KSCF_LONG			= 0x00000008,	/// 是否为超长LOB数据
	KSCF_NULLABLE		= 0x00000010,	/// 是否允许为空
	KSCF_READ_ONLY		= 0x00000020,	/// 只读列	
	KSCF_PRIMARY_KEY	= 0x00000040,	/// 列属于主键
	KSCF_FOREIGN_KEY	= 0x00000080,	/// 列属于外键
	KSCF_UNIQUE			= 0x00000100,	/// 列值唯一
	KSCF_AUTO_KEY		= 0x00000200,	/// 自增列
	KSCF_CASE_SENSITIVE = 0x00000400,	/// 大小写相关列
	KSCF_SORTABLE		= 0x00000800,	/// 可排序列
	KSCF_HAS_DEFAULT	= 0x00001000,	/// 有默认值
	KSCF_ROWID			= 0x00002000,	/// 行标识符
	KSCF_ROW			= 0x00004000,	/// 该列为行
	KSCF_ROWSET			= 0x00008000,	/// 该列为行集
	KSCF_MAY_DEFER		= 0x00010000,	/// 可延迟发送，除非显式请求，否则数据并不传送
	KSCF_VIRTUAL		= 0x00020000,	/// 虚拟列(历史表中的特殊列)
	KSCF_COMPUTE		= 0x00040000,	/// 计算列
} KDB_SQL_COLUMN_FLAGS;

/// <summary> 
/// 表属性。
/// </summary> 
typedef enum KDBSqlTableFlags
{
	KSTF_HISTORY_TABLE		= 0x00000001,	// 历史数据表，查询时需要特殊处理
	KSTF_TEMP_TABLE			= 0x00000002,	// 临时数据表
	KSTF_BOOKMARKABLE		= 0x00000004,	// 支持书签
	KSTF_READ_ONLY			= 0x00000008,	// 只读表，不能修改
	KSTF_SYSTEM_TABLE		= 0x00000010,	// 系统表，不能修改
}KDB_SQL_TABLE_FLAGS;


/// <summary> 
/// 索引属性。
/// </summary>
typedef enum KDBSqlIndexFlags
{
	KSIF_UNIQUE			= 0x00000001,	/// 索引是唯一的
	KSIF_CLUSTERED		= 0x00000002,	/// 索引是聚族的
	KSIF_SORT_BOOKMARK	= 0x00000004,	/// 对重复索引按书签排序		
	KSIF_AUTO_UPDATE	= 0x00000008,	/// 索引自动更新	
	KSIF_TEMP_INDEX		= 0x00000010,	/// 临时索引(查询处理)
	KSIF_PRIMARY_KEY	= 0x00000020,	/// 索引就是主键
	KSIF_SYSTEM_INDEX	= 0x00000040,	/// 系统特殊索引
}KDB_SQL_INDEX_FLAGS;


/// <summary> 
/// 命令的结果状态
/// </summary> 
typedef enum KDBSqlCommandStatus
{
	KSCS_UNKNWON = 0,						/// 未知的状态
	KSCS_COMMAND_OK,						/// 成功完成一个没有返回数据的命令
	KSCS_TUPLES_OK,							/// 成功执行查询(SELECT)
	KSCS_COMMAND_ERROR,						/// 一般性错误
} KDB_SQL_COMMAND_STATUS;


/// <summary> 
/// 结果集的类型。
/// </summary> 
typedef enum KDBSqlResultType
{
	KSRT_UNKNOWN	= 0,				/// 未知类型
	KSRT_ROWCOUNT	= 1,				/// 影响的行数
	KSRT_ROWSET		= 2,				/// 行集
	KSRT_STATUS		= 3,				/// 错误状态
} KDB_SQL_RESULT_TYPE;

//==============================================================================
// 
/// SQL相关结构定义
// 
//==============================================================================


/// <summary> 
/// 列信息。
/// </summary> 
typedef struct KDBTableColumnInfo
{
	KDB_WSTR				Name;			/// 列名
	KDB_WSTR				TableName;		/// 表名
	KDB_WSTR				DefaultValue;	/// 列值
	KDB_UINT32				Ordinal;		/// 列编号
	KDB_UINT32				Flags;			/// 列标志
	KDB_UINT32				Size;			/// 列大小(最大长度)
	KDB_UINT16				DataType;		/// 列数据类型
	KDB_UINT16				NativeType;		/// SQL数据类型
	KDB_UINT8				Precision;		/// 精度(数值类型)
	KDB_UINT8				Scale;			/// 刻度(数值类型)
	KDB_UINT32				Searchable;		/// 可检索属性
	KDB_INT32				AutoKeyIncrement;/// 自增列增量
	KDB_INT32				AutoKeySeed;	///  自增列初始值
} KDB_TABLE_COLUMN_INFO , *PKDB_TABLE_COLUMN_INFO;


/// <summary> 
/// 列架构信息( COLUMNS rowset)
/// </summary> 
typedef struct KDBSchemaColumnProperties
{
	KDB_WSTR				TableCatalog;			/// 数据库名称
	KDB_WSTR				TableSchema;			/// 模式名称
	KDB_WSTR				TableName;				/// 表名(关系名)
	KDB_WSTR				ColumnName;				/// 列名(域名)
	KDB_UINT32				ColumnOrdinal;			/// 列编号
	KDB_UINT16				DataType;				/// 列数据类型
	KDB_UINT32				MaxCharLength;			/// 列最大长度
	KDB_UINT32				MaxOctetLength;			/// 列最大长度(字节数)
	KDB_UINT8				NumericPrecision;		/// 数值类型的精度
	KDB_INT8				NumericScale;			/// 数值类型的比例尺
	KDB_UINT32				ColumnFlags;			/// 列选项
	KDB_UINT32				Searchable;				/// 列的可检索属性
	KDB_WSTR				ColumnDefault;			/// 默认值
	KDB_INT32				AutoKeyIncrement;		/// 自增列的步进值
	KDB_INT32				AutoKeySeed;			/// 自增列的初始值
} KDB_SCHEMA_COLUMN_PROPERTIES , *PKDB_SCHEMA_COLUMN_PROPERTIES;

/// <summary> 
/// 列架构信息记录集
/// </summary> 
/// <seealso cref="KDBSchemaColumnProperties"/> 
typedef struct KDBSchemaColumnRecordset
{
	KDB_UINT32						NumberOfRecords;	/// 记录数目
	PKDB_SCHEMA_COLUMN_PROPERTIES	ColumnRecords;		/// 列信息记录
} KDB_SCHEMA_COLUMN_RECORDSET , *PKDB_SCHEMA_COLUMN_RECORDSET;

/// <summary> 
/// 表架构信息
/// </summary> 
typedef struct KDBSchemaTableProperties
{
	KDB_WSTR				TableCatalog;			/// 数据库名称
	KDB_WSTR				TableSchema;			/// 模式名称
	KDB_WSTR				TableName;				/// 表名(关系名)		
	KDB_WSTR				TableType;				/// 表类型(普通表、系统表、视图、系统视图等)
	KDB_UINT32				TableFlags;				/// 表的属性
}KDB_SCHEMA_TABLE_PROPERTIES , *PKDB_SCHEMA_TABLE_PROPERTIES;

/// <summary> 
/// 表架构信息记录集
/// </summary> 
typedef struct KDBSchemaTableRecordset
{
	KDB_UINT32						NumberOfRecords;	/// 记录数
	PKDB_SCHEMA_TABLE_PROPERTIES	TableRecords;		/// 表记录
} KDB_SCHEMA_TABLE_RECORDSET , *PKDB_SCHEMA_TABLE_RECORDSET;


/// <summary> 
/// 索引列架构信息。
/// </summary> 
typedef struct KDBSchemaIndexColumnProperties
{
	KDB_WSTR			ColumnName;		/// 列名
	KDB_BOOLEAN			CollationAsc;	/// 升序
} KDB_SCHEMA_INDEX_COLUMN_PROPERTIES,*PKDB_SCHEMA_INDEX_COLUMN_PROPERTIES;

/// <summary> 
/// 索引的架构信息。
/// </summary> 
typedef struct KDBSchemaIndexProperties
{
	KDB_WSTR							TableCatalog;	/// 数据库名称
	KDB_WSTR							TableSchema;	/// 模式名称
	KDB_WSTR							TableName;		/// 表名(关系名)		
	KDB_WSTR							IndexName;		/// 索引名
	KDB_UINT16							IndexType;		/// 索引类型
	KDB_UINT32							IndexFlags;		/// 索引标志
	KDB_UINT32							InitialSize;	/// 初始大小
	KDB_UINT32							FillFactor;		/// 填充比例(0-100)	
	KDB_UINT32							ColumnCount;	/// 索引键列数
	PKDB_SCHEMA_INDEX_COLUMN_PROPERTIES ColumnInfo;		/// 列信息
} KDB_SCHEMA_INDEX_PORPERTIES,*PKDB_SCHEMA_INDEX_PORPERTIES;

/// <summary> 
/// 索引架构记录集。
/// </summary> 
typedef struct KDBSchemaIndexRecordset
{
	KDB_UINT32							NumberOfRecords;/// 记录条数
	PKDB_SCHEMA_INDEX_PORPERTIES		IndexRecords;	/// 索引记录
} KDB_SCHEMA_INDEX_RECORDSET,*PKDB_SCHEMA_INDEX_RECORDSET;



//==============================================================================
// 
// SQL相关接口定义
// 
//==============================================================================

	/// <summary> 
	/// 	执行SQL命令。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="QueryCommand">
	/// 	SQL命令。
	/// </param>
	/// <param name="QueryResults">
	/// 	[out]用于保存返回的多结果集句柄。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBSqlGetNextResult"/> 
	KDB_RET KDBAPI KDBSqlExecute( 
		KDB_HANDLE				DBHandle,
		KDB_CWSTR				QueryCommand,
		KDB_MULTIPLE_RESULT*	QueryResults );

	/// <summary> 
	/// 	读取下一个查询结果。
	/// </summary> 
	/// <param name="QueryResults">
	/// 	多结果集句柄。
	/// </param>
	/// <param name="QueryResult">
	/// 	[out]用于保存返回的结果。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBSqlExecute"/> 
	/// <seealso cref="KDBSqlHasMoreResults"/> 
	KDB_RET KDBAPI KDBSqlGetNextResult( 
		KDB_MULTIPLE_RESULT		QueryResults,
		KDB_RESULT*				QueryResult );
	
	/// <summary> 
	/// 	检查是否还有结果集未处理。
	/// </summary> 
	/// <param name="QueryResults">
	/// 	多结果集句柄。
	/// </param>
	/// <returns>
	/// 	如果还有查询结果集未处理，则返回KDB_TRUE，否则返回KDB_FALSE。
	/// </returns>
	KDB_BOOLEAN KDBAPI KDBSqlHasMoreResults(
		KDB_MULTIPLE_RESULT		QueryResults );

	/// <summary> 
	/// 	释放查询结果分配的内存。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	KDB_VOID KDBAPI KDBSqlFreeResult(
		KDB_RESULT				QueryResult );

	/// <summary> 
	///		释放查询多结果集合句柄。
	/// </summary> 
	/// <param name="QueryResults">
	///		多结果集句柄。
	/// </param>
    KDB_VOID KDBAPI KDBSqlFreeMultipleResult(
		KDB_MULTIPLE_RESULT		QueryResults );

	//==============================================================================
	// 
	// 访问查询结果集的函数
	// 
	//==============================================================================

	/// <summary> 
	/// 	检索命令的结果类型。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <returns>
	/// 	返回命令的执行状态。
	/// </returns>
	KDB_SQL_RESULT_TYPE KDBAPI KDBSqlResultTypeInfo(
		KDB_RESULT				QueryResult );

	
	/// <summary> 
	/// 	返回与查询关联的错误信息。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <returns>
	/// 	返回与查询关联的错误信息，在没有错误时返回一个空字符串。
	/// </returns>
	KDB_CWSTR KDBAPI KDBSqlResultErrorInfo( 
		KDB_RESULT				QueryResult );

	/// <summary> 
	/// 	返回与查询关联的错误信息。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <returns>
	/// 	返回与查询关联的错误码。
	/// </returns>
	KDB_RET	  KDBAPI KDBSqlResultErrorCode (
		KDB_RESULT				QueryResult );

	/// <summary> 
	/// 	获得结果集的元组(行)的数目或受SQL命令影响的行数。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <returns>
	/// 	返回结果集的元组数目或受影响的行数，失败时返回-1。
	/// </returns>
	KDB_INT32 KDBAPI KDBSqlTupleCount(
		KDB_RESULT				QueryResult );


	/// <summary> 
	/// 	获得查询结果中的列个数。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <returns>
	/// 	成功时返回列的数目，失败时返回-1。
	/// </returns>
	KDB_INT32 KDBAPI KDBSqlColumnCount(
		KDB_RESULT				QueryResult );


	/// <summary> 
	/// 	检索列的信息（元数据）。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <param name="ColumnOrdinal">
	/// 	列编号，从1开始。
	/// </param>
	/// <param name="ColumnInfo">
	/// 	用于保存列信息，<see cref="KDB_TABLE_COLUMN_INFO"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误码。
	/// </returns>
	/// <example>
	///		<code>
	///			
	/// 		KDB_TABLE_COLUMN_INFO columnInfo = { 0 };
	/// 		KDB_RET ret = KDBSqlColumnGetInfo(
	/// 						QueryResult,
	/// 						1,
	/// 						&columnInfo );
	/// 		if( KOK(ret) )
	/// 		{
	/// 			wprintf( 
	/// 				KWSTR( "Name:%s\n Ordinal:%d\n Flags:%d\n Type:%d\n Size:%d\n" ) ,
	/// 				columnInfo.Name,
	/// 				columnInfo.Ordinal,
	/// 				columnInfo.Flags,
	/// 				columnInfo.Type,
	/// 				columnInfo.Size);
	/// 			
	/// 			// 释放内存
	/// 			KDBSqlColumnFreeInfo( &columnInfo );
	/// 		}
	/// 
	///		</code>
	/// </example>
	KDB_RET KDBAPI KDBSqlColumnGetInfo(
		KDB_RESULT					QueryResult,
		KDB_UINT32					ColumnOrdinal,
		PKDB_TABLE_COLUMN_INFO		ColumnInfo );

	/// <summary> 
	/// 	释放列信息结构所分配的内存。
	/// </summary> 
	/// <param name="ColumnInfo">
	/// 	列信息结构，<see cref="KDB_TABLE_COLUMN_INFO"/> 。
	/// </param>
	/// <seealso cref="KDBSqlColumnGetInfo"/> 
	KDB_VOID KDBAPI KDBSqlColumnFreeInfo(
		PKDB_TABLE_COLUMN_INFO		ColumnInfo );

	/// <summary> 
	/// 	返回与给出的列编号相关联的列(字段)的名称。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <param name="ColumnOrdinal">
	/// 	列的编号（从1开始）。
	/// </param>
	/// <returns>
	/// 	成功时返回相应的列名，如果出现错误则返回NULL。
	/// </returns>
	KDB_CWSTR KDBAPI KDBSqlColumnName(
		KDB_RESULT				QueryResult ,
		KDB_UINT32				ColumnOrdinal );
		

	/// <summary> 
	/// 	根据列名称检索列编号。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <param name="ColumnName">
	/// 	列名称。
	/// </param>
	/// <returns>
	/// 	返回与给出的列名称相关联的列编号，如果给出的名字不匹配任何列，则返回-1。
	/// </returns>
	KDB_INT32 KDBAPI KDBSqlColumnOrdinal(
		KDB_RESULT			QueryResult ,
		KDB_CWSTR			ColumnName );
	
	/// <summary> 
	/// 	检索数据列的类型信息（元数据）。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <param name="ColumnOrdinal">
	/// 	列编号，从1开始。
	/// </param>
	/// <returns>
	/// 	成功时返回列的类型，失败时返回-1。
	/// </returns>
	/// <remarks> 
	///		只能检索那些在结果集中出现的列的元数据。	
	/// </remarks> 
	KDB_INT32 KDBAPI KDBSqlColumnType(
		KDB_RESULT				QueryResult,
		KDB_UINT32				ColumnOrdinal );


	/// <summary> 
	/// 	检索数据列的大小（元数据）。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <param name="ColumnOrdinal">
	/// 	列编号，从1开始。
	/// </param>
	/// <returns>
	/// 	成功时返回数据列的大小，失败时返回－1。
	/// </returns>
	/// <remarks> 
	/// 	只能检索那些在结果集中出现的列的元数据。
	/// </remarks> 
	KDB_INT32 KDBAPI KDBSqlColumnSize( 
		KDB_RESULT				QueryResult,
		KDB_UINT32				ColumnOrdinal );
	
	/// <summary> 
	/// 	按不定类型检索数据列的值。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果。
	/// </param>
	/// <param name="TupleIndex">
	/// 	元组编号，从1开始。
	/// </param>
	/// <param name="ColumnOrdinal">
	/// 	列编号，从1开始。
	/// </param>
	/// <param name="ColumnValue">
	/// 	用于保存返回的列值，<see cref="KDB_VALUE"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET KDBAPI KDBSqlColumnValue(
		KDB_RESULT				QueryResult,
		KDB_UINT32				TupleIndex,
		KDB_UINT32				ColumnOrdinal,
		PKDB_VALUE				ColumnValue );

	/// <summary> 
	///		测试一个数据域是否为空(NULL)。
	/// </summary> 
	/// <param name="QueryResult">
	/// 	查询结果数据结构。
	/// </param>
	/// <param name="TupleIndex">
	/// 	元组(行、记录)索引。
	/// </param>
	/// <param name="ColumnOrdinal">
	/// 	数据域(字段、列)索引。
	/// </param>
	/// <returns>
	/// 	如果数据域为空，则返回KDB_TRUE，否则返回KDB_FALSE（包括出现错误的时候）。
	/// </returns>
	KDB_BOOLEAN KDBAPI KDBSqlColumnIsNull(
		KDB_RESULT				QueryResult,
		KDB_UINT32				TupleIndex,
		KDB_UINT32				ColumnOrdinal );
		

//==============================================================================
// 
// 数据库构架信息获取函数
// 
//==============================================================================

	/// <summary> 
	/// 	检索数据库中表的列信息。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	数据库服务器连接句柄。
	/// </param>
	/// <param name="TableCatalog">
	/// 	数据库名。
	/// </param>
	/// <param name="TableSchema">
	/// 	模式名。
	/// </param>
	/// <param name="TableName">
	/// 	表名。
	/// </param>
	/// <param name="ColumnName">
	/// 	列名。
	/// </param>
	/// <param name="ColumnRecordset">
	/// 	保存结果记录集，<see cref="KDB_SCHEMA_COLUMN_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误码。
	/// </returns>
	/// <remarks> 
	/// 	TableCatalog、TableSchema、TableName、ColumnName都可以为空，数据库
	///		服务器将依次应用这四个参数（如果该参数不为空的话）来筛选符合条件的列。
	/// </remarks> 
	/// <example>
	///		检索数据库中表“History”的所有列信息：
	///		<code>
	///			
	/// 		KDB_SCHEMA_COLUMN_RECORDSET recordset;
	/// 		KDB_RET ret = KDBSqlSchemaColumnOpenRecordset(
	/// 						DBHandle,
	/// 						NULL,
	/// 						NULL,
	/// 						KWSTR("History"),
	/// 						NULL,
	/// 						&recordset );
	/// 		if( KOK( ret ) )
	/// 		{
	/// 			for( KDB_UINT32 index = 0; index < recordset.NumberOfRecords; index++ )
	/// 			{
	/// 				PKDB_SCHEMA_COLUMN_PROPERTIES pColumnProperties = &recordset.ColumnRecords[index];
	/// 				wprintf( KWSTR(" TableCatalog:%s\n TableSchema:%s\n TableName:%s\n ColumnName:%s\n Description:%s\n"),
	/// 					pColumnProperties->TableCatalog,
	/// 					pColumnProperties->TableSchema,
	/// 					pColumnProperties->TableName,
	/// 					pColumnProperties->ColumnName,
	/// 					pColumnProperties->Description );
	/// 			}
	/// 
	/// 			// 关闭列信息记录集并释放内存
	/// 			KDBSqlSchemaColumnCloseRecordset( &recordset );
	/// 		}
	///			
	///		</code>
	/// </example>
	/// <seealso cref="KDBSqlSchemaColumnCloseRecordset"/> 
	KDB_RET KDBAPI KDBSqlSchemaColumnOpenRecordset(
		KDB_HANDLE						DBHandle,
		KDB_CWSTR						TableCatalog,
		KDB_CWSTR						TableSchema,
		KDB_CWSTR						TableName,
		KDB_CWSTR						ColumnName,
		PKDB_SCHEMA_COLUMN_RECORDSET	ColumnRecordset);

	/// <summary> 
	/// 	关闭列信息记录集，释放分配的内存。
	/// </summary> 
	/// <param name="ColumnRecordset">
	/// 	列信息记录集，<see cref="KDB_SCHEMA_COLUMN_RECORDSET"/>。
	/// </param>
	/// <seealso cref="KDBSqlSchemaColumnOpenRecordset"/> 
	KDB_VOID KDBAPI KDBSqlSchemaColumnCloseRecordset(
		PKDB_SCHEMA_COLUMN_RECORDSET	ColumnRecordset );

	

	/// <summary> 
	/// 	检索数据库中所有的表信息。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	数据库服务器连接句柄。
	/// </param>
	/// <param name="TableCatalog">
	/// 	数据库名。
	/// </param>
	/// <param name="TableSchema">
	/// 	表模式名。
	/// </param>
	/// <param name="TableName">
	/// 	表名。
	/// </param>
	/// <param name="TableType">
	/// 	表类型。
	/// </param>
	/// <param name="TableRecordset">
	/// 	用于保存返回的表信息记录集，<see cref="KDB_SCHEMA_TABLE_RECORDSET"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时则返回相应的错误码。
	/// </returns>
	/// <remarks> 
	/// 	TableCatalog、TableSchema、TableName、TableType都可以为空，服务器将
	///		依次应用这四个参数（如果该参数不为空的话）来筛选符合条件的表。
	/// </remarks> 
	/// <example>
	///		检索数据库中所有表的信息：
	///		<code>
	///			
	/// 		KDB_SCHEMA_TABLE_RECORDSET recordset;
	/// 		KDB_RET ret = KDBSqlSchemaTableOpenRecordset(
	/// 						DBHandle,
	/// 						NULL,
	/// 						NULL,
	/// 						NULL,
	/// 						NULL,
	/// 						&recordset);
	/// 		if( KOK(ret) )
	/// 		{
	/// 			for( KDB_UINT32 index = 0; index < recordset.NumberOfRecords; index++ )
	/// 			{
	/// 				PKDB_SCHEMA_TABLE_PROPERTIES pTableProperties = &recordset.TableRecords[index];
	/// 				wprintf( KWSTR("TableCatalog:%s\n TableSchema:%s\n TableName:%s\n TableType:%s\n Description:%s\n"),
	/// 						pTableProperties->TableCatalog,
	/// 						pTableProperties->TableSchema,
	/// 						pTableProperties->TableName,
	/// 						pTableProperties->TableType,
	/// 						pTableProperties->Description );
	/// 			}
	/// 			KDBSqlSchemaTableCloseRecordset( &recordset );
	/// 		}
	///			
	///		</code>
	/// </example>
	KDB_RET KDBAPI KDBSqlSchemaTableOpenRecordset(
		KDB_HANDLE					DBHandle,
		KDB_CWSTR					TableCatalog, 
		KDB_CWSTR					TableSchema,
		KDB_CWSTR					TableName,
		KDB_CWSTR					TableType,
		PKDB_SCHEMA_TABLE_RECORDSET	TableRecordset);

	/// <summary> 
	/// 	关闭表信息记录集，释放分配的内存。
	/// </summary> 
	/// <param name="TableRecordset">
	/// 	表信息记录集。
	/// </param>
	/// <seealso cref="KDBSqlSchemaTableOpenRecordset"/> 
	KDB_VOID KDBAPI KDBSqlSchemaTableCloseRecordset(
		PKDB_SCHEMA_TABLE_RECORDSET	TableRecordset);

	
	/// <summary> 
	///		检索数据库中所有表的索引信息。
	/// </summary> 
	/// <param name="DBHandle">
	///		数据库连接句柄。
	/// </param>
	/// <param name="TableCatalog">
	///		数据库名。
	/// </param>
	/// <param name="TableSchema">
	///		模式名。
	/// </param>
	/// <param name="TableName">
	///		表名。
	/// </param>
	/// <param name="IndexName">
	///		索引名。
	/// </param>
	/// <param name="IndexRecordset">
	///		用于保存返回的索引记录集。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <remarks> 
	/// 	TableCatalog、TableSchema、TableName、IndexName都可以为空，服务器将
	///		依次应用这四个参数（如果该参数不为空的话）来筛选符合条件的表。
	/// </remarks> 
	KDB_RET KDBAPI KDBSqlSchemaIndexOpenRecordset(
		KDB_HANDLE					DBHandle,
		KDB_CWSTR					TableCatalog, 
		KDB_CWSTR					TableSchema,
		KDB_CWSTR					TableName,
		KDB_CWSTR					IndexName,
		PKDB_SCHEMA_INDEX_RECORDSET	IndexRecordset );

	/// <summary> 
	///		关闭索引信息记录集，释放分配的内存。
	/// </summary> 
	/// <param name="IndexRecordset">
	///		待释放的索引记录集。
	/// </param>
	/// <seealso cref="KDBSqlSchemaIndexOpenRecordset"/>
	KDB_VOID KDBAPI KDBSqlSchemaIndexCloseRecordset(
		PKDB_SCHEMA_INDEX_RECORDSET IndexRecordset );


	/// <summary> 
	/// 	检索变量的实时数据。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="NumberOfTags">
	/// 	变量个数。
	/// </param>
	/// <param name="TagNames">
	/// 	变量名数组。
	/// </param>
	/// <param name="DigitalAsString">
	/// 	是否以字符串方式返回Digital类型。
	/// </param>
	/// <param name="AllowGoodQualityData">
	///		是否只允许返回好质量戳的数据
	/// </param>
	/// <param name="DataProperties">
	/// 	数据属性数组，<see cref="KDB_DATA_PROPERTIES"/> 。
	/// </param>
	/// <param name="ErrorStatuses">
	/// 	错误状态数组。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBDataFreeCurrentValue"/> 
	KDB_RET KDBAPI KDBDataGetCurrentValue2(
		KDB_HANDLE				DBHandle, 
		KDB_UINT32				NumberOfTags, 
		KDB_WSTR_ARRAY			TagNames, 
		KDB_BOOLEAN				DigitalAsString,
		KDB_BOOLEAN				AllowGoodQualityData,
		PKDB_DATA_PROPERTIES	DataProperties,	
		KDB_RET*				ErrorStatuses);

	/// <summary> 
	/// 	检索给定时间后变量的实时数据。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="DateTime">
	/// 	给定的时刻
	/// </param>
	/// <param name="DigitalAsString">
	/// 	是否以字符串方式返回Digital类型。
	/// </param>
	/// <param name="AllowGoodQualityData">
	///		是否只允许返回好质量戳的数据
	/// </param>
	/// <param name="DataRecordsets">
	/// 	变量数据记录集
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBDataFreeCurrentValue"/> 
	KDB_RET KDBAPI KDBDataGetCurrentValueAfter2(
		KDB_HANDLE				DBHandle, 
		PKDB_TIMESTAMP			DateTime,
		KDB_BOOLEAN				DigitalAsString,
		KDB_BOOLEAN				AllowGoodQualityData,
		PKDB_DATA_RECORDSETS	DataRecordsets );
	
	/// <summary> 
	/// 	检索变量数据。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="DataCriteria">
	/// 	数据检索条件，<see cref="KDB_DATA_CRITERIA"/> 。
	/// </param>
	/// <param name="DataRecordsets">
	/// 	数据记录集集合，<see cref="KDB_DATA_RECORDSETS"/> 。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <example>
	///		检索变量Tag1和Tag2的的数据，采样间隔为1秒，每个采样区间内取最大、最小两个值：
	///		<code>
	///				// 构造检索条件
	///				KDB_DATA_CRITERIA criteria ={ 0 };
	///				ZeroMemory( &criteria,sizeof( KDB_DATA_CRITERIA ) ) ;
	///				KDB_WSTR TagNames[] = {
	///						KWSTR("Tag00001"),KWSTR("Tag00002"),
	///						KWSTR("Tag00003"),KWSTR("Tag00004"),
	///						KWSTR("Tag00005"),
	///				};
	///				criteria.NumberOfTags = 5;
	///				criteria.TagNames     = TagNames;
	///				KDBUtilUnicodeStringToTimeStamp( KWSTR("2005-09-30 15:00:00" ) , &criteria.StartTime );	
	///				KDBUtilUnicodeStringToTimeStamp( KWSTR("2005-10-20 00:00:00" ) , &criteria.EndTime );
	///				criteria.DataVersion		= KDAV_ALL;
	///				criteria.SamplingMode		= KSAM_RAW_BY_TIME;
	///				criteria.RowCount			= 1000000;
	///
	///				// 检索数据
	///				KDB_DATA_RECORDSETS	Recordsets = { 0 } ;
	///				DWORD StartTick = GetTickCount();
	///				KDB_RET ret = KDBDataOpenRecordset( 
	///					m_hClient,
	///					&criteria,
	///					&Recordsets );
	///				DWORD EndTick = GetTickCount();
	///				if( KER(ret) )
	///				{
	///					wprintf( KWSTR("KDBDataOpenRecordset failed, error code : %d\n") , ret);
	///					return;
	///				}
	///				else 
	///				{
	///					wprintf( KWSTR( "KDBDataOpenRecordset : %d\n" ), EndTick - StartTick );
	///				}
	///
	///				// 输出检索结果
	///				for( KDB_UINT32 index = 0; index < 5; index++ )
	///				{
	///					if( KER(Recordsets.DataRecordset[index].ErrorStatus) )
	///					{
	///						WCHAR ErrorString[256] = { 0 };
	///						KDBUtilGetErrorDescription( Recordsets.DataRecordset[index].ErrorStatus,ErrorString,256 );
	///						wprintf( KWSTR("Search data of tag %d failed, error code: %d %s\n") ,
	///							index+1 ,Recordsets.DataRecordset[index].ErrorStatus,ErrorString );
	///						continue;
	///					}
	///
	///					// 显示变量记录集的最后一条记录
	///					PKDB_DATA_RECORDSET pDataRecordset = &Recordsets.DataRecordset[index] ;
	///					wprintf( KWSTR("TagName:%s %d\n") , pDataRecordset->TagName,pDataRecordset->NumberOfRecords );
	///					PKDB_DATA_PROPERTIES pDataProperties = &pDataRecordset->DataRecords[pDataRecordset->NumberOfRecords-1];
	///					WCHAR TimeString[256] = { 0 };
	///					KDBUtilTimeStampToUnicodeString( &pDataProperties->TimeStamp,TimeString );
	///					wprintf( KWSTR("Timestamp: %s\tValue: %08.3f\tQuality: %d\n") ,
	///						TimeString,
	///						pDataProperties->Value.r4Val,
	///						pDataProperties->Quality );
	///				}
	///
	///				// 关闭记录集
	///				KDBDataCloseRecordset( &Recordsets );
	///
	///		</code>
	/// </example>
	/// <seealso cref="KDBDataCloseRecordset"/> 
	KDB_RET	KDBAPI  KDBDataOpenRecordset2(
		KDB_HANDLE				DBHandle, 
		PKDB_DATA_CRITERIA2		DataCriteria,
        PKDB_DATA_RECORDSETS	DataRecordsets);
        
	/// <summary> 
	///		检索变量数据。
	/// </summary> 
	/// <param name="DBHandle">
	///		连接句柄。
	/// </param>
	/// <param name="DataCriteria">
	///		数据检索条件，<see cref="KDB_DATA_CRITERIA"/> 。
	/// </param>
	/// <param name="RecordsetHandle">
	///		[out]保存返回的变量数据结果集。
	/// </param>
	/// <returns>
	///		成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	KDB_RET	KDBAPI  KDBDataOpenRecordsetHandle2(
		KDB_HANDLE						DBHandle, 
		PKDB_DATA_CRITERIA2				DataCriteria,
		KDB_DATA_RECORDSET_HANDLE*		RecordsetHandle );

	/// <summary> 
	/// 	恢复先前备份的存储文件。
	/// </summary> 
	/// <param name="DBHandle">
	/// 	连接句柄。
	/// </param>
	/// <param name="StartTime">
	/// 	待恢复的存储文件的数据起始时间。
	/// </param>
	/// <param name="EndTime">
	/// 	待恢复的存储文件的数据终止时间。
	/// </param>
	/// <returns>
	/// 	成功时返回KERR_OK，失败时返回相应的错误代码。
	/// </returns>
	/// <seealso cref="KDBArchiveBackupStore"/> 
	KDB_RET KDBAPI KDBArchiveRestoreStoreByTime(
		KDB_HANDLE				DBHandle, 
		KDB_TIMESTAMP			StartTime,
		KDB_TIMESTAMP			EndTime);


//==============================================================================
#ifdef  __cplusplus
}
#endif
//==============================================================================
#pragma pack( pop , BEFOREKRTDBAPI )
//==============================================================================
#endif ///  __KRTDBAPI__H__INCLUDED__
//==============================================================================

