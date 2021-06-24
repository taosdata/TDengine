/**
 *  @file   conf.c
 *  @author Sheng Di (sdi1@anl.gov or disheng222@gmail.com)
 *  @date   2015.
 *  @brief  Configuration loading functions for the SZ library.
 *  (C) 2015 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <math.h>
#include "string.h"
#include "sz.h"
#include "iniparser.h"
#include "Huffman.h"
#include "pastri.h"

/*-------------------------------------------------------------------------*/
/**
    @brief      It reads the configuration given in the configuration file.
    @return     integer         1 if successfull.

    This function reads the configuration given in the SZ configuration
    file and sets other required parameters.

 **/
 
/*struct node_t *pool;
node *qqq;
node *qq;
int n_nodes = 0, qend;
unsigned long **code;
unsigned char *cout;
int n_inode;*/ 
 
unsigned int roundUpToPowerOf2(unsigned int base)
{
  base -= 1;

  base = base | (base >> 1);
  base = base | (base >> 2);
  base = base | (base >> 4);
  base = base | (base >> 8);
  base = base | (base >> 16);

  return base + 1;
} 
 
void updateQuantizationInfo(int quant_intervals)
{
	exe_params->intvCapacity = quant_intervals;
	exe_params->intvRadius = quant_intervals/2;
} 
 
double computeABSErrBoundFromPSNR(double psnr, double threshold, double value_range)
{
	double v1 = psnr + 10 * log10(1-2.0/3.0*threshold);
	double v2 = v1/(-20);
	double v3 = pow(10, v2);
	return value_range * v3;
} 

double computeABSErrBoundFromNORM_ERR(double normErr, size_t nbEle)
{
	return sqrt(3.0/nbEle)*normErr;
} 

 
/*-------------------------------------------------------------------------*/
/**
 * 
 * 
 * @return the status of loading conf. file: 1 (success) or 0 (error code);
 * */
int SZ_ReadConf(const char* sz_cfgFile) {
    // Check access to SZ configuration file and load dictionary
    //record the setting in confparams_cpr
    confparams_cpr = (sz_params*)malloc(sizeof(sz_params));    
    exe_params = (sz_exedata*)malloc(sizeof(sz_exedata));
    
    int x = 1;
    char sol_name[256];
    char *modeBuf;
    char *errBoundMode;
    char *endianTypeString;
    dictionary *ini;
    char *par;

	char *y = (char*)&x;
	
	if(*y==1)
		sysEndianType = LITTLE_ENDIAN_SYSTEM;
	else //=0
		sysEndianType = BIG_ENDIAN_SYSTEM;
    
    confparams_cpr->plus_bits = 3;
    
    if(sz_cfgFile == NULL)
    {
		dataEndianType = LITTLE_ENDIAN_DATA;
		confparams_cpr->sol_ID = SZ;
		confparams_cpr->max_quant_intervals = 65536;
		confparams_cpr->maxRangeRadius = confparams_cpr->max_quant_intervals/2;
				
		exe_params->intvCapacity = confparams_cpr->maxRangeRadius*2;
		exe_params->intvRadius = confparams_cpr->maxRangeRadius;
		
		confparams_cpr->quantization_intervals = 0;
		exe_params->optQuantMode = 1;
		confparams_cpr->predThreshold = 0.99;
		confparams_cpr->sampleDistance = 100;
		
		confparams_cpr->szMode = SZ_BEST_COMPRESSION;
		confparams_cpr->losslessCompressor = ZSTD_COMPRESSOR; //other option: GZIP_COMPRESSOR;
		if(confparams_cpr->losslessCompressor==ZSTD_COMPRESSOR)
			confparams_cpr->gzipMode = 3; //fast mode
		else
			confparams_cpr->gzipMode = 1; //high speed mode
		
		confparams_cpr->errorBoundMode = PSNR;
		confparams_cpr->psnr = 90;
		confparams_cpr->absErrBound = 1E-4;
		confparams_cpr->relBoundRatio = 1E-4;
		confparams_cpr->accelerate_pw_rel_compression = 1;
		
		confparams_cpr->pw_relBoundRatio = 1E-3;
		confparams_cpr->segment_size = 36;
		
		confparams_cpr->pwr_type = SZ_PWR_MIN_TYPE;
		
		confparams_cpr->snapshotCmprStep = 5;
		
		confparams_cpr->withRegression = SZ_WITH_LINEAR_REGRESSION;
	
		confparams_cpr->randomAccess = 0; //0: no random access , 1: support random access
	
		confparams_cpr->protectValueRange = 0;
	
		return SZ_SCES;
	}
    
    if (access(sz_cfgFile, F_OK) != 0)
    {
        printf("[SZ] Configuration file NOT accessible.\n");
        return SZ_NSCS;
    }
    
    //printf("[SZ] Reading SZ configuration file (%s) ...\n", sz_cfgFile);    
    ini = iniparser_load(sz_cfgFile);
    if (ini == NULL)
    {
        printf("[SZ] Iniparser failed to parse the conf. file.\n");
        return SZ_NSCS;
    }

	endianTypeString = iniparser_getstring(ini, "ENV:dataEndianType", "LITTLE_ENDIAN_DATA");
	if(strcmp(endianTypeString, "LITTLE_ENDIAN_DATA")==0)
		dataEndianType = LITTLE_ENDIAN_DATA;
	else if(strcmp(endianTypeString, "BIG_ENDIAN_DATA")==0)
		dataEndianType = BIG_ENDIAN_DATA;
	else
	{
		printf("Error: Wrong dataEndianType: please set it correctly in sz.config.\n");
		iniparser_freedict(ini);
		return SZ_NSCS;
	}

	// Reading/setting detection parameters
	
	par = iniparser_getstring(ini, "ENV:sol_name", NULL);
	snprintf(sol_name, 256, "%s", par);
	
    if(strcmp(sol_name, "SZ")==0)
		confparams_cpr->sol_ID = SZ;
	else if(strcmp(sol_name, "PASTRI")==0)
		confparams_cpr->sol_ID = PASTRI;
	else if(strcmp(sol_name, "SZ_Transpose")==0)
		confparams_cpr->sol_ID = SZ_Transpose;
	else{
		printf("[SZ] Error: wrong solution name (please check sz.config file), sol=%s\n", sol_name);
		iniparser_freedict(ini);
		return SZ_NSCS;
	}
	
	if(confparams_cpr->sol_ID==SZ || confparams_cpr->sol_ID==SZ_Transpose)
	{
		int max_quant_intervals = iniparser_getint(ini, "PARAMETER:max_quant_intervals", 65536);
		confparams_cpr->max_quant_intervals = max_quant_intervals;
		
		int quantization_intervals = (int)iniparser_getint(ini, "PARAMETER:quantization_intervals", 0);
		confparams_cpr->quantization_intervals = quantization_intervals;
		if(quantization_intervals>0)
		{
			updateQuantizationInfo(quantization_intervals);
			confparams_cpr->max_quant_intervals = max_quant_intervals = quantization_intervals;
			exe_params->optQuantMode = 0;
		}
		else //==0
		{
			confparams_cpr->maxRangeRadius = max_quant_intervals/2;

			exe_params->intvCapacity = confparams_cpr->maxRangeRadius*2;
			exe_params->intvRadius = confparams_cpr->maxRangeRadius;
			
			exe_params->optQuantMode = 1;
		}
		
		if(quantization_intervals%2!=0)
		{
			printf("Error: quantization_intervals must be an even number!\n");
			iniparser_freedict(ini);
			return SZ_NSCS;
		}
		
		confparams_cpr->predThreshold = (float)iniparser_getdouble(ini, "PARAMETER:predThreshold", 0);
		confparams_cpr->sampleDistance = (int)iniparser_getint(ini, "PARAMETER:sampleDistance", 0);
		
		modeBuf = iniparser_getstring(ini, "PARAMETER:szMode", NULL);
		if(modeBuf==NULL)
		{
			printf("[SZ] Error: Null szMode setting (please check sz.config file)\n");
			iniparser_freedict(ini);
			return SZ_NSCS;					
		}
		else if(strcmp(modeBuf, "SZ_BEST_SPEED")==0)
			confparams_cpr->szMode = SZ_BEST_SPEED;
		else if(strcmp(modeBuf, "SZ_DEFAULT_COMPRESSION")==0)
			confparams_cpr->szMode = SZ_DEFAULT_COMPRESSION;
		else if(strcmp(modeBuf, "SZ_BEST_COMPRESSION")==0)
			confparams_cpr->szMode = SZ_BEST_COMPRESSION;
		else
		{
			printf("[SZ] Error: Wrong szMode setting (please check sz.config file)\n");
			iniparser_freedict(ini);
			return SZ_NSCS;	
		}
		
		modeBuf = iniparser_getstring(ini, "PARAMETER:losslessCompressor", "ZSTD_COMPRESSOR");
		if(strcmp(modeBuf, "GZIP_COMPRESSOR")==0)
			confparams_cpr->losslessCompressor = GZIP_COMPRESSOR;
		else if(strcmp(modeBuf, "ZSTD_COMPRESSOR")==0)
			confparams_cpr->losslessCompressor = ZSTD_COMPRESSOR;
		else
		{
			printf("[SZ] Error: Wrong losslessCompressor setting (please check sz.config file)\n");\
			printf("No Such a lossless compressor: %s\n", modeBuf);
			iniparser_freedict(ini);
			return SZ_NSCS;	
		}		
		
		modeBuf = iniparser_getstring(ini, "PARAMETER:withLinearRegression", "YES");
		if(strcmp(modeBuf, "YES")==0 || strcmp(modeBuf, "yes")==0)
			confparams_cpr->withRegression = SZ_WITH_LINEAR_REGRESSION;
		else
			confparams_cpr->withRegression = SZ_NO_REGRESSION;
		
		modeBuf = iniparser_getstring(ini, "PARAMETER:gzipMode", "Gzip_BEST_SPEED");
		if(modeBuf==NULL)
		{
			printf("[SZ] Error: Null Gzip mode setting (please check sz.config file)\n");
			iniparser_freedict(ini);
			return SZ_NSCS;					
		}		
		else if(strcmp(modeBuf, "Gzip_NO_COMPRESSION")==0)
			confparams_cpr->gzipMode = 0;
		else if(strcmp(modeBuf, "Gzip_BEST_SPEED")==0)
			confparams_cpr->gzipMode = 1;
		else if(strcmp(modeBuf, "Gzip_BEST_COMPRESSION")==0)
			confparams_cpr->gzipMode = 9;
		else if(strcmp(modeBuf, "Gzip_DEFAULT_COMPRESSION")==0)
			confparams_cpr->gzipMode = -1;
		else
		{
			printf("[SZ] Error: Wrong gzip Mode (please check sz.config file)\n");
			return SZ_NSCS;
		}
		
		modeBuf = iniparser_getstring(ini, "PARAMETER:zstdMode", "Zstd_HIGH_SPEED");		
		if(modeBuf==NULL)
		{
			printf("[SZ] Error: Null Zstd mode setting (please check sz.config file)\n");
			iniparser_freedict(ini);
			return SZ_NSCS;					
		}		
		else if(strcmp(modeBuf, "Zstd_BEST_SPEED")==0)
			confparams_cpr->gzipMode = 1;
		else if(strcmp(modeBuf, "Zstd_HIGH_SPEED")==0)
			confparams_cpr->gzipMode = 3;
		else if(strcmp(modeBuf, "Zstd_HIGH_COMPRESSION")==0)
			confparams_cpr->gzipMode = 19;
		else if(strcmp(modeBuf, "Zstd_BEST_COMPRESSION")==0)
			confparams_cpr->gzipMode = 22;			
		else if(strcmp(modeBuf, "Zstd_DEFAULT_COMPRESSION")==0)
			confparams_cpr->gzipMode = 3;
		else
		{
			printf("[SZ] Error: Wrong zstd Mode (please check sz.config file)\n");
			return SZ_NSCS;
		}		
		
		modeBuf = iniparser_getstring(ini, "PARAMETER:protectValueRange", "YES");
		if(strcmp(modeBuf, "YES")==0)
			confparams_cpr->protectValueRange = 1;
		else
			confparams_cpr->protectValueRange = 0;
		
		confparams_cpr->randomAccess = (int)iniparser_getint(ini, "PARAMETER:randomAccess", 0);
		
		//TODO
		confparams_cpr->snapshotCmprStep = (int)iniparser_getint(ini, "PARAMETER:snapshotCmprStep", 5);
				
		errBoundMode = iniparser_getstring(ini, "PARAMETER:errorBoundMode", NULL);
		if(errBoundMode==NULL)
		{
			printf("[SZ] Error: Null error bound setting (please check sz.config file)\n");
			iniparser_freedict(ini);
			return SZ_NSCS;				
		}
		else if(strcmp(errBoundMode,"ABS")==0||strcmp(errBoundMode,"abs")==0)
			confparams_cpr->errorBoundMode=ABS;
		else if(strcmp(errBoundMode, "REL")==0||strcmp(errBoundMode,"rel")==0)
			confparams_cpr->errorBoundMode=REL;
		else if(strcmp(errBoundMode, "VR_REL")==0||strcmp(errBoundMode, "vr_rel")==0)
			confparams_cpr->errorBoundMode=REL;
		else if(strcmp(errBoundMode, "ABS_AND_REL")==0||strcmp(errBoundMode, "abs_and_rel")==0)
			confparams_cpr->errorBoundMode=ABS_AND_REL;
		else if(strcmp(errBoundMode, "ABS_OR_REL")==0||strcmp(errBoundMode, "abs_or_rel")==0)
			confparams_cpr->errorBoundMode=ABS_OR_REL;
		else if(strcmp(errBoundMode, "PW_REL")==0||strcmp(errBoundMode, "pw_rel")==0)
			confparams_cpr->errorBoundMode=PW_REL;
		else if(strcmp(errBoundMode, "PSNR")==0||strcmp(errBoundMode, "psnr")==0)
			confparams_cpr->errorBoundMode=PSNR;
		else if(strcmp(errBoundMode, "ABS_AND_PW_REL")==0||strcmp(errBoundMode, "abs_and_pw_rel")==0)
			confparams_cpr->errorBoundMode=ABS_AND_PW_REL;
		else if(strcmp(errBoundMode, "ABS_OR_PW_REL")==0||strcmp(errBoundMode, "abs_or_pw_rel")==0)
			confparams_cpr->errorBoundMode=ABS_OR_PW_REL;
		else if(strcmp(errBoundMode, "REL_AND_PW_REL")==0||strcmp(errBoundMode, "rel_and_pw_rel")==0)
			confparams_cpr->errorBoundMode=REL_AND_PW_REL;
		else if(strcmp(errBoundMode, "REL_OR_PW_REL")==0||strcmp(errBoundMode, "rel_or_pw_rel")==0)
			confparams_cpr->errorBoundMode=REL_OR_PW_REL;
		else if(strcmp(errBoundMode, "NORM")==0||strcmp(errBoundMode, "norm")==0)
			confparams_cpr->errorBoundMode=NORM;
		else
		{
			printf("[SZ] Error: Wrong error bound mode (please check sz.config file)\n");
			iniparser_freedict(ini);
			return SZ_NSCS;
		}
		
		confparams_cpr->absErrBound = (double)iniparser_getdouble(ini, "PARAMETER:absErrBound", 0);
		confparams_cpr->relBoundRatio = (double)iniparser_getdouble(ini, "PARAMETER:relBoundRatio", 0);
		confparams_cpr->psnr = (double)iniparser_getdouble(ini, "PARAMETER:psnr", 0);
		confparams_cpr->normErr = (double)iniparser_getdouble(ini, "PARAMETER:normErr", 0);
		confparams_cpr->pw_relBoundRatio = (double)iniparser_getdouble(ini, "PARAMETER:pw_relBoundRatio", 0);
		confparams_cpr->segment_size = (int)iniparser_getint(ini, "PARAMETER:segment_size", 0);
		confparams_cpr->accelerate_pw_rel_compression = (int)iniparser_getint(ini, "PARAMETER:accelerate_pw_rel_compression", 1);
		
		modeBuf = iniparser_getstring(ini, "PARAMETER:pwr_type", "MIN");
		
		if(strcmp(modeBuf, "MIN")==0)
			confparams_cpr->pwr_type = SZ_PWR_MIN_TYPE;
		else if(strcmp(modeBuf, "AVG")==0)
			confparams_cpr->pwr_type = SZ_PWR_AVG_TYPE;
		else if(strcmp(modeBuf, "MAX")==0)
			confparams_cpr->pwr_type = SZ_PWR_MAX_TYPE;
		else if(modeBuf!=NULL)
		{
			printf("[SZ] Error: Wrong pwr_type setting (please check sz.config file).\n");
			iniparser_freedict(ini);
			return SZ_NSCS;	
		}
		else //by default
			confparams_cpr->pwr_type = SZ_PWR_AVG_TYPE;
    
		//initialization for Huffman encoding
		//SZ_Reset();	
	}
	else if(confparams_cpr->sol_ID == PASTRI)
	{//load parameters for PSTRI
		pastri_par.bf[0] = (int)iniparser_getint(ini, "PARAMETER:basisFunction_0", 0);		
		pastri_par.bf[1] = (int)iniparser_getint(ini, "PARAMETER:basisFunction_1", 0);		
		pastri_par.bf[2] = (int)iniparser_getint(ini, "PARAMETER:basisFunction_2", 0);		
		pastri_par.bf[3] = (int)iniparser_getint(ini, "PARAMETER:basisFunction_3", 0);
		pastri_par.numBlocks = (int)iniparser_getint(ini, "PARAMETER:numBlocks", 0);		
		confparams_cpr->absErrBound = pastri_par.originalEb = (double)iniparser_getdouble(ini, "PARAMETER:absErrBound", 1E-3);
	}
	
    iniparser_freedict(ini);
    return SZ_SCES;
}

/*-------------------------------------------------------------------------*/
/**
    @brief      It reads and tests the configuration given.
    @return     integer         1 if successfull.

    This function reads the configuration file. Then test that the
    configuration parameters are correct (including directories).

 **/
/*-------------------------------------------------------------------------*/
int SZ_LoadConf(const char* sz_cfgFile) {
    int res = SZ_ReadConf(sz_cfgFile);
    if (res != SZ_SCES)
    {
        printf("[SZ] ERROR: Impossible to read configuration.\n");
        return SZ_NSCS;
    }
    return SZ_SCES;
}

int checkVersion(char* version)
{
	int i = 0;
	for(;i<3;i++)
		if(version[i]!=versionNumber[i])
			return 0;
	return 1;
}

inline int computeVersion(int major, int minor, int revision)
{
	return major*10000+minor*100+revision;
}

int checkVersion2(char* version)
{
	int major = version[0];
	int minor = version[1];
	int revision = version[2];
	
	int preVersion = 20108;
	int givenVersion = computeVersion(major, minor, revision);
	//int currentVersion = computeVersion(SZ_VER_MAJOR, SZ_VER_MINOR, SZ_VER_REVISION);
	if(givenVersion < preVersion) //only for old version (older than 2.1.8), we will check whether version is consistent exactly.
		return checkVersion(version);
	return 1;
}

void initSZ_TSC()
{
	sz_tsc = (sz_tsc_metadata*)malloc(sizeof(sz_tsc_metadata));
	memset(sz_tsc, 0, sizeof(sz_tsc_metadata));
	/*sprintf(sz_tsc->metadata_filename, "sz_tsc_metainfo.txt");
	sz_tsc->metadata_file = fopen(sz_tsc->metadata_filename, "wb");
	if (sz_tsc->metadata_file == NULL)
	{
		printf("Failed to open sz_tsc_metainfo.txt file for writing metainfo.\n");
		exit(1);
	}
	fputs("#metadata of the time-step based compression\n", sz_tsc->metadata_file);	*/
}

/*double fabs(double value)
{
	if(value<0)
		return -value;
	else
		return value;
}*/
