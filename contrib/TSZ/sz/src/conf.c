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

//
// set default value
//
void setDefaulParams(sz_exedata* exedata, sz_params* params)
{
	// sz_params
	if(params)
	{
		// first important
		params->errorBoundMode = SZ_ABS;
		params->absErrBound    = 1E-8;
		params->absErrBoundDouble = 1E-16;
		params->max_quant_intervals = 500;
		params->quantization_intervals = 100;
		params->losslessCompressor = ZSTD_COMPRESSOR; //other option: GZIP_COMPRESSOR;

        // second important
		params->sol_ID = SZ;		
		params->maxRangeRadius = params->max_quant_intervals/2;		
		params->predThreshold = 0.99;
		params->sampleDistance = 100;
		params->szMode = SZ_BEST_COMPRESSION;

        // other
		params->psnr = 90;
		params->relBoundRatio = 1E-8;
		params->accelerate_pw_rel_compression = 1;
		params->pw_relBoundRatio = 1E-3;
		params->segment_size = 36;
		params->pwr_type = SZ_PWR_MIN_TYPE;
		params->snapshotCmprStep = 5;
		params->withRegression = SZ_WITH_LINEAR_REGRESSION;
		params->randomAccess = 0; //0: no random access , 1: support random access
		params->protectValueRange = 0;		
		params->plus_bits = 3;
	}

    // sz_exedata
	if(exedata)
	{
		exedata->optQuantMode = 1;
		exedata->SZ_SIZE_TYPE = 4;
		if(params)
		{
			exedata->intvCapacity = params->maxRangeRadius*2;
			exedata->intvRadius   = params->maxRangeRadius;
		}
		else
		{
			exedata->intvCapacity = 500;
			exedata->intvRadius   = 200;
		}
	}
	
}

 
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
	if(confparams_cpr == NULL)
	   confparams_cpr = (sz_params*)malloc(sizeof(sz_params));    
	if(exe_params == NULL)
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
    
    
	// default option
    if(sz_cfgFile == NULL)
    {
		dataEndianType = LITTLE_ENDIAN_DATA;
		setDefaulParams(exe_params, confparams_cpr);
	
		return SZ_SUCCESS;
	}
    
    //printf("[SZ] Reading SZ configuration file (%s) ...\n", sz_cfgFile);    
    ini = iniparser_load(sz_cfgFile);
    if (ini == NULL)
    {
        printf("[SZ] Iniparser failed to parse the conf. file.\n");
        return SZ_FAILED;
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
		return SZ_FAILED;
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
		return SZ_FAILED;
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
			return SZ_FAILED;
		}
		
		confparams_cpr->predThreshold = (float)iniparser_getdouble(ini, "PARAMETER:predThreshold", 0);
		confparams_cpr->sampleDistance = (int)iniparser_getint(ini, "PARAMETER:sampleDistance", 0);
		
		modeBuf = iniparser_getstring(ini, "PARAMETER:szMode", NULL);
		if(modeBuf==NULL)
		{
			printf("[SZ] Error: Null szMode setting (please check sz.config file)\n");
			iniparser_freedict(ini);
			return SZ_FAILED;					
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
			return SZ_FAILED;	
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
			return SZ_FAILED;	
		}		
		
		modeBuf = iniparser_getstring(ini, "PARAMETER:withLinearRegression", "YES");
		if(strcmp(modeBuf, "YES")==0 || strcmp(modeBuf, "yes")==0)
			confparams_cpr->withRegression = SZ_WITH_LINEAR_REGRESSION;
		else
			confparams_cpr->withRegression = SZ_NO_REGRESSION;
						
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
			return SZ_FAILED;				
		}
		else if(strcmp(errBoundMode,"ABS")==0||strcmp(errBoundMode,"abs")==0)
			confparams_cpr->errorBoundMode=SZ_ABS;
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
			return SZ_FAILED;
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
			return SZ_FAILED;	
		}
		else //by default
			confparams_cpr->pwr_type = SZ_PWR_AVG_TYPE;
    
		//initialization for Huffman encoding
		//SZ_Reset();	
	}

	
    iniparser_freedict(ini);
    return SZ_SUCCESS;
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
    if (res != SZ_SUCCESS)
    {
        printf("[SZ] ERROR: Impossible to read configuration.\n");
        return SZ_FAILED;
    }
    return SZ_SUCCESS;
}
