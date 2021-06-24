/**
 *  @file VarSet.h
 *  @author Sheng Di
 *  @date July, 2016
 *  @brief Header file for the Variable.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _VarSet_H
#define _VarSet_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

typedef struct sz_multisteps
{
	char compressionType;
	int predictionMode;
	int lastSnapshotStep; //the previous snapshot step
	unsigned int currentStep; //current time step of the execution/simulation
	
	//void* ori_data; //original data pointer, which serve as the key for retrieving hist_data
	void* hist_data; //historical data in past time steps
} sz_multisteps;

typedef struct SZ_Variable
{
	unsigned char var_id;
	char* varName;
	char compressType; //102 means HZ; 101 means SZ 
	int dataType; //SZ_FLOAT or SZ_DOUBLE
	size_t r5;
	size_t r4;
	size_t r3;
	size_t r2;
	size_t r1;
	int errBoundMode;
	double absErrBound;
	double relBoundRatio;
	double pwRelBoundRatio;
	void* data;
	sz_multisteps *multisteps;
	unsigned char* compressedBytes;
	size_t compressedSize;
	struct SZ_Variable* next;
} SZ_Variable;

typedef struct SZ_VarSet
{
	unsigned short count;
	struct SZ_Variable *header;
	struct SZ_Variable *lastVar;
} SZ_VarSet;

void free_Variable_keepOriginalData(SZ_Variable* v);
void free_Variable_keepCompressedBytes(SZ_Variable* v);
void free_Variable_all(SZ_Variable* v);
void SZ_batchAddVar(int var_id, char* varName, int dataType, void* data, 
			int errBoundMode, double absErrBound, double relBoundRatio, double pwRelBoundRatio,
			size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);
int SZ_batchDelVar_vset(SZ_VarSet* vset, char* varName);
int SZ_batchDelVar(char* varName);
int SZ_batchDelVar_ID_vset(SZ_VarSet* vset, int var_id);
int SZ_batchDelVar_ID(int var_id);

SZ_Variable* SZ_searchVar(char* varName);
void* SZ_getVarData(char* varName, size_t *r5, size_t *r4, size_t *r3, size_t *r2, size_t *r1);

void free_VarSet_vset(SZ_VarSet *vset, int mode);
void SZ_freeVarSet(int mode);

void free_multisteps(sz_multisteps* multisteps);
int checkVarID(unsigned char cur_var_id, unsigned char* var_ids, int var_count);
SZ_Variable* SZ_getVariable(int var_id);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _VarSet_H  ----- */
