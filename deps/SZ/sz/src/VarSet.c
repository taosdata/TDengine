/**
 *  @file Variable.c
 *  @author Sheng Di
 *  @date July, 2016
 *  @brief TypeManager is used to manage the type array: parsing of the bytes and other types in between.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "VarSet.h"
#include "sz.h"

void free_Variable_keepOriginalData(SZ_Variable* v)
{
	if(v->varName!=NULL)
		free(v->varName);	
	if(v->compressedBytes!=NULL)
		free(v->compressedBytes);
	if(v->multisteps!=NULL)
		free_multisteps(v->multisteps);	
	free(v);
}

/**
 * 
 * @deprecated
 * */
void free_Variable_keepCompressedBytes(SZ_Variable* v)
{
	if(v->varName!=NULL)
		free(v->varName);
	if(v->data!=NULL)
		free(v->data);
	if(v->multisteps!=NULL)
		free_multisteps(v->multisteps);	
	free(v);
}

void free_Variable_all(SZ_Variable* v)
{
	if(v->varName!=NULL)
		free(v->varName);
	if(v->data!=NULL)
		free(v->data);
	if(v->compressedBytes!=NULL)
		free(v->compressedBytes);
	if(v->multisteps!=NULL)
		free_multisteps(v->multisteps);
	free(v);
}

void SZ_batchAddVar(int var_id, char* varName, int dataType, void* data, 
			int errBoundMode, double absErrBound, double relBoundRatio, double pwRelBoundRatio, 
			size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{	
	if(sz_varset==NULL)
	{
		sz_varset = (SZ_VarSet*)malloc(sizeof(SZ_VarSet));
		sz_varset->header = (SZ_Variable*)malloc(sizeof(SZ_Variable));
		sz_varset->header->next = NULL;
		sz_varset->lastVar = sz_varset->header;
		sz_varset->count = 0;		
	}
	
	SZ_Variable* var = (SZ_Variable*)malloc(sizeof(SZ_Variable));
	memset(var, 0, sizeof(SZ_Variable));
	var->var_id = var_id;
	var->varName = (char*)malloc(strlen(varName)+1);
	memcpy(var->varName, varName, strlen(varName)+1);
	//var->varName = varName;
	var->dataType = dataType;
	var->r5 = r5;
	var->r4 = r4;
	var->r3 = r3;
	var->r2 = r2;
	var->r1 = r1;
	var->errBoundMode = errBoundMode;
	var->absErrBound = absErrBound;
	var->relBoundRatio = relBoundRatio;
	var->pwRelBoundRatio = pwRelBoundRatio;
	var->data = data;
	
	var->multisteps = (sz_multisteps*)malloc(sizeof(sz_multisteps));
	memset(var->multisteps, 0, sizeof(sz_multisteps));
	
	size_t dataLen = computeDataLength(r5, r4, r3, r2, r1);
	if(dataType==SZ_FLOAT)
	{
		var->multisteps->hist_data = (float*)malloc(sizeof(float)*dataLen);
		memset(var->multisteps->hist_data, 0, sizeof(float)*dataLen);
	}
	else if(dataType==SZ_DOUBLE)
	{
		var->multisteps->hist_data = (double*)malloc(sizeof(double)*dataLen);
		memset(var->multisteps->hist_data, 0, sizeof(double)*dataLen);
	}
	var->compressedBytes = NULL;
	var->next = NULL;
	
	sz_varset->count ++;
	sz_varset->lastVar->next = var;
	sz_varset->lastVar = var;
}

int SZ_batchDelVar_ID(int var_id)
{
	int state = SZ_batchDelVar_ID_vset(sz_varset, var_id);
	return state;
}

int SZ_batchDelVar(char* varName)
{
	int state = SZ_batchDelVar_vset(sz_varset, varName);
	return state;
}

int SZ_batchDelVar_ID_vset(SZ_VarSet* vset, int var_id)
{
	int delSuccess = SZ_NSCS;
	SZ_Variable* p = vset->header;
	SZ_Variable* q = p->next;
	while(q != NULL)
	{
		if(q->var_id == var_id)
		{
			p->next = q->next;
			//free_Variable_all(q);
			free_Variable_keepOriginalData(q);
			vset->count --;
			delSuccess = SZ_SCES;
			if(q->next==NULL) //means that q is the last variable
				vset->lastVar = p;			
			break;
		}
			
		p = p->next;
		q = q->next;	
	}
	
	return delSuccess;	
}

int SZ_batchDelVar_vset(SZ_VarSet* vset, char* varName)
{
	int delSuccess = SZ_NSCS;
	SZ_Variable* p = vset->header;
	SZ_Variable* q = p->next;
	while(q != NULL)
	{
		int cmpResult = strcmp(q->varName, varName);
		if(cmpResult==0)
		{
			p->next = q->next;
			//free_Variable_all(q);
			free_Variable_keepOriginalData(q);
			vset->count --;
			delSuccess = SZ_SCES;
			break;
		}
		p = p->next;
		q = q->next;	
	}
	
	return delSuccess;
}

SZ_Variable* SZ_searchVar(char* varName)
{
	SZ_Variable* p = sz_varset->header->next;
	while(p!=NULL)
	{
		int checkName = strcmp(p->varName, varName);
		if(checkName==0)
			return p;
		p = p->next;
	}	
	return NULL;
}

void* SZ_getVarData(char* varName, size_t *r5, size_t *r4, size_t *r3, size_t *r2, size_t *r1)
{
	SZ_Variable* v = SZ_searchVar(varName);
	*r5 = v->r5;
	*r4 = v->r4;
	*r3 = v->r3;
	*r2 = v->r2;
	*r1 = v->r1;
	return (void*)v->data;
}

/**
 * 
 * int mode: SZ_MAINTAIN_VAR_DATA, Z_DESTROY_WHOLE_VARSET
 * */
void SZ_freeVarSet(int mode)
{
	free_VarSet_vset(sz_varset, mode);
}

//free_VarSet will completely destroy the SZ_VarSet, so don't do it until you really don't need it any more!
/**
 * 
 * int mode: SZ_MAINTAIN_VAR_DATA, Z_DESTROY_WHOLE_VARSET
 * */
void free_VarSet_vset(SZ_VarSet *vset, int mode)
{
	if(vset==NULL)
		return;
	SZ_Variable *p = vset->header;
	while(p->next!=NULL)
	{
		SZ_Variable *q = p->next;
		p->next = q->next;
		if(mode==SZ_MAINTAIN_VAR_DATA)
			free_Variable_keepOriginalData(q);
		else if(mode==SZ_DESTROY_WHOLE_VARSET)
			free_Variable_all(q);
	}
	free(sz_varset->header);
	free(vset);
}

void free_multisteps(sz_multisteps* multisteps)
{
	if(multisteps->hist_data!=NULL)
		free(multisteps->hist_data);
	free(multisteps);
}

inline int checkVarID(unsigned char cur_var_id, unsigned char* var_ids, int var_count)
{
	int j = 0;
	for(j=0;j<var_count;j++)
	{
		if(var_ids[j]==cur_var_id)
			return 1;
	}
	return 0;
}

SZ_Variable* SZ_getVariable(int var_id)
{
	SZ_Variable* p = sz_varset->header->next;
	while(p!=NULL)
	{
		if(var_id == p->var_id)
			return p;
		p = p->next;
	}	
	return NULL;
} 
