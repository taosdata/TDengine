#include "des.h"
#include "kcglue_des.h"

/* $Id: kcglue_des.c,v 1.2 2001/12/04 02:05:33 rjs3 Exp $
 * kclient and des have different definitions for key schedules
 * this file is to include in the kclient code without dragging in the des definitions
 */
int kcglue_des_key_sched(void *akey,void *asched)
{
	return des_key_sched(akey,asched);
}

void kcglue_des_ecb_encrypt(void *asrc,void *adest,void *asched,int direction)
{
	des_ecb_encrypt(asrc,adest,asched,direction);
}

void kcglue_des_pcbc_encrypt(void *asrc,void *adest,long length,void *asched,void *akey,int direction)
{
	des_pcbc_encrypt(asrc,adest,length,asched,akey,direction);
}

