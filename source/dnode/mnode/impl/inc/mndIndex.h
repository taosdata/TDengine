#ifndef _TD_MND_IDX_H_
#define _TD_MND_IDX_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t  mndInitIdx(SMnode *pMnode);
void     mndCleanupIdx(SMnode *pMnode);
SIdxObj *mndAcquireIdx(SMnode *pMnode, char *Name);
void     mndReleaseIdx(SMnode *pMnode, SIdxObj *pSma);
int32_t  mndDropIdxsByStb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb);
int32_t  mndDropIdxsByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t  mndGetTableIdx(SMnode *pMnode, char *tbFName, STableIndexRsp *rsp, bool *exist);

int32_t mndRetrieveTagIdx(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_IDX_H_*/