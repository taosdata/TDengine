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
int32_t  mndGetIdxsByTagName(SMnode *pMnode, SStbObj *pStb, char *tagName, SIdxObj *pIdx);
int32_t  mndGetTableIdx(SMnode *pMnode, char *tbFName, STableIndexRsp *rsp, bool *exist);

int32_t mndRetrieveTagIdx(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
int32_t mndProcessDropTagIdxReq(SRpcMsg *pReq);

int32_t mndSetCreateIdxPrepareLogs(SMnode *pMnode, STrans *pTrans, SIdxObj *pIdx);
int32_t mndSetCreateIdxCommitLogs(SMnode *pMnode, STrans *pTrans, SIdxObj *pIdx);
int32_t mndSetDropIdxPrepareLogs(SMnode *pMnode, STrans *pTrans, SIdxObj *pIdx);
int32_t mndSetDropIdxCommitLogs(SMnode *pMnode, STrans *pTrans, SIdxObj *pIdx);

int32_t mndSetAlterIdxPrepareLogs(SMnode *pMnode, STrans *pTrans, SIdxObj *pIdx);
int32_t mndSetAlterIdxCommitLogs(SMnode *pMnode, STrans *pTrans, SIdxObj *pIdx);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_IDX_H_*/
