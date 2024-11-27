#include "streamInt.h"

/**
 *  pre-request: checkpoint interval should be 60s
 * @param pTask
 * @param checkpointId
 */
void chkptFailedByRetrieveReqToSource(SStreamTask* pTask, int64_t checkpointId) {
  streamMutexLock(&pTask->lock);

  // set current checkpoint failed immediately, set failed checkpoint id before clear the checkpoint info
  streamTaskSetFailedCheckpointId(pTask, checkpointId);
  streamMutexUnlock(&pTask->lock);

  // the checkpoint interval should be 60s, and the next checkpoint req should be issued by mnode
  taosMsleep(65*1000);
}