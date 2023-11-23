//
// Created by mingming wanng on 2023/11/15.
//

#ifndef TDENGINE_STREAM_H
#define TDENGINE_STREAM_H

#define STREAM_EXEC_EXTRACT_DATA_IN_WAL_ID (-1)
#define STREAM_EXEC_START_ALL_TASKS_ID     (-2)
#define STREAM_EXEC_RESTART_ALL_TASKS_ID   (-3)

typedef struct STaskUpdateEntry {
  int64_t streamId;
  int32_t taskId;
  int32_t transId;
} STaskUpdateEntry;

#endif  // TDENGINE_STREAM_H
