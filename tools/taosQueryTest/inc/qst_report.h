/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_REPORT_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_REPORT_H_

#include "qst_process.h"
#include "qst_runner.h"

EQstCode qst_report_refresh(const SQstShmMapping *mapping,
                            const SQstRunConfig *config);
EQstCode qst_log_event(const SQstProcessSlot *self, EQstCode code,
                       const char *event_name, SQstRef diagnostic_ref);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_REPORT_H_
