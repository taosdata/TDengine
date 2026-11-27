/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
