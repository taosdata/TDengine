/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include <executorimpl.h>
#include <gtest/gtest.h>
#include <tglobal.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "tep.h"
#include "trpc.h"
#include "stub.h"
#include "executor.h"

/**
{
	"Id":	{
		"QueryId":	1.3108161807422521e+19,
		"TemplateId":	0,
		"SubplanId":	0
	},
	"Node":	{
		"Name":	"TableScan",
		"Targets":	[{
				"Base":	{
					"Schema":	{
						"Type":	9,
						"ColId":	5000,
						"Bytes":	8
					},
					"Columns":	[{
							"TableId":	1,
							"Flag":	0,
							"Info":	{
								"ColId":	1,
								"Type":	9,
								"Bytes":	8
							}
						}],
					"InterBytes":	0
				},
				"Expr":	{
					"Type":	4,
					"Column":	{
						"Type":	9,
						"ColId":	1,
						"Bytes":	8
					}
				}
			}, {
				"Base":	{
					"Schema":	{
						"Type":	4,
						"ColId":	5001,
						"Bytes":	4
					},
					"Columns":	[{
							"TableId":	1,
							"Flag":	0,
							"Info":	{
								"ColId":	2,
								"Type":	4,
								"Bytes":	4
							}
						}],
					"InterBytes":	0
				},
				"Expr":	{
					"Type":	4,
					"Column":	{
						"Type":	4,
						"ColId":	2,
						"Bytes":	4
					}
				}
			}],
		"InputSchema":	[{
				"Type":	9,
				"ColId":	5000,
				"Bytes":	8
			}, {
				"Type":	4,
				"ColId":	5001,
				"Bytes":	4
			}],
		"TableScan":	{
			"TableId":	1,
			"TableType":	2,
			"Flag":	0,
			"Window":	{
				"StartKey":	-9.2233720368547758e+18,
				"EndKey":	9.2233720368547758e+18
			}
		}
	},
	"DataSink":	{
		"Name":	"Dispatch",
		"Dispatch":	{
		}
	}
}
 */

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, build_executor_tree_Test) {


  const char* msg = "{\n"
      "\t\"Id\":\t{\n"
      "\t\t\"QueryId\":\t1.3108161807422521e+19,\n"
      "\t\t\"TemplateId\":\t0,\n"
      "\t\t\"SubplanId\":\t0\n"
      "\t},\n"
      "\t\"Node\":\t{\n"
      "\t\t\"Name\":\t\"TableScan\",\n"
      "\t\t\"Targets\":\t[{\n"
      "\t\t\t\t\"Base\":\t{\n"
      "\t\t\t\t\t\"Schema\":\t{\n"
      "\t\t\t\t\t\t\"Type\":\t9,\n"
      "\t\t\t\t\t\t\"ColId\":\t5000,\n"
      "\t\t\t\t\t\t\"Bytes\":\t8\n"
      "\t\t\t\t\t},\n"
      "\t\t\t\t\t\"Columns\":\t[{\n"
      "\t\t\t\t\t\t\t\"TableId\":\t1,\n"
      "\t\t\t\t\t\t\t\"Flag\":\t0,\n"
      "\t\t\t\t\t\t\t\"Info\":\t{\n"
      "\t\t\t\t\t\t\t\t\"ColId\":\t1,\n"
      "\t\t\t\t\t\t\t\t\"Type\":\t9,\n"
      "\t\t\t\t\t\t\t\t\"Bytes\":\t8\n"
      "\t\t\t\t\t\t\t}\n"
      "\t\t\t\t\t\t}],\n"
      "\t\t\t\t\t\"InterBytes\":\t0\n"
      "\t\t\t\t},\n"
      "\t\t\t\t\"Expr\":\t{\n"
      "\t\t\t\t\t\"Type\":\t4,\n"
      "\t\t\t\t\t\"Column\":\t{\n"
      "\t\t\t\t\t\t\"Type\":\t9,\n"
      "\t\t\t\t\t\t\"ColId\":\t1,\n"
      "\t\t\t\t\t\t\"Bytes\":\t8\n"
      "\t\t\t\t\t}\n"
      "\t\t\t\t}\n"
      "\t\t\t}, {\n"
      "\t\t\t\t\"Base\":\t{\n"
      "\t\t\t\t\t\"Schema\":\t{\n"
      "\t\t\t\t\t\t\"Type\":\t4,\n"
      "\t\t\t\t\t\t\"ColId\":\t5001,\n"
      "\t\t\t\t\t\t\"Bytes\":\t4\n"
      "\t\t\t\t\t},\n"
      "\t\t\t\t\t\"Columns\":\t[{\n"
      "\t\t\t\t\t\t\t\"TableId\":\t1,\n"
      "\t\t\t\t\t\t\t\"Flag\":\t0,\n"
      "\t\t\t\t\t\t\t\"Info\":\t{\n"
      "\t\t\t\t\t\t\t\t\"ColId\":\t2,\n"
      "\t\t\t\t\t\t\t\t\"Type\":\t4,\n"
      "\t\t\t\t\t\t\t\t\"Bytes\":\t4\n"
      "\t\t\t\t\t\t\t}\n"
      "\t\t\t\t\t\t}],\n"
      "\t\t\t\t\t\"InterBytes\":\t0\n"
      "\t\t\t\t},\n"
      "\t\t\t\t\"Expr\":\t{\n"
      "\t\t\t\t\t\"Type\":\t4,\n"
      "\t\t\t\t\t\"Column\":\t{\n"
      "\t\t\t\t\t\t\"Type\":\t4,\n"
      "\t\t\t\t\t\t\"ColId\":\t2,\n"
      "\t\t\t\t\t\t\"Bytes\":\t4\n"
      "\t\t\t\t\t}\n"
      "\t\t\t\t}\n"
      "\t\t\t}],\n"
      "\t\t\"InputSchema\":\t[{\n"
      "\t\t\t\t\"Type\":\t9,\n"
      "\t\t\t\t\"ColId\":\t5000,\n"
      "\t\t\t\t\"Bytes\":\t8\n"
      "\t\t\t}, {\n"
      "\t\t\t\t\"Type\":\t4,\n"
      "\t\t\t\t\"ColId\":\t5001,\n"
      "\t\t\t\t\"Bytes\":\t4\n"
      "\t\t\t}],\n"
      "\t\t\"TableScan\":\t{\n"
      "\t\t\t\"TableId\":\t1,\n"
      "\t\t\t\"TableType\":\t2,\n"
      "\t\t\t\"Flag\":\t0,\n"
      "\t\t\t\"Window\":\t{\n"
      "\t\t\t\t\"StartKey\":\t-9.2233720368547758e+18,\n"
      "\t\t\t\t\"EndKey\":\t9.2233720368547758e+18\n"
      "\t\t\t}\n"
      "\t\t}\n"
      "\t},\n"
      "\t\"DataSink\":\t{\n"
      "\t\t\"Name\":\t\"Dispatch\",\n"
      "\t\t\"Dispatch\":\t{\n"
      "\t\t}\n"
      "\t}\n"
      "}";

  SExecTaskInfo* pTaskInfo = nullptr;
  DataSinkHandle sinkHandle = nullptr;
  int32_t code = qCreateExecTask((void*) 1, 2, 1, NULL, (void**) &pTaskInfo, &sinkHandle);
}

#pragma GCC diagnostic pop