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

#if defined(INCLUDE_AS_ENUM)  // enum define mode
  #undef OP_ENUM_MACRO
  #define OP_ENUM_MACRO(op)    OP_##op,
#elif defined(INCLUDE_AS_NAME)  // comment define mode
  #undef OP_ENUM_MACRO
  #define OP_ENUM_MACRO(op)    #op,
#else
  #error To use this include file, first define either INCLUDE_AS_ENUM or INCLUDE_AS_NAME
#endif

OP_ENUM_MACRO(StreamScan)
OP_ENUM_MACRO(TableScan)
OP_ENUM_MACRO(TableSeqScan)
OP_ENUM_MACRO(TagScan)
OP_ENUM_MACRO(SystemTableScan)
OP_ENUM_MACRO(StreamBlockScan)
OP_ENUM_MACRO(Aggregate)
OP_ENUM_MACRO(Project)
// OP_ENUM_MACRO(Groupby)
OP_ENUM_MACRO(Limit)
OP_ENUM_MACRO(SLimit)
OP_ENUM_MACRO(TimeWindow)
OP_ENUM_MACRO(SessionWindow)
OP_ENUM_MACRO(StateWindow)
OP_ENUM_MACRO(Fill)
OP_ENUM_MACRO(MultiTableAggregate)
OP_ENUM_MACRO(MultiTableTimeInterval)
OP_ENUM_MACRO(Filter)
OP_ENUM_MACRO(Distinct)
OP_ENUM_MACRO(Join)
OP_ENUM_MACRO(AllTimeWindow)
OP_ENUM_MACRO(AllMultiTableTimeInterval)
OP_ENUM_MACRO(Order)
OP_ENUM_MACRO(Exchange)
OP_ENUM_MACRO(SortedMerge)

//OP_ENUM_MACRO(TableScan)
