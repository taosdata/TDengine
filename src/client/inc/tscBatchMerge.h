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

#ifndef TDENGINE_TSCBATCHMERGE_H
#define TDENGINE_TSCBATCHMERGE_H

#include "hash.h"
#include "taosmsg.h"
#include "tarray.h"
#include "tscUtil.h"
#include "tsclient.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * A builder of SSubmitBlk.
 */
typedef struct SSubmitBlkBuilder {
  // the metadata of the SSubmitBlk.
  SSubmitBlk* metadata;
  
  // the array stores all the rows in a table, aka SArray<SMemRow>.
  SArray* rows;
  
} SSubmitBlkBuilder;

/**
 * The builder to build SSubmitMsg::blocks.
 */
typedef struct SSubmitMsgBlocksBuilder {
  // SHashObj<table_uid, SSubmitBlkBuilder*>.
  SHashObj* blockBuilders;
  int64_t vgId;
} SSubmitMsgBlocksBuilder;

/**
 * STableDataBlocksBuilder is a tool to build data blocks by append the existing data blocks in a vnode.
 */
typedef struct STableDataBlocksBuilder {
  SSubmitMsgBlocksBuilder* blocksBuilder;
  STableDataBlocks* firstBlock;
  int64_t vgId;
} STableDataBlocksBuilder;

/**
 * STableDataBlocksListBuilder is a tool to build vnode data blocks list by appending exist data blocks.
 */
typedef struct STableDataBlocksListBuilder {
  SHashObj* dataBlocksBuilders;
} STableDataBlocksListBuilder;

/**
 * A Builder to build SInsertStatementParam::pTableNameList.
 */
typedef struct STableNameListBuilder {
  // store the unsorted table names, SArray<SName*>.
  SArray* pTableNameList;
} STableNameListBuilder;

/**
 * Create a SSubmitBlkBuilder using exist metadata.
 * 
 * @param metadata  the metadata.
 * @return          the SSubmitBlkBuilder.
 */
SSubmitBlkBuilder* createSSubmitBlkBuilder(SSubmitBlk* metadata);

/**
 * Destroy the SSubmitBlkBuilder.
 * 
 * @param builder the SSubmitBlkBuilder.
 */
void destroySSubmitBlkBuilder(SSubmitBlkBuilder* builder);

/**
 * Append a SSubmitBlk* to the builder. The table uid in pBlock must be the same with the builder's.
 * 
 * @param builder  the SSubmitBlkBuilder.
 * @param pBlock   the pBlock to append.
 * @return         whether the append is success.
 */
bool appendSSubmitBlkBuilder(SSubmitBlkBuilder* builder, SSubmitBlk *pBlock);


/**
 * Build and write SSubmitBlk to `target`
 * 
 * @param builder   the SSubmitBlkBuilder.
 * @param target    the target to write.
 * @param nRows     the number of rows in SSubmitBlk*.
 * @return          the writen bytes.
 */
size_t writeSSubmitBlkBuilder(SSubmitBlkBuilder* builder, SSubmitBlk* target, size_t* nRows);

/**
 * Get the expected writen bytes of `writeSSubmitBlkBuilder`.
 * 
 * @param builder  the SSubmitBlkBuilder.
 * @return         the expected writen bytes of `writeSSubmitBlkBuilder`.
 */
size_t nWriteSSubmitBlkBuilder(SSubmitBlkBuilder* builder);

/**
 * Create a SSubmitMsgBuilder.
 * 
 * @param vgId  the vgId of SSubmitMsg.
 * @return      the SSubmitMsgBuilder.
 */
SSubmitMsgBlocksBuilder* createSSubmitMsgBuilder(int64_t vgId);

/**
 * Get the expected writen bytes of `writeSSubmitMsgBlocksBuilder`.
 * 
 * @param builder  the SSubmitMsgBlocksBuilder.
 * @return         the expected writen bytes of `writeSSubmitMsgBlocksBuilder`.
 */
size_t nWriteSSubmitMsgBuilder(SSubmitMsgBlocksBuilder* builder);

/**
 * Build and write SSubmitMsg::blocks to `pBlocks`
 * 
 * @param builder   the SSubmitBlkBuilder.
 * @param pBlocks   the target to write.
 * @param nRows     the number of row in SSubmitMsg::blocks.
 * @return          the writen bytes.
 */
size_t writeSSubmitMsgBlocksBuilder(SSubmitMsgBlocksBuilder* builder, SSubmitBlk* pBlocks, size_t* nRows);

/**
 * Get the number of block in SSubmitMsgBlocksBuilder.
 * @param builder the SSubmitMsgBlocksBuilder.
 * @return        the number of SSubmitBlk block.
 */
size_t nBlockSSubmitMsgBlocksBuilder(SSubmitMsgBlocksBuilder* builder);

/**
 * Destroy the SSubmitMsgBlocksBuilder.
 * 
 * @param builder the SSubmitMsgBlocksBuilder to destroy.
 */
void destroySSubmitMsgBuilder(SSubmitMsgBlocksBuilder* builder);

/**
 * Append SSubmitMsg* to the SSubmitMsgBlocksBuilder.
 * 
 * @param builder       the SSubmitMsgBlocksBuilder.
 * @param pBlocks       the SSubmitBlk in SSubmitMsg::blocks. 
 * @param nBlocks       the number of blocks in SSubmitMsg.
 * @return whether the append is success.
 */
bool appendSSubmitMsgBlocks(SSubmitMsgBlocksBuilder* builder, SSubmitBlk* pBlocks, size_t nBlocks);

/**
 * Create the STableDataBlocksBuilder.
 * 
 * @param vgId the vgId of STableDataBlocksBuilder.
 * @return the STableDataBlocksBuilder.
 */
STableDataBlocksBuilder* createSTableDataBlocksBuilder(int64_t vgId);

/**
 * Destroy the STableDataBlocksBuilder.
 * @param builder the STableDataBlocksBuilder.
 */
void destroySTableDataBlocksBuilder(STableDataBlocksBuilder *builder);

/**
 * Append a data blocks to STableDataBlocksBuilder.
 * @param builder    the STableDataBlocksBuilder.
 * @param dataBlocks the dataBlocks to append. the vgId of dataBlocks must be same with the STableDataBlocksBuilder.
 * @return           whether the append is success.
 */
bool appendSTableDataBlocksBuilder(STableDataBlocksBuilder* builder, STableDataBlocks* dataBlocks);

/**
 * Build the data blocks for single vnode.
 * @param builder   the STableDataBlocksBuilder.
 * @param nRows     the number of row in STableDataBlocks.
 * @return          the data blocks for single vnode.
 */
STableDataBlocks* buildSTableDataBlocksBuilder(STableDataBlocksBuilder* builder, size_t* nRows);

/**
 * Create the STableDataBlocksListBuilder.
 * 
 * @return the STableDataBlocksListBuilder.
 */
STableDataBlocksListBuilder* createSTableDataBlocksListBuilder();

/**
 * Destroy the STableDataBlocksListBuilder.
 * 
 * @param builder the STableDataBlocksListBuilder.
 */
void destroySTableDataBlocksListBuilder(STableDataBlocksListBuilder* builder);

/**
 * Append a data blocks to STableDataBlocksListBuilder.
 * 
 * @param builder       the STableDataBlocksListBuilder.
 * @param dataBlocks    the data blocks.
 * @return              whether the append is success.
 */
bool appendSTableDataBlocksListBuilder(STableDataBlocksListBuilder* builder, STableDataBlocks* dataBlocks);

/**
 * Build the vnode data blocks list.
 * 
 * @param builder   the STableDataBlocksListBuilder.
 * @param nTables   the number of table in vnode data blocks list.
 * @param nRows     the number of row in vnode data blocks list. 
 * @return          the vnode data blocks list.
 */
SArray* buildSTableDataBlocksListBuilder(STableDataBlocksListBuilder* builder, size_t* nTables, size_t* nRows);

/**
 * Create STableNameListBuilder.
 */
STableNameListBuilder* createSTableNameListBuilder();

/**
 * Destroy the STableNameListBuilder.
 * @param builder the STableNameListBuilder.
 */
void destroySTableNameListBuilder(STableNameListBuilder* builder);

/**
 * Insert a SName to builder.
 * 
 * @param builder  the STableNameListBuilder.
 * @param name     the table name.
 * @return         whether it is success.
 */
bool insertSTableNameListBuilder(STableNameListBuilder* builder, SName* name);

/**
 * Build the STable name list.
 * 
 * @param builder       the STableNameListBuilder.
 * @param numOfTables   the number of table.
 * @return              the STable name list.
 */
SName** buildSTableNameListBuilder(STableNameListBuilder* builder, size_t* numOfTables);

/**
 * Merge the KV-PayLoad SQL objects into single one. 
 * The statements here must be an insertion statement and no schema attached.
 * 
 * @param polls  the array of SSqlObj*.
 * @param nPolls the number of SSqlObj* in the array.
 * @param result the returned result. result is not null!
 * @return       the status code.
 */
int32_t tscMergeSSqlObjs(SSqlObj** polls, size_t nPolls, SSqlObj *result);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCBATCHMERGE_H
