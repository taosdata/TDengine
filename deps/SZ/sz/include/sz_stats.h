/**
 *  @file ByteToolkit.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the ByteToolkit.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _STATS_H
#define _STATS_H

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct sz_stats
{
	int use_mean;
	
	size_t blockSize;
	
	float lorenzoPercent;
	float regressionPercent;
	size_t lorenzoBlocks;
	size_t regressionBlocks;
	size_t totalBlocks;
	
	//size_t huffmanTreeHeight;
	size_t huffmanTreeSize; //before the final zstd
	size_t huffmanCodingSize; //before the final zstd
	float huffmanCompressionRatio;
	int huffmanNodeCount;
		
	size_t unpredictCount;
	float unpredictPercent;
	
	float zstdCompressionRatio; //not available yet
	
} sz_stats;

extern sz_stats sz_stat;


void writeBlockInfo(int use_mean, size_t blockSize, size_t regressionBlocks, size_t totalBlocks);
void writeHuffmanInfo(size_t huffmanTreeSize, size_t huffmanCodingSize, size_t totalDataSize, int huffmanNocdeCount);
void writeZstdCompressionRatio(float zstdCompressionRatio);
void writeUnpredictDataCounts(size_t unpredictCount, size_t totalNumElements);
void printSZStats();

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _STATS_H  ----- */
