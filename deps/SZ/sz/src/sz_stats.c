#include <sz_stats.h>

sz_stats sz_stat;

void writeBlockInfo(int use_mean, size_t blockSize, size_t regressionBlocks, size_t totalBlocks)
{
	sz_stat.use_mean = use_mean;
	sz_stat.blockSize = blockSize;
	sz_stat.lorenzoBlocks = totalBlocks - regressionBlocks;
	sz_stat.regressionBlocks = regressionBlocks;
	sz_stat.totalBlocks = totalBlocks;
	sz_stat.lorenzoPercent = 1.0f*sz_stat.lorenzoBlocks/(float)totalBlocks;
	sz_stat.regressionPercent = 1.0f*regressionBlocks/(float)totalBlocks;
}

void writeHuffmanInfo(size_t huffmanTreeSize, size_t huffmanCodingSize, size_t totalDataSize, int huffmanNodeCount)
{
	sz_stat.huffmanTreeSize = huffmanTreeSize;
	sz_stat.huffmanCodingSize = huffmanCodingSize;
	sz_stat.huffmanCompressionRatio = 1.0f*totalDataSize/(huffmanTreeSize+huffmanCodingSize);
	sz_stat.huffmanNodeCount = huffmanNodeCount;
}

void writeZstdCompressionRatio(float zstdCompressionRatio)
{
	sz_stat.zstdCompressionRatio = zstdCompressionRatio;
}	


void writeUnpredictDataCounts(size_t unpredictCount, size_t totalNumElements)
{
	sz_stat.unpredictCount = unpredictCount;
	sz_stat.unpredictPercent = 1.0f*unpredictCount/totalNumElements;
}

void printSZStats()
{
	printf("===============stats about sz================\n");
	if(sz_stat.use_mean)
		printf("use_mean:                  YES\n");
	else
		printf("use_mean:                  NO\n");
		
	printf("blockSize                  %zu\n", sz_stat.blockSize);
	printf("lorenzoPercent             %f\n", sz_stat.lorenzoPercent);
	printf("regressionPercent          %f\n", sz_stat.regressionPercent);
	printf("lorenzoBlocks              %zu\n", sz_stat.lorenzoBlocks);
	printf("regressionBlocks           %zu\n", sz_stat.regressionBlocks);
	printf("totalBlocks                %zu\n", sz_stat.totalBlocks);
	
	printf("huffmanTreeSize            %zu\n", sz_stat.huffmanTreeSize);
	printf("huffmanCodingSize          %zu\n", sz_stat.huffmanCodingSize);
	printf("huffmanCompressionRatio    %f\n", sz_stat.huffmanCompressionRatio);
	printf("huffmanNodeCount           %d\n", sz_stat.huffmanNodeCount);
	
	//printf("zstdCompressionRatio       %f\n", sz_stat.zstdCompressionRatio);

	printf("unpredictCount             %zu\n", sz_stat.unpredictCount);
	printf("unpredictPercent           %f\n", sz_stat.unpredictPercent);
}
