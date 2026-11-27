/**
 *  @file Huffman.h
 *  @author Sheng Di
 *  @date Aug., 2016
 *  @brief Header file for the exponential segment constructor.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _Huffman_H
#define _Huffman_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//Note: when changing the following settings, intvCapacity in sz.h should be changed as well.
//#define allNodes 131072
//#define stateNum 65536

//Upper bound of the node count accepted by the tree builder when the type array comes from a
//compressed payload. The encoder derives its node count from the quantization interval count
//(stateNum = 2*intervals, nodeCount = 2*stateNum-1), which stays far below this limit.
#define MAX_HUFF_TREE_NODES (1 << 20)

//Upper bound of the Huffman tree height accepted from a compressed payload. unpad_tree_uchar/ushort/
//uint rebuild the tree recursively, so a payload describing a degenerate chain of nodeCount nodes
//(nodeCount is bounded by MAX_HUFF_TREE_NODES) would nest that many frames and overflow the stack.
//The encoder's longest code is only tens of bits for the type histograms SZ produces, so this is a
//wide margin above any legitimate tree.
#define MAX_HUFF_TREE_DEPTH 1024

typedef struct node_t {
	struct node_t *left, *right;
	size_t freq;
	char t; //in_node:0; otherwise:1
	unsigned int c;
} *node;

typedef struct HuffmanTree {
	unsigned int stateNum;
	unsigned int allNodes;
	struct node_t* pool;
	node *qqq, *qq; //the root node of the HuffmanTree is qq[1]
	int n_nodes; //n_nodes is for compression
	int qend; 
	uint64_t **code;
	unsigned char *cout;
	int n_inode; //n_inode is for decompression
	int maxBitCount;
} HuffmanTree;

HuffmanTree* createHuffmanTree(int stateNum);
HuffmanTree* createDefaultHuffmanTree();

node new_node(HuffmanTree *huffmanTree, size_t freq, unsigned int c, node a, node b);
node new_node2(HuffmanTree *huffmanTree, unsigned int c, unsigned char t);
void qinsert(HuffmanTree *huffmanTree, node n);
node qremove(HuffmanTree *huffmanTree);
void build_code(HuffmanTree *huffmanTree, node n, int len, uint64_t out1, uint64_t out2);
void init(HuffmanTree *huffmanTree, int *s, size_t length);
void init_static(HuffmanTree *huffmanTree, int *s, size_t length);
void encode(HuffmanTree *huffmanTree, int *s, size_t length, unsigned char *out, size_t *outSize);

int decode(unsigned char *s, size_t sSize, size_t targetLength, node t, int *out);

void pad_tree_uchar(HuffmanTree* huffmanTree, unsigned char* L, unsigned char* R, unsigned int* C, unsigned char* t, unsigned int i, node root);
void pad_tree_ushort(HuffmanTree* huffmanTree, unsigned short* L, unsigned short* R, unsigned int* C, unsigned char* t, unsigned int i, node root);
void pad_tree_uint(HuffmanTree* huffmanTree, unsigned int* L, unsigned int* R, unsigned int* C, unsigned char* t, unsigned int i, node root);
unsigned int convert_HuffTree_to_bytes_anyStates(HuffmanTree* huffmanTree, int nodeCount, unsigned char** out);
void unpad_tree_uchar(HuffmanTree* huffmanTree, unsigned char* L, unsigned char* R, unsigned int* C, unsigned char *t, unsigned int i, node root);
void unpad_tree_ushort(HuffmanTree* huffmanTree, unsigned short* L, unsigned short* R, unsigned int* C, unsigned char* t, unsigned int i, node root);
void unpad_tree_uint(HuffmanTree* huffmanTree, unsigned int* L, unsigned int* R, unsigned int* C, unsigned char* t, unsigned int i, node root);
int reconstruct_HuffTree_from_bytes_anyStates(HuffmanTree *huffmanTree, unsigned char* bytes, size_t bytesLen, int nodeCount, unsigned int maxSymbolValue, node *rootOut, size_t *treeBytesOut);
void encode_withTree(HuffmanTree* huffmanTree, int *s, size_t length, unsigned char **out, size_t *outSize);
int decode_withTree(HuffmanTree* huffmanTree, unsigned char *s, size_t sSize, size_t targetLength, unsigned int maxSymbolValue, int *out);
void SZ_ReleaseHuffman(HuffmanTree* huffmanTree);

#ifdef __cplusplus
}
#endif

#endif
