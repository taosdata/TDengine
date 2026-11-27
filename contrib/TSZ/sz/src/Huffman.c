/**
 *  @file Huffman.c
 *  @author Sheng Di
 *  @date Aug., 2016
 *  @brief Customized Huffman Encoding, Compression and Decompression functions
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Huffman.h"
#include "sz.h"


HuffmanTree* createHuffmanTree(int stateNum)
{			
	if(stateNum <= 0 || stateNum > MAX_HUFF_TREE_NODES) //stateNum comes from the payload on the decode path
	{
		printf("Error: invalid Huffman stateNum %d\n", stateNum);
		return NULL;
	}

	HuffmanTree *huffmanTree = (HuffmanTree*)malloc(sizeof(HuffmanTree));
	if(huffmanTree == NULL)
		return NULL;
	memset(huffmanTree, 0, sizeof(HuffmanTree));
	huffmanTree->stateNum = stateNum;
	huffmanTree->allNodes = 2*stateNum;
	
	huffmanTree->pool = (struct node_t*)malloc(huffmanTree->allNodes*2*sizeof(struct node_t));
	huffmanTree->qqq = (node*)malloc(huffmanTree->allNodes*2*sizeof(node));
	huffmanTree->code = (uint64_t**)malloc(huffmanTree->stateNum*sizeof(uint64_t*));
	huffmanTree->cout = (unsigned char *)malloc(huffmanTree->stateNum*sizeof(unsigned char));
	if(huffmanTree->pool == NULL || huffmanTree->qqq == NULL || huffmanTree->code == NULL || huffmanTree->cout == NULL)
	{
		free(huffmanTree->pool);
		free(huffmanTree->qqq);
		free(huffmanTree->code);
		free(huffmanTree->cout);
		free(huffmanTree);
		return NULL;
	}
	
	memset(huffmanTree->pool, 0, huffmanTree->allNodes*2*sizeof(struct node_t));
	memset(huffmanTree->qqq, 0, huffmanTree->allNodes*2*sizeof(node));
    memset(huffmanTree->code, 0, huffmanTree->stateNum*sizeof(uint64_t*));
    memset(huffmanTree->cout, 0, huffmanTree->stateNum*sizeof(unsigned char));
	huffmanTree->qq = huffmanTree->qqq - 1;
	huffmanTree->n_nodes = 0;
    huffmanTree->n_inode = 0;
    huffmanTree->qend = 1;	
    
    return huffmanTree;
}

HuffmanTree* createDefaultHuffmanTree()
{
	int maxRangeRadius = 32768;
	int stateNum = maxRangeRadius << 1; //*2

    return createHuffmanTree(stateNum);
}
 
node new_node(HuffmanTree* huffmanTree, size_t freq, unsigned int c, node a, node b)
{
	node n = huffmanTree->pool + huffmanTree->n_nodes++;
	if (freq) 
	{
		n->c = c;
		n->freq = freq;
		n->t = 1;
	}
	else {
		n->left = a; 
		n->right = b;
		n->freq = a->freq + b->freq;
		n->t = 0;
		//n->c = 0;
	}
	return n;
}
 
node new_node2(HuffmanTree *huffmanTree, unsigned int c, unsigned char t)
{
	//the pool holds allNodes*2 == 4*stateNum entries (createHuffmanTree); never write past it
	if(huffmanTree == NULL || huffmanTree->pool == NULL)
		return NULL;
	if(huffmanTree->n_nodes < 0 || (unsigned int)huffmanTree->n_nodes >= huffmanTree->allNodes*2)
		return NULL;
	huffmanTree->pool[huffmanTree->n_nodes].c = c;
	huffmanTree->pool[huffmanTree->n_nodes].t = t;
	return huffmanTree->pool + huffmanTree->n_nodes++;
} 
 
/* priority queue */
void qinsert(HuffmanTree *huffmanTree, node n)
{
	int j, i = huffmanTree->qend++;
	while ((j = (i>>1)))  //j=i/2
	{
		if (huffmanTree->qq[j]->freq <= n->freq) break;
		huffmanTree->qq[i] = huffmanTree->qq[j], i = j;
	}
	huffmanTree->qq[i] = n;
}
 
node qremove(HuffmanTree* huffmanTree)
{
	int i, l;
	node n = huffmanTree->qq[i = 1];
	node p;
	if (huffmanTree->qend < 2) return 0;
	huffmanTree->qend --;
	huffmanTree->qq[i] = huffmanTree->qq[huffmanTree->qend];
	
	while ((l = (i<<1)) < huffmanTree->qend)  //l=(i*2)
	{
		if (l + 1 < huffmanTree->qend && huffmanTree->qq[l + 1]->freq < huffmanTree->qq[l]->freq) l++;
		if(huffmanTree->qq[i]->freq > huffmanTree->qq[l]->freq)
		{
			p = huffmanTree->qq[i];
			huffmanTree->qq[i] = huffmanTree->qq[l];
			huffmanTree->qq[l] = p;
			i = l;			
		}	
		else
		{
			break;
		}
		
	}
	
	return n;
}
 
/* walk the tree and put 0s and 1s */
/**
 * @out1 should be set to 0.
 * @out2 should be 0 as well.
 * @index: the index of the byte
 * */
void build_code(HuffmanTree *huffmanTree, node n, int len, uint64_t out1, uint64_t out2)
{
	if (n->t) {
		huffmanTree->code[n->c] = (uint64_t*)malloc(2*sizeof(uint64_t));
		if(len<=64)
		{
			if(len == 0)
			  (huffmanTree->code[n->c])[0] = 0;
			else 
			  (huffmanTree->code[n->c])[0] = out1 << (64 - len);
			(huffmanTree->code[n->c])[1] = out2;
		}
		else
		{
			(huffmanTree->code[n->c])[0] = out1;
			(huffmanTree->code[n->c])[1] = out2 << (128 - len);
		}
		huffmanTree->cout[n->c] = (unsigned char)len;
		return;
	}
	int index = len >> 6; //=len/64
	if(index == 0)
	{
		out1 = out1 << 1;
		out1 = out1 | 0;
		build_code(huffmanTree, n->left, len + 1, out1, 0);
		out1 = out1 | 1;
		build_code(huffmanTree, n->right, len + 1, out1, 0);		
	}
	else
	{
		if(len%64!=0)
			out2 = out2 << 1;
		out2 = out2 | 0;
		build_code(huffmanTree, n->left, len + 1, out1, out2);
		out2 = out2 | 1;
		build_code(huffmanTree, n->right, len + 1, out1, out2);	
	}
}

/**
 * Compute the frequency of the data and build the Huffman tree
 * @param HuffmanTree* huffmanTree (output)
 * @param int *s (input)
 * @param size_t length (input)
 * */
void init(HuffmanTree* huffmanTree, int *s, size_t length)
{
	size_t i, index;
	size_t *freq = (size_t *)malloc(huffmanTree->allNodes*sizeof(size_t));
	memset(freq, 0, huffmanTree->allNodes*sizeof(size_t));
	for(i = 0;i < length;i++)
	{
		index = s[i];
		freq[index]++;
	}

	for (i = 0; i < huffmanTree->allNodes; i++)
		if (freq[i])
			qinsert(huffmanTree, new_node(huffmanTree, freq[i], i, 0, 0));

	while (huffmanTree->qend > 2)
		qinsert(huffmanTree, new_node(huffmanTree, 0, 0, qremove(huffmanTree), qremove(huffmanTree)));

	build_code(huffmanTree, huffmanTree->qq[1], 0, 0, 0);
	free(freq);
}

void init_static(HuffmanTree* huffmanTree, int *s, size_t length)
{
	size_t i;
	size_t *freq = (size_t *)malloc(huffmanTree->allNodes*sizeof(size_t));
	memset(freq, 0, huffmanTree->allNodes*sizeof(size_t));


	for (i = 0; i < huffmanTree->allNodes; i++)
		if (freq[i])
			qinsert(huffmanTree, new_node(huffmanTree, freq[i], i, 0, 0));

	while (huffmanTree->qend > 2)
		qinsert(huffmanTree, new_node(huffmanTree, 0, 0, qremove(huffmanTree), qremove(huffmanTree)));

	build_code(huffmanTree, huffmanTree->qq[1], 0, 0, 0);
	free(freq);
}
 
void encode(HuffmanTree *huffmanTree, int *s, size_t length, unsigned char *out, size_t *outSize)
{
	size_t i = 0;
	unsigned char bitSize = 0, byteSize, byteSizep;
	int state;
	unsigned char *p = out;
	int lackBits = 0;
	//long totalBitSize = 0, maxBitSize = 0, bitSize21 = 0, bitSize32 = 0;
	for (i = 0;i<length;i++) 
	{
		state = s[i];
		bitSize = huffmanTree->cout[state];	
		
		//printf("%d %d : %d %u\n",i, state, bitSize, (code[state])[0] >> (64-cout[state])); 
		//debug: compute the average bitSize and the count that is over 32... 	
		/*if(bitSize>=21)
			bitSize21++;
		if(bitSize>=32)
			bitSize32++;
		if(maxBitSize<bitSize)
			maxBitSize = bitSize;
		totalBitSize+=bitSize;*/

		if(lackBits==0)
		{
			byteSize = bitSize%8==0 ? bitSize/8 : bitSize/8+1; //it's equal to the number of bytes involved (for *outSize)
			byteSizep = bitSize/8; //it's used to move the pointer p for next data
			if(byteSize<=8)
			{
				int64ToBytes_bigEndian(p, (huffmanTree->code[state])[0]);
				p += byteSizep;
			}
			else //byteSize>8
			{
				int64ToBytes_bigEndian(p, (huffmanTree->code[state])[0]);
				p += 8;
				int64ToBytes_bigEndian(p, (huffmanTree->code[state])[1]);
				p += (byteSizep - 8);
			}
			*outSize += byteSize;
			lackBits = bitSize%8==0 ? 0 : 8 - bitSize%8;
		}
		else
		{
			*p = (*p) | (unsigned char)((huffmanTree->code[state])[0] >> (64 - lackBits));
			if(lackBits < bitSize)
			{
				p++;
				//(*outSize)++;
				uint64_t newCode = (huffmanTree->code[state])[0] << lackBits;
				int64ToBytes_bigEndian(p, newCode);

				if(bitSize<=64)
				{
					bitSize -= lackBits;
					byteSize = bitSize%8==0 ? bitSize/8 : bitSize/8+1;
					byteSizep = bitSize/8;
					p += byteSizep;
					(*outSize)+=byteSize;
					lackBits = bitSize%8==0 ? 0 : 8 - bitSize%8;
				}
				else //bitSize > 64
				{
					byteSizep = 7; //must be 7 bytes, because lackBits!=0
					p+=byteSizep;
					(*outSize)+=byteSize;

					bitSize -= 64;
					if(lackBits < bitSize)
					{
						*p = (*p) | (unsigned char)((huffmanTree->code[state])[0] >> (64 - lackBits));
						p++;
						//(*outSize)++;
						newCode = (huffmanTree->code[state])[1] << lackBits;
						int64ToBytes_bigEndian(p, newCode);
						bitSize -= lackBits;
						byteSize = bitSize%8==0 ? bitSize/8 : bitSize/8+1;
						byteSizep = bitSize/8;
						p += byteSizep;
						(*outSize)+=byteSize;
						lackBits = bitSize%8==0 ? 0 : 8 - bitSize%8;
					}
					else //lackBits >= bitSize
					{
						*p = (*p) | (unsigned char)((huffmanTree->code[state])[0] >> (64 - bitSize));
						lackBits -= bitSize;
					}
				}
			}
			else //lackBits >= bitSize
			{
				lackBits -= bitSize;
				if(lackBits==0)
					p++;
			}
		}
	}
//	for(i=0;i<stateNum;i++)
//		if(code[i]!=NULL) free(code[i]);
	/*printf("max bitsize = %d\n", maxBitSize);
	printf("bitSize21 ratio = %f\n", ((float)bitSize21)/length);
	printf("bitSize32 ratio = %f\n", ((float)bitSize32)/length);
	printf("avg bit size = %f\n", ((float)totalBitSize)/length);*/
}
 
int decode(unsigned char *s, size_t sSize, size_t targetLength, node t, int *out)
{
	size_t i = 0, byteIndex = 0, count = 0;
	int r; 
	node n = t;
	
	if(s == NULL || t == NULL || out == NULL)
		return SZ_FORMAT_ERR;

	if(n->t) //root->t==1 means that all state values are the same (constant)
	{
		for(count=0;count<targetLength;count++)
			out[count] = n->c;
		return SZ_SUCCESS;
	}
	
	for(i=0;count<targetLength;i++)
	{
		
		byteIndex = i>>3; //i/8
		if(byteIndex >= sSize) //truncated bit stream: stop instead of reading past the payload
			return SZ_FORMAT_ERR;
		r = i%8;
		if(((s[byteIndex] >> (7-r)) & 0x01) == 0)
			n = n->left;
		else
			n = n->right;

		if (n == NULL) //a child pointer that the serialized tree never populated
			return SZ_FORMAT_ERR;

		if (n->t) {
			//putchar(n->c); 
			out[count] = n->c;
			n = t; 
			count++;
		}
	}
//	putchar('\n');
	if (t != n) printf("garbage input\n");
	return SZ_SUCCESS;
}

void pad_tree_uchar(HuffmanTree* huffmanTree, unsigned char* L, unsigned char* R, unsigned int* C, unsigned char* t, unsigned int i, node root)
{
	C[i] = root->c;
	t[i] = root->t;
	node lroot = root->left;
	if(lroot!=0)
	{
		huffmanTree->n_inode++;
		L[i] = huffmanTree->n_inode;
		pad_tree_uchar(huffmanTree, L,R,C,t, huffmanTree->n_inode, lroot);
	}
	node rroot = root->right;
	if(rroot!=0)
	{
		huffmanTree->n_inode++;
		R[i] = huffmanTree->n_inode;
		pad_tree_uchar(huffmanTree, L,R,C,t, huffmanTree->n_inode, rroot);
	}
}  

void pad_tree_ushort(HuffmanTree* huffmanTree, unsigned short* L, unsigned short* R, unsigned int* C, unsigned char* t, unsigned int i, node root)
{
	C[i] = root->c;
	t[i] = root->t;
	node lroot = root->left;
	if(lroot!=0)
	{
		huffmanTree->n_inode++;
		L[i] = huffmanTree->n_inode;
		pad_tree_ushort(huffmanTree,L,R,C,t,huffmanTree->n_inode, lroot);
	}
	node rroot = root->right;
	if(rroot!=0)
	{
		huffmanTree->n_inode++;
		R[i] = huffmanTree->n_inode;
		pad_tree_ushort(huffmanTree,L,R,C,t,huffmanTree->n_inode, rroot);
	}	
}

void pad_tree_uint(HuffmanTree* huffmanTree, unsigned int* L, unsigned int* R, unsigned int* C, unsigned char* t, unsigned int i, node root)
{
	C[i] = root->c;
	t[i] = root->t;
	node lroot = root->left;
	if(lroot!=0)
	{
		huffmanTree->n_inode++;
		L[i] = huffmanTree->n_inode;
		pad_tree_uint(huffmanTree,L,R,C,t,huffmanTree->n_inode, lroot);
	}
	node rroot = root->right;
	if(rroot!=0)
	{
		huffmanTree->n_inode++;
		R[i] = huffmanTree->n_inode;
		pad_tree_uint(huffmanTree,L,R,C,t,huffmanTree->n_inode, rroot);
	}
}
 
unsigned int convert_HuffTree_to_bytes_anyStates(HuffmanTree* huffmanTree, int nodeCount, unsigned char** out) 
{
	if(nodeCount<=256)
	{
		unsigned char* L = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		memset(L, 0, nodeCount*sizeof(unsigned char));
		unsigned char* R = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		memset(R, 0, nodeCount*sizeof(unsigned char));
		unsigned int* C = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));
		memset(C, 0, nodeCount*sizeof(unsigned int));
		unsigned char* t = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		memset(t, 0, nodeCount*sizeof(unsigned char));

		pad_tree_uchar(huffmanTree,L,R,C,t,0,huffmanTree->qq[1]);

		unsigned int totalSize = 1+3*nodeCount*sizeof(unsigned char)+nodeCount*sizeof(unsigned int);	
		*out = (unsigned char*)malloc(totalSize*sizeof(unsigned char));
		(*out)[0] = (unsigned char)sysEndianType;
		memcpy(*out+1, L, nodeCount*sizeof(unsigned char));
		memcpy((*out)+1+nodeCount*sizeof(unsigned char),R,nodeCount*sizeof(unsigned char));
		memcpy((*out)+1+2*nodeCount*sizeof(unsigned char),C,nodeCount*sizeof(unsigned int));
		memcpy((*out)+1+2*nodeCount*sizeof(unsigned char)+nodeCount*sizeof(unsigned int), t, nodeCount*sizeof(unsigned char));
		free(L);
		free(R);
		free(C);
		free(t);
		return totalSize;

	}
	else if(nodeCount<=65536)
	{
		unsigned short* L = (unsigned short*)malloc(nodeCount*sizeof(unsigned short));
		memset(L, 0, nodeCount*sizeof(unsigned short));
		unsigned short* R = (unsigned short*)malloc(nodeCount*sizeof(unsigned short));
		memset(R, 0, nodeCount*sizeof(unsigned short));
		unsigned int* C = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));	
		memset(C, 0, nodeCount*sizeof(unsigned int));		
		unsigned char* t = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		memset(t, 0, nodeCount*sizeof(unsigned char));		
		pad_tree_ushort(huffmanTree,L,R,C,t,0,huffmanTree->qq[1]);
		unsigned int totalSize = 1+2*nodeCount*sizeof(unsigned short)+nodeCount*sizeof(unsigned char) + nodeCount*sizeof(unsigned int);
		*out = (unsigned char*)malloc(totalSize);
		(*out)[0] = (unsigned char)sysEndianType;		
		memcpy(*out+1, L, nodeCount*sizeof(unsigned short));
		memcpy((*out)+1+nodeCount*sizeof(unsigned short),R,nodeCount*sizeof(unsigned short));
		memcpy((*out)+1+2*nodeCount*sizeof(unsigned short),C,nodeCount*sizeof(unsigned int));
		memcpy((*out)+1+2*nodeCount*sizeof(unsigned short)+nodeCount*sizeof(unsigned int),t,nodeCount*sizeof(unsigned char));
		free(L);
		free(R);
		free(C);
		free(t);		
		return totalSize;
	}
	else //nodeCount>65536
	{
		unsigned int* L = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));
		memset(L, 0, nodeCount*sizeof(unsigned int));
		unsigned int* R = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));
		memset(R, 0, nodeCount*sizeof(unsigned int));
		unsigned int* C = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));	
		memset(C, 0, nodeCount*sizeof(unsigned int));
		unsigned char* t = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		memset(t, 0, nodeCount*sizeof(unsigned char));
		pad_tree_uint(huffmanTree, L,R,C,t,0,huffmanTree->qq[1]);
		
		//debug
		//node root = new_node2(0,0);
		//unpad_tree_uint(L,R,C,t,0,root);		
		
		unsigned int totalSize = 1+3*nodeCount*sizeof(unsigned int)+nodeCount*sizeof(unsigned char);
		*out = (unsigned char*)malloc(totalSize);
		(*out)[0] = (unsigned char)sysEndianType;
		memcpy(*out+1, L, nodeCount*sizeof(unsigned int));
		memcpy((*out)+1+nodeCount*sizeof(unsigned int),R,nodeCount*sizeof(unsigned int));
		memcpy((*out)+1+2*nodeCount*sizeof(unsigned int),C,nodeCount*sizeof(unsigned int));
		memcpy((*out)+1+3*nodeCount*sizeof(unsigned int),t,nodeCount*sizeof(unsigned char));
		free(L);
		free(R);
		free(C);
		free(t);
		return totalSize;		
	}
}

/*
 * A serialized Huffman tree is fully controlled by the compressed payload: nodeCount, the child
 * index arrays L/R and the symbol array C. Before unpad_tree_* walks those arrays (and appends to
 * the fixed-size Huffman pool), make sure they describe a well formed tree: all symbols inside the
 * quantization range, every child index < nodeCount, each node referenced at most once as a child,
 * and all nodeCount nodes reachable from the root (index 0), which rules out cycles and dangling
 * children. Returns 1 when the arrays can be rebuilt safely, 0 otherwise.
 */
static unsigned int huffChildIndex(const void *arr, size_t i, int width)
{
	if(width == 1)
		return (unsigned int)((const unsigned char *)arr)[i];
	if(width == 2)
		return (unsigned int)((const unsigned short *)arr)[i];
	return ((const unsigned int *)arr)[i];
}

static int isValidSerializedTree(const void *L, const void *R, const unsigned int *C, int nodeCount, int width, unsigned int maxSymbolValue)
{
	unsigned char *used = (unsigned char *)calloc((size_t)nodeCount, sizeof(unsigned char));
	unsigned char *visited = (unsigned char *)calloc((size_t)nodeCount, sizeof(unsigned char));
	unsigned int *stack = (unsigned int *)malloc((size_t)nodeCount*sizeof(unsigned int));
	unsigned int *depth = (unsigned int *)malloc((size_t)nodeCount*sizeof(unsigned int));
	int ok = 1;
	size_t reachable = 0, top = 0;
	int i, k;

	if(used == NULL || visited == NULL || stack == NULL || depth == NULL)
	{
		free(used);
		free(visited);
		free(stack);
		free(depth);
		return 0;
	}

	for(i = 0; i < nodeCount; i++)
	{
		if(C[i] > maxSymbolValue)
		{
			ok = 0;
			break;
		}
	}

	for(i = 0; i < nodeCount && ok; i++)
	{
		for(k = 0; k < 2; k++)
		{
			unsigned int c = huffChildIndex(k == 0 ? L : R, (size_t)i, width);
			if(c == 0) //0 means "no child"
				continue;
			if(c >= (unsigned int)nodeCount || used[c])
			{
				ok = 0;
				break;
			}
			used[c] = 1;
		}
	}

	if(ok)
	{
		stack[top++] = 0;
		visited[0] = 1;
		depth[0] = 0;
		reachable = 1;
		while(top > 0 && ok)
		{
			unsigned int n = stack[--top];
			for(k = 0; k < 2; k++)
			{
				unsigned int c = huffChildIndex(k == 0 ? L : R, (size_t)n, width);
				if(c == 0)
					continue;
				if(c >= (unsigned int)nodeCount || visited[c])
				{
					ok = 0;
					break;
				}
				visited[c] = 1;
				depth[c] = depth[n] + 1;
				if(depth[c] > MAX_HUFF_TREE_DEPTH) //a degenerate chain must never reach the recursive rebuild
				{
					ok = 0;
					break;
				}
				reachable++;
				stack[top++] = c;
			}
		}
		if(ok && reachable != (size_t)nodeCount)
			ok = 0;
	}

	free(used);
	free(visited);
	free(stack);
	free(depth);
	return ok;
}

void unpad_tree_uchar(HuffmanTree* huffmanTree, unsigned char* L, unsigned char* R, unsigned int* C, unsigned char *t, unsigned int i, node root)
{
	//root->c = C[i];
	if(root->t==0)
	{
		unsigned char l, r;
		l = L[i];
		if(l!=0)
		{
			node lroot = new_node2(huffmanTree,C[l],t[l]);
			root->left = lroot;
			unpad_tree_uchar(huffmanTree,L,R,C,t,l,lroot);
		}
		r = R[i];
		if(r!=0)
		{
			node rroot = new_node2(huffmanTree,C[r],t[r]);
			root->right = rroot;
			unpad_tree_uchar(huffmanTree,L,R,C,t,r,rroot);
		}
	}
}

void unpad_tree_ushort(HuffmanTree* huffmanTree, unsigned short* L, unsigned short* R, unsigned int* C, unsigned char* t, unsigned int i, node root)
{
	//root->c = C[i];
	if(root->t==0)
	{
		unsigned short l, r;
		l = L[i];
		if(l!=0)
		{
			node lroot = new_node2(huffmanTree,C[l],t[l]);
			root->left = lroot;
			unpad_tree_ushort(huffmanTree,L,R,C,t,l,lroot);
		}
		r = R[i];
		if(r!=0)
		{
			node rroot = new_node2(huffmanTree,C[r],t[r]);
			root->right = rroot;
			unpad_tree_ushort(huffmanTree,L,R,C,t,r,rroot);
		}
	}
}

void unpad_tree_uint(HuffmanTree* huffmanTree, unsigned int* L, unsigned int* R, unsigned int* C, unsigned char* t, unsigned int i, node root)
{
	//root->c = C[i];
	if(root->t==0)
	{
		unsigned int l, r;
		l = L[i];
		if(l!=0)
		{
			node lroot = new_node2(huffmanTree,C[l],t[l]);
			root->left = lroot;
			unpad_tree_uint(huffmanTree,L,R,C,t,l,lroot);
		}
		r = R[i];
		if(r!=0)
		{
			node rroot = new_node2(huffmanTree,C[r],t[r]);
			root->right = rroot;
			unpad_tree_uint(huffmanTree,L,R,C,t,r,rroot);
		}
	}
}

int reconstruct_HuffTree_from_bytes_anyStates(HuffmanTree *huffmanTree, unsigned char* bytes, size_t bytesLen, int nodeCount, unsigned int maxSymbolValue, node *rootOut, size_t *treeBytesOut)
{
	size_t treeByteSize;

	if(huffmanTree == NULL || bytes == NULL || rootOut == NULL || treeBytesOut == NULL)
		return SZ_FORMAT_ERR;

	//nodeCount is read from the payload: bound it by the Huffman pool created for this stream and
	//by the bytes actually available (the smallest layout needs 7 bytes per node)
	if(nodeCount <= 0 || nodeCount > MAX_HUFF_TREE_NODES || (unsigned int)nodeCount > huffmanTree->allNodes)
		return SZ_FORMAT_ERR;

	if(nodeCount<=256)
		treeByteSize = 1 + 7*(size_t)nodeCount;
	else if(nodeCount<=65536)
		treeByteSize = 1 + 9*(size_t)nodeCount;
	else
		treeByteSize = 1 + 13*(size_t)nodeCount;

	if(treeByteSize > bytesLen)
		return SZ_FORMAT_ERR;

	if(nodeCount<=256)
	{
		unsigned char* L = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		unsigned char* R = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		unsigned int* C = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));
		unsigned char* t = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		if(L == NULL || R == NULL || C == NULL || t == NULL)
		{
			free(L);
			free(R);
			free(C);
			free(t);
			return SZ_FORMAT_ERR;
		}
		memset(L, 0, nodeCount*sizeof(unsigned char));
		memset(R, 0, nodeCount*sizeof(unsigned char));
		memset(C, 0, nodeCount*sizeof(unsigned int));
		memset(t, 0, nodeCount*sizeof(unsigned char));
		unsigned char cmpSysEndianType = bytes[0];
		if(cmpSysEndianType!=(unsigned char)sysEndianType)
		{
			unsigned char* p = (unsigned char*)(bytes+1+2*nodeCount*sizeof(unsigned char));
			size_t i = 0, size = nodeCount*sizeof(unsigned int);
			while(1)
			{
				symTransform_4bytes(p);
				i+=sizeof(unsigned int);
				if(i<size)
					p+=sizeof(unsigned int);
				else
					break;
			}		
		}
		memcpy(L, bytes+1, nodeCount*sizeof(unsigned char));
		memcpy(R, bytes+1+nodeCount*sizeof(unsigned char), nodeCount*sizeof(unsigned char));
		memcpy(C, bytes+1+2*nodeCount*sizeof(unsigned char), nodeCount*sizeof(unsigned int));	
		memcpy(t, bytes+1+2*nodeCount*sizeof(unsigned char)+nodeCount*sizeof(unsigned int), nodeCount*sizeof(unsigned char));
		if(!isValidSerializedTree((const void*)L, (const void*)R, C, nodeCount, 1, maxSymbolValue))
		{
			free(L);
			free(R);
			free(C);
			free(t);
			return SZ_FORMAT_ERR;
		}
		node root = new_node2(huffmanTree, C[0],t[0]);
		if(root == NULL)
		{
			free(L);
			free(R);
			free(C);
			free(t);
			return SZ_FORMAT_ERR;
		}
		unpad_tree_uchar(huffmanTree,L,R,C,t,0,root);
		free(L);
		free(R);
		free(C);
		free(t);
		*rootOut = root;
		*treeBytesOut = treeByteSize;
		return SZ_SUCCESS;
	}
	else if(nodeCount<=65536)
	{
		unsigned short* L = (unsigned short*)malloc(nodeCount*sizeof(unsigned short));
		unsigned short* R = (unsigned short*)malloc(nodeCount*sizeof(unsigned short));
		unsigned int* C = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));
		unsigned char* t = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		if(L == NULL || R == NULL || C == NULL || t == NULL)
		{
			free(L);
			free(R);
			free(C);
			free(t);
			return SZ_FORMAT_ERR;
		}
		memset(L, 0, nodeCount*sizeof(unsigned short));
		memset(R, 0, nodeCount*sizeof(unsigned short));
		memset(C, 0, nodeCount*sizeof(unsigned int));
		memset(t, 0, nodeCount*sizeof(unsigned char));	
				
		unsigned char cmpSysEndianType = bytes[0];	
		if(cmpSysEndianType!=(unsigned char)sysEndianType)
		{
			unsigned char* p = (unsigned char*)(bytes+1);
			size_t i = 0, size = 2*nodeCount*sizeof(unsigned short);
			
			while(1)
			{
				symTransform_2bytes(p);
				i+=sizeof(unsigned short);
				if(i<size)
					p+=sizeof(unsigned short);
				else
					break;
			}
			
			size = nodeCount*sizeof(unsigned int);
			while(1)
			{
				symTransform_4bytes(p);
				i+=sizeof(unsigned int);
				if(i<size)
					p+=sizeof(unsigned int);
				else
					break;				
			}
		}

		memcpy(L, bytes+1, nodeCount*sizeof(unsigned short));
		memcpy(R, bytes+1+nodeCount*sizeof(unsigned short), nodeCount*sizeof(unsigned short));
		memcpy(C, bytes+1+2*nodeCount*sizeof(unsigned short), nodeCount*sizeof(unsigned int));	

		memcpy(t, bytes+1+2*nodeCount*sizeof(unsigned short)+nodeCount*sizeof(unsigned int), nodeCount*sizeof(unsigned char));	

		if(!isValidSerializedTree((const void*)L, (const void*)R, C, nodeCount, 2, maxSymbolValue))
		{
			free(L);
			free(R);
			free(C);
			free(t);
			return SZ_FORMAT_ERR;
		}
		node root = new_node2(huffmanTree,0,0);
		if(root == NULL)
		{
			free(L);
			free(R);
			free(C);
			free(t);
			return SZ_FORMAT_ERR;
		}
		unpad_tree_ushort(huffmanTree,L,R,C,t,0,root);
		free(L);
		free(R);
		free(C);
		free(t);
		*rootOut = root;
		*treeBytesOut = treeByteSize;
		return SZ_SUCCESS;
	}
	else //nodeCount>65536
	{
		unsigned int* L = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));
		unsigned int* R = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));
		unsigned int* C = (unsigned int*)malloc(nodeCount*sizeof(unsigned int));
		unsigned char* t = (unsigned char*)malloc(nodeCount*sizeof(unsigned char));
		if(L == NULL || R == NULL || C == NULL || t == NULL)
		{
			free(L);
			free(R);
			free(C);
			free(t);
			return SZ_FORMAT_ERR;
		}
		memset(L, 0, nodeCount*sizeof(unsigned int));
		memset(R, 0, nodeCount*sizeof(unsigned int));
		memset(C, 0, nodeCount*sizeof(unsigned int));
		memset(t, 0, nodeCount*sizeof(unsigned char));
		unsigned char cmpSysEndianType = bytes[0];
		if(cmpSysEndianType!=(unsigned char)sysEndianType)
		{
			unsigned char* p = (unsigned char*)(bytes+1);
			size_t i = 0, size = 3*nodeCount*sizeof(unsigned int);
			while(1)
			{
				symTransform_4bytes(p);
				i+=sizeof(unsigned int);
				if(i<size)
					p+=sizeof(unsigned int);
				else
					break;
			}
		}

		memcpy(L, bytes+1, nodeCount*sizeof(unsigned int));
		memcpy(R, bytes+1+nodeCount*sizeof(unsigned int), nodeCount*sizeof(unsigned int));
		memcpy(C, bytes+1+2*nodeCount*sizeof(unsigned int), nodeCount*sizeof(unsigned int));	
	
		memcpy(t, bytes+1+3*nodeCount*sizeof(unsigned int), nodeCount*sizeof(unsigned char));			
					
		if(!isValidSerializedTree((const void*)L, (const void*)R, C, nodeCount, 4, maxSymbolValue))
		{
			free(L);
			free(R);
			free(C);
			free(t);
			return SZ_FORMAT_ERR;
		}
		node root = new_node2(huffmanTree,0,0);
		if(root == NULL)
		{
			free(L);
			free(R);
			free(C);
			free(t);
			return SZ_FORMAT_ERR;
		}
		unpad_tree_uint(huffmanTree,L,R,C,t,0,root);
		free(L);
		free(R);
		free(C);
		free(t);
		*rootOut = root;
		*treeBytesOut = treeByteSize;
		return SZ_SUCCESS;
	}
}

void encode_withTree(HuffmanTree* huffmanTree, int *s, size_t length, unsigned char **out, size_t *outSize)
{
	size_t i; 
	int nodeCount = 0;
	unsigned char *treeBytes, buffer[4];
	
	init(huffmanTree, s, length);
	for (i = 0; i < huffmanTree->stateNum; i++)
		if (huffmanTree->code[i]) nodeCount++; 
	nodeCount = nodeCount*2-1;
	unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree,nodeCount, &treeBytes);
	//printf("treeByteSize = %d\n", treeByteSize);

	*out = (unsigned char*)malloc(length*sizeof(int)+treeByteSize);
	int32ToBytes_bigEndian(buffer, nodeCount);
	memcpy(*out, buffer, 4);
	int32ToBytes_bigEndian(buffer, huffmanTree->stateNum/2); //real number of intervals
	memcpy(*out+4, buffer, 4);
	memcpy(*out+8, treeBytes, treeByteSize);
	free(treeBytes);
	size_t enCodeSize = 0;
	encode(huffmanTree, s, length, *out+8+treeByteSize, &enCodeSize);
	*outSize = 8+treeByteSize+enCodeSize;
}

/**
 * @par *out rememmber to allocate targetLength short_type data for it beforehand.
 * 
 * */
int decode_withTree(HuffmanTree* huffmanTree, unsigned char *s, size_t sSize, size_t targetLength, unsigned int maxSymbolValue, int *out)
{
	size_t encodeStartIndex;
	node root = NULL;
	size_t treeBytes = 0;
	int nodeCount;

	if(huffmanTree == NULL || s == NULL || out == NULL || sSize < 8) //4 bytes nodeCount + 4 bytes stateNum
		return SZ_FORMAT_ERR;

	nodeCount = bytesToInt32_bigEndian(s);
	if(reconstruct_HuffTree_from_bytes_anyStates(huffmanTree, s+8, sSize-8, nodeCount, maxSymbolValue, &root, &treeBytes) != SZ_SUCCESS)
		return SZ_FORMAT_ERR;

	//sdi: Debug
/*	build_code(root, 0, 0, 0);
	int i;
	uint64_t code_1, code_2;
	for (i = 0; i < stateNum; i++)
		if (code[i])
		{
			printf("%d: %lu,%lu ; %u\n", i, (code[i])[0],(code[i])[1], cout[i]);
			//code_1 = (code[i])[0];
		}*/

	encodeStartIndex = treeBytes;
	return decode(s+8+encodeStartIndex, sSize-8-encodeStartIndex, targetLength, root, out);
}

void SZ_ReleaseHuffman(HuffmanTree* huffmanTree)
{
	size_t i;
	free(huffmanTree->pool);
	huffmanTree->pool = NULL;
	free(huffmanTree->qqq);
	huffmanTree->qqq = NULL;
	for(i=0;i<huffmanTree->stateNum;i++)
	{
		if(huffmanTree->code[i]!=NULL)
			free(huffmanTree->code[i]);
	}
	free(huffmanTree->code);
	huffmanTree->code = NULL;
	free(huffmanTree->cout);
	huffmanTree->cout = NULL;	
	free(huffmanTree);
	huffmanTree = NULL;
}
