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
	HuffmanTree *huffmanTree = (HuffmanTree*)malloc(sizeof(HuffmanTree));
	memset(huffmanTree, 0, sizeof(HuffmanTree));
	huffmanTree->stateNum = stateNum;
	huffmanTree->allNodes = 2*stateNum;
	
	huffmanTree->pool = (struct node_t*)malloc(huffmanTree->allNodes*2*sizeof(struct node_t));
	huffmanTree->qqq = (node*)malloc(huffmanTree->allNodes*2*sizeof(node));
	huffmanTree->code = (unsigned long**)malloc(huffmanTree->stateNum*sizeof(unsigned long*));
	huffmanTree->cout = (unsigned char *)malloc(huffmanTree->stateNum*sizeof(unsigned char));
	
	memset(huffmanTree->pool, 0, huffmanTree->allNodes*2*sizeof(struct node_t));
	memset(huffmanTree->qqq, 0, huffmanTree->allNodes*2*sizeof(node));
    memset(huffmanTree->code, 0, huffmanTree->stateNum*sizeof(unsigned long*));
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
void build_code(HuffmanTree *huffmanTree, node n, int len, unsigned long out1, unsigned long out2)
{
	if (n->t) {
		huffmanTree->code[n->c] = (unsigned long*)malloc(2*sizeof(unsigned long));
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
				longToBytes_bigEndian(p, (huffmanTree->code[state])[0]);
				p += byteSizep;
			}
			else //byteSize>8
			{
				longToBytes_bigEndian(p, (huffmanTree->code[state])[0]);
				p += 8;
				longToBytes_bigEndian(p, (huffmanTree->code[state])[1]);
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
				long newCode = (huffmanTree->code[state])[0] << lackBits;
				longToBytes_bigEndian(p, newCode);

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
						longToBytes_bigEndian(p, newCode);
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
 
void decode(unsigned char *s, size_t targetLength, node t, int *out)
{
	size_t i = 0, byteIndex = 0, count = 0;
	int r; 
	node n = t;
	
	if(n->t) //root->t==1 means that all state values are the same (constant)
	{
		for(count=0;count<targetLength;count++)
			out[count] = n->c;
		return;
	}
	
	for(i=0;count<targetLength;i++)
	{
		
		byteIndex = i>>3; //i/8
		r = i%8;
		if(((s[byteIndex] >> (7-r)) & 0x01) == 0)
			n = n->left;
		else
			n = n->right;

		if (n->t) {
			//putchar(n->c); 
			out[count] = n->c;
			n = t; 
			count++;
		}
	}
//	putchar('\n');
	if (t != n) printf("garbage input\n");
	return;
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

node reconstruct_HuffTree_from_bytes_anyStates(HuffmanTree *huffmanTree, unsigned char* bytes, int nodeCount)
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
		node root = new_node2(huffmanTree, C[0],t[0]);
		unpad_tree_uchar(huffmanTree,L,R,C,t,0,root);
		free(L);
		free(R);
		free(C);
		free(t);
		return root;
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

		node root = new_node2(huffmanTree,0,0);
		unpad_tree_ushort(huffmanTree,L,R,C,t,0,root);
		free(L);
		free(R);
		free(C);
		free(t);		
		return root;				
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
					
		node root = new_node2(huffmanTree,0,0);
		unpad_tree_uint(huffmanTree,L,R,C,t,0,root);
		free(L);
		free(R);
		free(C);
		free(t);
		return root;
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
	intToBytes_bigEndian(buffer, nodeCount);
	memcpy(*out, buffer, 4);
	intToBytes_bigEndian(buffer, huffmanTree->stateNum/2); //real number of intervals
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
void decode_withTree(HuffmanTree* huffmanTree, unsigned char *s, size_t targetLength, int *out)
{
	size_t encodeStartIndex;
	size_t nodeCount = bytesToInt_bigEndian(s);
	node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree,s+8, nodeCount);

	//sdi: Debug
/*	build_code(root, 0, 0, 0);
	int i;
	unsigned long code_1, code_2;
	for (i = 0; i < stateNum; i++)
		if (code[i])
		{
			printf("%d: %lu,%lu ; %u\n", i, (code[i])[0],(code[i])[1], cout[i]);
			//code_1 = (code[i])[0];
		}*/

	if(nodeCount<=256)
		encodeStartIndex = 1+3*nodeCount*sizeof(unsigned char)+nodeCount*sizeof(unsigned int);
	else if(nodeCount<=65536)
		encodeStartIndex = 1+2*nodeCount*sizeof(unsigned short)+nodeCount*sizeof(unsigned char)+nodeCount*sizeof(unsigned int);
	else
		encodeStartIndex = 1+3*nodeCount*sizeof(unsigned int)+nodeCount*sizeof(unsigned char);
	decode(s+8+encodeStartIndex, targetLength, root, out);
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
