#include <gtest/gtest.h>

#include "taoserror.h"
#include "theap.h"

using namespace std;

typedef struct TNode {
    int32_t data;
    HeapNode node;
} TNodeMem;

#define container_of(ptr, type, member) ((type*)((char*)(ptr)-offsetof(type, member)))
int32_t heapCompare(const HeapNode* a, const HeapNode* b) {
    TNodeMem *ta = container_of(a, TNodeMem, node);
    TNodeMem *tb = container_of(b, TNodeMem, node);
    if (ta->data > tb->data) {
        return 0;
    } 
    return 1;
}

TEST(TD_UTIL_HEAP_TEST, heapTest) {
    Heap* heap = heapCreate(heapCompare);
    ASSERT_TRUE(heap != NULL);
    ASSERT_EQ(0, heapSize(heap));


    int32_t limit = 10;

    TNodeMem **pArr = (TNodeMem **)taosMemoryCalloc(100, sizeof(TNodeMem *)); 
    for (int i = 0; i < 100; i++) {
        TNodeMem *a = (TNodeMem *)taosMemoryCalloc(1, sizeof(TNodeMem)); 
        a->data = i%limit; 
        
        heapInsert(heap, &a->node);

        pArr[i] = a;
        TNodeMem *b = (TNodeMem *)taosMemoryCalloc(1, sizeof(TNodeMem)); 
        b->data = (limit - i)%limit; 
        heapInsert(heap, &b->node);
    }
    for (int i = 98; i < 100; i++) {
        TNodeMem *p = pArr[i];
        p->data = -100000;
    } 
    HeapNode *node = heapMin(heap);
    while (node != NULL) {
        TNodeMem *data = container_of(node, TNodeMem, node);
        heapRemove(heap, node);
        printf("%d\t", data->data);
	    node = heapMin(heap);
    }
    heapDestroy(heap);
}
