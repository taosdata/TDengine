#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

#include "tsqlfunction.h"
#include "ttimer.h"
#include "tcache.h"
#include "tutil.h"

/*
 * test for client cache, refactor by using ref count
 */
void clientCacheTest() {
    debugFlag = 199;

    const int32_t REFRESH_TIME_IN_SEC = 2;
    void* tscTmr = taosTmrInit (tsMaxMgmtConnections*2, 200, 60000, "TSC");
    void* tscMetaCache = taosInitDataCache(tsMaxMeterConnections, tscTmr, REFRESH_TIME_IN_SEC);

    char* key1 = "test1";
    char* data1 = "test11";

    char* cachedObj = taosAddDataIntoCache(tscMetaCache, key1, data1, strlen(data1), 1);
    sleep(REFRESH_TIME_IN_SEC+1);
    printf("obj is still valid: %s\n", cachedObj);

    char* data2 = "test22";
    taosRemoveDataFromCache(tscMetaCache, cachedObj, false);

    /* the object is cleared by cache clean operation */
    cachedObj = taosUpdateDataFromCache(tscMetaCache, key1, data2, strlen(data2), 20);
    printf("after updated: %s\n", cachedObj);

    printf("start to remove data from cache\n");
    taosRemoveDataFromCache(tscMetaCache, cachedObj, false);
    printf("end of removing data from cache\n");

    getchar();

    char* key3 = "test2";
    char* data3 = "kkkkkkk";

    char* cachedObj2 = taosAddDataIntoCache(tscMetaCache, key3, data3, strlen(data3), 1);
    printf("%s\n", cachedObj2);

    taosRemoveDataFromCache(tscMetaCache, cachedObj2, false);

    sleep(3);
    char* d = taosGetDataFromCache(tscMetaCache, key3);
//    assert(d == NULL);

    char* key5 = "test5";
    char* data5 = "data5kkkkk";
    cachedObj2 = taosAddDataIntoCache(tscMetaCache, key5, data5, strlen(data5), 20);

    char* data6= "new Data after updated";
    taosRemoveDataFromCache(tscMetaCache, cachedObj2, false);

    cachedObj2 = taosUpdateDataFromCache(tscMetaCache, key5, data6, strlen(data6), 20);
    printf("%s\n", cachedObj2);

    taosRemoveDataFromCache(tscMetaCache, cachedObj2, true);

    char* data7 = "add call update procedure";
    cachedObj2 = taosAddDataIntoCache(tscMetaCache, key5, data7, strlen(data7), 20);
    printf("%s\n=======================================\n\n", cachedObj2);

    char* cc = taosGetDataFromCache(tscMetaCache, key5);

    taosRemoveDataFromCache(tscMetaCache, cachedObj2, true);
    taosRemoveDataFromCache(tscMetaCache, cc, false);

    char* data8 = "ttft";
    char* key6 = "key6";

    char* ft = taosAddDataIntoCache(tscMetaCache, key6, data8, strlen(data8), 20);
    taosRemoveDataFromCache(tscMetaCache, ft, false);
//    getchar();

    /**
     * 140ns
     */
    uint64_t startTime = taosGetTimestampMs();
    printf("Cache Performance Test\nstart time:%lld\n", startTime);
    for(int32_t i=0; i<1000; ++i) {
        char* dd = taosGetDataFromCache(tscMetaCache, key6);
        taosRemoveDataFromCache(tscMetaCache, dd, false);
    }

    uint64_t endTime = taosGetTimestampMs();
    printf("End of Test, %lld\nTotal Elapsed Time:%lld ms.", endTime, endTime - startTime);

    taosCleanUpDataCache(tscMetaCache);
}

void testMMapRead(char* filePath) {
    int handle = open(filePath, O_RDONLY);
    size_t offset = 0;
    size_t len = 4096;

    char att[4096] = {0};

    struct stat st;
    stat(filePath, &st);
    size_t fileSize = st.st_size;

    size_t mapSize = 4096*4096;
    char* starMem = mmap(NULL, mapSize, PROT_READ, MAP_SHARED, handle, offset);
    madvise(starMem, len, MADV_SEQUENTIAL);
    size_t endPos = mapSize;

    size_t k = 0;
    while(offset < fileSize) {
        while(offset < endPos && offset < fileSize) {
            memcpy(att, starMem + k, len);
            offset += len;
            k += len;
        }

        int ret = munmap(starMem, mapSize);
        starMem = mmap(NULL, mapSize, PROT_READ, MAP_SHARED, handle, offset);
        endPos += mapSize;
        k = 0;
    }
    munmap(starMem, mapSize);
    taosClose(handle);
}

int main(int32_t argc, char** argv) {
//    patternMatchTest();
//    clientCacheTest();
    testMMapRead(argv[1]);
    return 0;
}

