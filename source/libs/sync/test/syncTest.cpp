#include <stdio.h>
#include "syncInt.h"

int main() {
    printf("test \n");

    syncStartEnv();

    char temp[100];
    snprintf(temp, 100, "./debug.log");
    taosInitLog(temp, 10000, 1);
    tsAsyncLog = 0;

    for (int i = 0; i < 100; i++) {
        sDebug("log:%d -------- \n", i);
    }

    fflush(NULL);
    //taosCloseLog();

    while(1) {
        sleep(3);
    }
    return 0;
}

