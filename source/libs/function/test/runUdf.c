#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "uv.h"
#include "os.h"
#include "tudf.h"

int main(int argc, char *argv[]) {
    startUdfService();
    uv_sleep(1000);
    char path[256] = {0};
    size_t cwdSize = 256;
    int err = uv_cwd(path, &cwdSize);
    if (err != 0) {
        fprintf(stderr, "err cwd: %s\n", uv_strerror(err));
	    return err;
    }
    fprintf(stdout, "current working directory:%s\n", path);
    strcat(path, "/libudf1.so");
    SUdfInfo udfInfo = {.udfName="udf1", .path=path};

    UdfHandle handle;
    setupUdf(&udfInfo, &handle);

    //char state[5000000] = "state";
    //char input[5000000] = "input";
    int dataSize = 500;
    int callCount = 2;
    if (argc > 1) dataSize = atoi(argv[1]);
    if (argc > 2) callCount = atoi(argv[2]);
    char *state = taosMemoryMalloc(dataSize);
    char *input = taosMemoryMalloc(dataSize);
    SUdfDataBlock blockInput = {.data = input, .size = dataSize};
    SUdfDataBlock blockOutput;
    char* newState;
    int32_t newStateSize;
    for (int l = 0; l < callCount; ++l) {
        callUdf(handle, 0, state, dataSize, blockInput, &newState, &newStateSize, &blockOutput);
    }
    taosMemoryFree(state);
    taosMemoryFree(input);
    teardownUdf(handle);

    stopUdfService();
}
