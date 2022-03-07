#include "qSqlparser.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size){
    char *new_str = (char *)malloc(size+1);
    if (new_str == NULL){
        return 0;
    }
    memcpy(new_str, data, size);
    new_str[size] = '\0';

    qSqlParse(new_str);

    free(new_str);
    return 0;
}
