#include <stdio.h> 
#include <sys/time.h>
#include <stdint.h> 
#include <string.h> 
#include <tcrc32c.h>

#include "test_string.c" 

double get_curr_time();

int main(int argc, char * argv[]) {
    /* TODO: Read the string */
    /* char *  str    = "hello world"; */
    int     len = strlen(str);


    double ts1 = get_curr_time();
    uint32_t crc1 = crc32c_sf(0, str, len);
    ts1 = get_curr_time() - ts1;

    double ts2 = get_curr_time();
    uint32_t crc2 = crc32c_hw(0, str, len);
    ts2 = get_curr_time() - ts2;

    printf("Total bytes: %d bytes crc1: 0x%08x crc2: 0x%08x result: %s\n", len, crc1, crc2, crc1 == crc2 ? "Match" : "Mismatch");
    printf("Software time: %10.4f seconds\tSpeed: %5.2fG/s\n", ts1, ((double)(len))/1024./1024./1024./ts1);
    printf("Hardware time: %10.4f seconds\tSpeed: %5.2fG/s\n", ts2, ((double)(len))/1024./1024./1024./ts2);
}

double get_curr_time(){
    struct timeval tv;

    gettimeofday(&tv, NULL);

    return tv.tv_sec + tv.tv_usec * 1E-6;
}
