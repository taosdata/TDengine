#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
	char *buf = calloc(1, 4);
	int n = snprintf(buf, 4, "size");	

	printf("write size:%d \t buf:%s \t len:%d\n", n, buf, (int)(strlen(buf)));
	buf[4] = 10;
	return 1;
}
