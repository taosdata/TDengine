#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned char hexchars[] = "0123456789ABCDEF";

int urlencode(char *result, const int resultsz, const char *str, const int strsz)
{
    int   i, j;
    char  ch;

    if (resultsz < 0 || strsz < 0) {
        return -1;
    }

    for (i = 0, j = 0; i < strsz && j < resultsz; i++) {
        ch = str[i];
        if ((ch >= 'A' && ch <= 'Z') ||
            (ch >= 'a' && ch <= 'z') ||
            (ch >= '0' && ch <= '9') ||
            ch == '/' || ch == ';' || ch == '=' || /* non-standard */
            ch == '.' || ch == '-' || ch == '*' || ch == '_')
        {
            result[j++] = ch;
        } else if (ch == ' ') {
            result[j++] = '%';
            result[j++] = '2';
            result[j++] = '0';
        } else {
            if (j + 3 <= resultsz) {
                result[j++] = '%';
                result[j++] = hexchars[(unsigned char) ch >> 4];
                result[j++] = hexchars[(unsigned char) ch & 0xF];
            } else {
                return -2;
            }
        }
    }

    if (i == 0) {
        return 0;
    } else if (i == strsz) {
        return j;
    }

    return -2;
}

#if 0
int main()
{
    int  ret;
    char result[512];
    const char *test = "/jopens-sss/sss/retr;staList=TJ.* SX.* SH.* ZJ.* SD.* SJ.* SC.* YN.* XZ.* SN.* XJ.* QS.* ZS.*";

    memset(result, 0, sizeof(result));
    ret = urlencode(result, 511, test, strlen(test));
    if (ret > 0) {
        printf("result = %s\r\n", result);
    }

    return 0;
}
#endif
