#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <regex.h>

#include "comp_lib.h"

double get_curr_time_in_sec() {
    struct timeval tv;

    gettimeofday(&tv, NULL);

    return tv.tv_sec + tv.tv_usec * 1E-6;
}

int regex_match(const char * s, const char * reg) {

    regex_t regex;
    char msgbuf[100];
    int cflags = REG_EXTENDED;

    /* Compile regular expression */
    if (regcomp(&regex, reg, cflags) != 0) {   
        fprintf(stderr, "Fail to compile regex");
        exit(-1);
    }   

    /* Execute regular expression */
    int reti = regexec(&regex, s, 0, NULL, 0); 
    if (!reti) {
        regfree(&regex);
        return 1;
    }   
    else if (reti == REG_NOMATCH) {
        regfree(&regex);
        return 0;
    }   
    else {
        regerror(reti, &regex, msgbuf, sizeof(msgbuf));
        fprintf(stderr, "Regex match failed: %s\n", msgbuf);
        regfree(&regex);
        exit(-1);
    }   

    return 0;
}

