#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <signal.h>
#include "libmseed.h"


#define  MAX_TSQL_LEN  65535


int quit = 0;


void handler(int sig) {
   fprintf(stderr, "SIGINT received, quit\r\n");
   quit = 1;
}


int main(int argc, char *argv[])
{
    int               opt, n, i, retcode;
    int               status       = 0;
    int               verbose      = 0;
    uint32_t          flags        = 0;
    MS3Record        *msr          = NULL;
    const char       *if_name      = NULL;
    const char       *of_name      = NULL;
    FILE             *fp           = NULL;
    char              net[LM_SIDLEN], stat[LM_SIDLEN], loc[LM_SIDLEN], chan[LM_SIDLEN];
    char              timestr[64];
    int32_t          *idata;
    int64_t           samples, npts, time;
    struct sigaction  act;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s -i infile -o outfile\r\n", argv[0]);
        exit(1);
    } 

    while ((opt = getopt(argc, argv, "i:o:")) != -1) {   
        switch (opt) {
            case 'i':
                if_name = strdup(optarg);
                break;
            case 'o':
                of_name = strdup(optarg);
                break;
            default:
                fprintf(stderr, "Usage: %s -i infile -o outfile\r\n", argv[0]);
                exit(1);
        }
    }

    if (if_name == NULL || if_name[0] == '\0') {
        fprintf(stderr, "the option -i was missing!\r\n");
        exit(1);
    }

    if (of_name == NULL || of_name[0] == '\0') {
        fprintf(stderr, "the option -o was missing!\r\n");
        exit(1);
    }

    act.sa_handler = handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, 0);

    usleep(500000);

    fp = fopen(of_name, "w");
    if (fp == NULL) {
        fprintf(stderr, "failed to open %s\r\n", of_name);
        exit(1);
    }

    fprintf(stderr, "running extract data from %s to %s, please wait...\r\n", if_name, of_name);

    samples = 0;
    flags = MSF_UNPACKDATA | MSF_VALIDATECRC |  MSF_PNAMERANGE;

    // read from miniseed and inert into database
    while ((retcode = ms3_readmsr(&msr, if_name, NULL, NULL, flags, verbose)) == MS_NOERROR) {
        if (quit == 1) {
            break;
        }

        npts = msr->numsamples;
        if (npts <= 0) {
            continue;
        }

        samples += npts;

        time = (int64_t) round(msr->starttime);
        idata = (int32_t *) msr->datasamples;
        n = (int) round(1000 * 1000 * 1000.0 / msr->samprate);

        memset(net, 0, LM_SIDLEN);
        memset(stat, 0, LM_SIDLEN);
        memset(loc, 0, LM_SIDLEN);
        memset(chan, 0, LM_SIDLEN);

        if (ms_sid2nslc(msr->sid, net, stat, loc, chan)) {
            fprintf(stderr, "failed to parse sid: %s\r\n", msr->sid);
            break;
        }

        for (i = 0; i < npts; i++) {
            ms_nstime2timestrz(time, timestr, ISOMONTHDAY, MICRO);
            fprintf(fp, "%s %d\n", timestr, idata[i]);
            time += n;
        }
    }

    if (retcode != MS_ENDOFFILE) {
        ms_log(2, "cannot read %s: %s\r\n", if_name, ms_errorstr (retcode));
    }

    if (status == 0) {
        fprintf(stderr, "done, inserted %ld record(s)\r\n", samples);
    }
 
    /* cleanup memory and close file */
    if (msr) {
        ms3_readmsr(&msr, NULL, NULL, NULL, flags, verbose);
    }

    if (fp) {
        fclose(fp);
    }

    return status;
}
