#pragma once

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <time.h>
#include <ctype.h>
#include <inttypes.h>
#include <math.h>

#define DIMOF(x) (sizeof(x) / sizeof((x)[0]))
#define INT64_NULL 0x8000000000000000L

struct Record
{
    union {
        char tbname[64];
        Record* next;
    };

    int64_t catchtime;
    int64_t TIMESTAMP;
    int64_t seqnum;
    int64_t imsi;
    char imei[20];
    int64_t lac;
    char equid[20];
    int64_t lacinctimer;
    char lacsettime[20];
    char homearea[20];
    char msisdn[20];
    int64_t spcode;
    char imptime[20];
    int64_t system;
    char longitude[32]; // double
    char latitude[32];  // double
    int64_t pn;
    int64_t freq;
    char mac[20];
    int64_t smssendstatus;
    int64_t rssi;
    char esn[20];
    char tmsi[20];
    char areacode[20];
    char recordtype[20];
    char relatenum[20];
    char relatehomeac[20];
    char curarea[20];
    char neid[20];
    char lai[20];
    char ci[20];
    char billtype[20];
    char calltype[20];
    char dtmf[40];
    int64_t callduration;
    int64_t cause;
    int64_t rlgtime;
    int64_t alerttime;
    int64_t connecttime;
    int64_t disconnecttime;
    char sid[20];
    int64_t idflag;
    char rawrelatenum[20];
    int64_t redirflag;
    char origcalledno[20];
    int64_t disconnecttype;
    char newlai[20];
    char newci[20];
    char newlongitude[32]; // double
    char newlatitude[32];  // double
    char voiceflag[20];
    char voicekeya[20];
    char voicekeyb[20];
    char peersid[20];
    char oldlai[20];
    char oldci[20];
    char oldlongitude[32]; // double
    char oldlatitude[32];  // double
    int64_t stated;
    int64_t sendtime;
    char message[2000];
    char msgtag[20];
    int64_t BRAND;
    int64_t PCI;
    int64_t USERNAME;
    int64_t TERMINATECAUSE;
    int64_t WX_OPEN_ID;
    int64_t WX_TID;
    int64_t MSISDN_FY_TF;
    int64_t MSISDN_FY_TIME;
    int64_t IMSI_FY_TF;
    int64_t IMSI_FY_TIME;
    int64_t IMEI_FY_TF;
    int64_t IMEI_FY_TIME;
    int64_t MAC_FY_TF;
    int64_t MAC_FY_TIME;
    int64_t USERNAME_FY_TF;
    int64_t USERNAME_FY_TIME;

    char equipment[20];
    char number[20];
    char type[20];
    char address[20];
};

int parse_line_rd( char* line, Record* r );
int parse_line_cdr( char* line, Record* r );
int parse_line_evt( char* line, Record* r );
int parse_line_sms( char* line, Record* r );
