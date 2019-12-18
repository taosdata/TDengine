#include "jinfo.h"


static int parse_int64( const char* s, int64_t* result )
{
    if( *s == 0 )
    {
        *result = INT64_NULL;
        return 1;
    }

    char* e;
    *result = strtol( s, &e, 10 );

    if( *e != 0 )
        return 0;

    return e - s + 1;
}



#define PARSE_INT64( result ) \
    do { \
        ++column; \
        int n = parse_int64(line, &(result)); \
        if( n == 0 )    \
        { \
            printf( "%s ", line ); \
            return column; \
        } \
        line += n; \
    } while( 0 );



#define PARSE_STRING( result ) \
    do { \
        ++column;   \
        size_t l = strlen( line ) + 1; \
        memcpy( result, line, l ); \
        line += l; \
    } while( 0 );



#define PARSE_DOUBLE( result ) \
    do { \
        ++column;   \
        char* e; \
        strtod( line, &e ); \
        if( *e != 0 ) \
        { \
            printf( "%s ", line ); \
            return column; \
        } \
        size_t l = e - line + 1; \
        memcpy( result, line, l ); \
        line += l; \
    } while( 0 );



static void split_comma( char* s )
{
    while( *s != 0 )
    {
        if( *s == ',' || *s == '\n' || *s == '\r' )
            *s = 0;
        ++s;
    }
}

static void split( char* str, const char* sep )
{
    size_t sl = strlen( sep );
    char* s = str;
    for( char* e = strstr(s, sep); e != NULL; e = strstr(s, sep) )
    {
        memcpy( str, s, e - s );
        str += e - s;
        *(str++) = 0;
        s = e + sl;
    }
    strcpy( str, s );
    for( int i = (int)strlen(str) - 1; i >= 0; --i )
        if( str[i] == '\n' || str[i] == '\r' )
            str[i] = 0;
}


int parse_line_cdr( char* line, Record* r )
{
    int column = 0;
    split_comma( line );

    PARSE_INT64( r->catchtime )     // BEGINTIME
    PARSE_STRING( r->msisdn )       // MSISDN
    PARSE_STRING( r->homearea )     // HOMEAREA
    PARSE_STRING( r->relatenum )    // RELATENUM
    PARSE_STRING( r->relatehomeac ) // RELATEHOMEAC
    PARSE_INT64( r->imsi )          // IMSI
    PARSE_STRING( r->imei )         // IMEI
    PARSE_STRING( r->curarea )      // CURAREA
    PARSE_STRING( r->neid )         // NEID
    PARSE_STRING( r->lai )          // LAI
    PARSE_STRING( r->ci )           // CI
    PARSE_DOUBLE( r->longitude )    // LONGITUDE
    PARSE_DOUBLE( r->latitude )     // LATITUDE
    PARSE_STRING( r->billtype )     // BILLTYPE
    PARSE_STRING( r->calltype )     // CALLTYPE
    PARSE_STRING( r->dtmf )         // DTMF
    PARSE_INT64( r->callduration )  // CALLDURATION
    PARSE_INT64( r->cause )         // CAUSE
    PARSE_INT64( r->rlgtime )       // RLGTIME
    PARSE_INT64( r->alerttime )     // ALERTTIME
    PARSE_INT64( r->connecttime )   // CONNECTTIME
    PARSE_INT64( r->disconnecttime )// DISCONNECTTIME
    PARSE_STRING( r->sid )          // SID
    PARSE_INT64( r->idflag )        // IDFLAG
    PARSE_STRING( r->rawrelatenum ) // RAWRELATENUM
    PARSE_INT64( r->redirflag )     // REDIRFLAG
    PARSE_STRING( r->origcalledno ) // ORIGCALLEDNO
    PARSE_INT64( r->disconnecttype )// DISCONNECTTYPE
    PARSE_STRING( r->newlai )       // NEWLAI
    PARSE_STRING( r->newci )        // NEWCI
    PARSE_DOUBLE( r->newlongitude )  // NEWLONGITUDE
    PARSE_DOUBLE( r->newlatitude )   // NEWLATITUDE
    PARSE_STRING( r->tmsi )         // TMSI
    PARSE_INT64( r->spcode )        // SPCODE
    PARSE_STRING( r->voiceflag )    // VOICEFLAG
    PARSE_STRING( r->voicekeya )    // VOICEKEYA
    PARSE_STRING( r->voicekeyb )    // VOICEKEYB
    PARSE_STRING( r->peersid )      // PEERSID

    sprintf( r->equipment, "%" PRId64 "-%s-%s", r->spcode, r->lai, r->ci );
    r->type[0] = '2';

    sprintf( r->tbname, "wfw%" PRId64 "_%s_%s", r->spcode, r->lai, r->ci );

    r->TIMESTAMP = INT64_NULL;
    r->seqnum = INT64_NULL;
    r->lac = INT64_NULL;
    r->lacinctimer = INT64_NULL;
    r->system = INT64_NULL;
    r->pn = INT64_NULL;
    r->freq = INT64_NULL;
    r->smssendstatus = INT64_NULL;
    r->rssi = INT64_NULL;
    r->stated = INT64_NULL;
    r->sendtime = INT64_NULL;
    r->BRAND = INT64_NULL;
    r->PCI = INT64_NULL;
    r->USERNAME = INT64_NULL;
    r->TERMINATECAUSE = INT64_NULL;
    r->WX_OPEN_ID = INT64_NULL;
    r->WX_TID = INT64_NULL;
    r->MSISDN_FY_TF = INT64_NULL;
    r->MSISDN_FY_TIME = INT64_NULL;
    r->IMSI_FY_TF = INT64_NULL;
    r->IMSI_FY_TIME = INT64_NULL;
    r->IMEI_FY_TF = INT64_NULL;
    r->IMEI_FY_TIME = INT64_NULL;
    r->MAC_FY_TF = INT64_NULL;
    r->MAC_FY_TIME = INT64_NULL;
    r->USERNAME_FY_TF = INT64_NULL;
    r->USERNAME_FY_TIME = INT64_NULL;

    return 0;
}



int parse_line_evt( char* line, Record* r)
{
    int column = 0;
    split_comma( line );

    PARSE_INT64( r->catchtime )     // BEGINTIME
    PARSE_STRING( r->calltype )     // CALLTYPE
    PARSE_STRING( r->msisdn )       // MSISDN
    PARSE_STRING( r->homearea )     // HOMEAREA
    PARSE_STRING( r->relatenum )    // RELATENUM
    PARSE_STRING( r->relatehomeac ) // RELATEHOMEAC
    PARSE_INT64( r->imsi )          // IMSI
    PARSE_STRING( r->imei )          // IMEI
    PARSE_STRING( r->curarea )      // CURAREA
    PARSE_STRING( r->neid )         // NEID
    PARSE_STRING( r->lai )          // LAI
    PARSE_STRING( r->ci )           // CI
    PARSE_DOUBLE( r->longitude )    // LONGITUDE
    PARSE_DOUBLE( r->latitude )     // LATITUDE
    PARSE_STRING( r->oldlai )       // OLDLAI
    PARSE_STRING( r->oldci )        // OLDCI
    PARSE_DOUBLE( r->oldlongitude ) // OLDLONGITUDE
    PARSE_DOUBLE( r->oldlatitude )  // OLDLATITUDE
    PARSE_STRING( r->sid )          // SID
    PARSE_INT64( r->stated )        // STATE
    PARSE_INT64( r->idflag )        // IDFLAG
    PARSE_STRING( r->dtmf )         // DTMF
    PARSE_STRING( r->tmsi )         // TMSI
    PARSE_INT64( r->spcode )        // SPCODE

    sprintf( r->equipment, "%" PRId64 "-%s-%s", r->spcode, r->lai, r->ci );
    r->type[0] = '2';
    sprintf( r->tbname, "wfw%" PRId64 "_%s_%s", r->spcode, r->lai, r->ci );

    r->TIMESTAMP = INT64_NULL;
    r->seqnum = INT64_NULL;
    r->lac = INT64_NULL;
    r->lacinctimer = INT64_NULL;
    r->system = INT64_NULL;
    r->pn = INT64_NULL;
    r->freq = INT64_NULL;
    r->smssendstatus = INT64_NULL;
    r->rssi = INT64_NULL;
    r->callduration = INT64_NULL;
    r->cause = INT64_NULL;
    r->rlgtime = INT64_NULL;
    r->alerttime = INT64_NULL;
    r->connecttime = INT64_NULL;
    r->disconnecttime = INT64_NULL;
    r->redirflag = INT64_NULL;
    r->disconnecttype = INT64_NULL;
    r->sendtime = INT64_NULL;
    r->BRAND = INT64_NULL;
    r->PCI = INT64_NULL;
    r->USERNAME = INT64_NULL;
    r->TERMINATECAUSE = INT64_NULL;
    r->WX_OPEN_ID = INT64_NULL;
    r->WX_TID = INT64_NULL;
    r->MSISDN_FY_TF = INT64_NULL;
    r->MSISDN_FY_TIME = INT64_NULL;
    r->IMSI_FY_TF = INT64_NULL;
    r->IMSI_FY_TIME = INT64_NULL;
    r->IMEI_FY_TF = INT64_NULL;
    r->IMEI_FY_TIME = INT64_NULL;
    r->MAC_FY_TF = INT64_NULL;
    r->MAC_FY_TIME = INT64_NULL;
    r->USERNAME_FY_TF = INT64_NULL;
    r->USERNAME_FY_TIME = INT64_NULL;

    return 0;
}



int parse_line_sms( char* line, Record* r)
{
    int column = 0;
    split( line, "${sp}" );

    PARSE_INT64( r->catchtime )     // BEGINTIME
    PARSE_STRING( r->msisdn )       // MSISDN
    PARSE_STRING( r->homearea )     // HOMEAREA
    PARSE_STRING( r->relatenum )    // RELATENUM
    PARSE_STRING( r->relatehomeac ) // RELATEHOMEAC
    PARSE_INT64( r->imsi )          // IMSI
    PARSE_STRING( r->imei )         // IMEI
    PARSE_STRING( r->curarea )      // CURAREA
    PARSE_STRING( r->neid )         // NEID
    PARSE_STRING( r->lai )          // LAI
    PARSE_STRING( r->ci )           // CI
    PARSE_DOUBLE( r->longitude )    // LONGITUDE
    PARSE_DOUBLE( r->latitude )     // LATITUDE
    PARSE_STRING( r->calltype )     // CALLTYPE
    PARSE_INT64( r->sendtime )      // SENDTIME
    PARSE_STRING( r->message )      // MESSAGE
    PARSE_STRING( r->sid )          // SID
    PARSE_STRING( r->msgtag )       // MSGTAG
    PARSE_INT64( r->idflag )        // IDFLAG
    PARSE_STRING( r->rawrelatenum ) // RAWRELATENUM
    PARSE_STRING( r->tmsi )         // TMSI
    PARSE_INT64( r->spcode )        // SPCODE

    sprintf( r->equipment, "%" PRId64 "-%s-%s", r->spcode, r->lai, r->ci );
    r->type[0] = '2';
    sprintf( r->tbname, "wfw%" PRId64 "_%s_%s", r->spcode, r->lai, r->ci );

    r->TIMESTAMP = INT64_NULL;
    r->seqnum = INT64_NULL;
    r->lac = INT64_NULL;
    r->lacinctimer = INT64_NULL;
    r->system = INT64_NULL;
    r->pn = INT64_NULL;
    r->freq = INT64_NULL;
    r->smssendstatus = INT64_NULL;
    r->rssi = INT64_NULL;
    r->callduration = INT64_NULL;
    r->cause = INT64_NULL;
    r->rlgtime = INT64_NULL;
    r->alerttime = INT64_NULL;
    r->connecttime = INT64_NULL;
    r->disconnecttime = INT64_NULL;
    r->redirflag = INT64_NULL;
    r->disconnecttype = INT64_NULL;
    r->stated = INT64_NULL;
    r->BRAND = INT64_NULL;
    r->PCI = INT64_NULL;
    r->USERNAME = INT64_NULL;
    r->TERMINATECAUSE = INT64_NULL;
    r->WX_OPEN_ID = INT64_NULL;
    r->WX_TID = INT64_NULL;
    r->MSISDN_FY_TF = INT64_NULL;
    r->MSISDN_FY_TIME = INT64_NULL;
    r->IMSI_FY_TF = INT64_NULL;
    r->IMSI_FY_TIME = INT64_NULL;
    r->IMEI_FY_TF = INT64_NULL;
    r->IMEI_FY_TIME = INT64_NULL;
    r->MAC_FY_TF = INT64_NULL;
    r->MAC_FY_TIME = INT64_NULL;
    r->USERNAME_FY_TF = INT64_NULL;
    r->USERNAME_FY_TIME = INT64_NULL;

    return 0;
}



int parse_line_rd( char* line, Record* r)
{
    int column = 0;
    char foo[20];
    int64_t t;
    struct tm tm = { 0 };
    split_comma( line );

    PARSE_INT64( t );        // DATETIME
    tm.tm_year = (int)( t / 10000000000l ) - 1900;
    tm.tm_mon = (int)( t % 10000000000l / 100000000l ) - 1;
    tm.tm_mday = (int)( t % 10000000l / 1000000l );
    tm.tm_hour = (int)( t % 1000000l / 10000l );
    tm.tm_min = (int)( t % 10000l / 100l );
    tm.tm_sec = (int)( t % 100l );
    r->catchtime = mktime( &tm ); 

    PARSE_STRING( r->homearea );        // HOMEAREA
    PARSE_STRING( r->msisdn );          // MSISDN
    PARSE_INT64( r->imsi );             // IMSI
    PARSE_STRING( r->imei );            // IMEI
    PARSE_STRING( r->tmsi );            // TMSI
    PARSE_STRING( r->areacode );        // AREACODE
    PARSE_STRING( r->equid );           // EQUID
    PARSE_DOUBLE( r->longitude );       // LONGITUDE
    PARSE_DOUBLE( r->latitude );        // LATITUDE
    PARSE_STRING( r->mac );             // MAC
    PARSE_INT64( r->rssi );             // RSSI
    PARSE_INT64( r->lac );              // LAC
    PARSE_INT64( r->lacinctimer );      // INCTIMER
    PARSE_STRING( foo )                 // AREACODETIME
    PARSE_INT64( r->spcode );           // SPCODE
    PARSE_STRING( foo )                 // NETWORK
    PARSE_STRING( r->esn );             // ESN
    PARSE_INT64( r->pn );               // PN
    PARSE_STRING( foo )                 // FREQUENCE
    PARSE_INT64( r->smssendstatus );    // SMSSENDSTATUS
    PARSE_INT64( r->seqnum );           // SEQNUM
    PARSE_INT64( r->BRAND );            // BRAND
    PARSE_INT64( r->PCI );              // PCI
    PARSE_INT64( r->USERNAME );         // USERNAME
    PARSE_INT64( r->TERMINATECAUSE );   // TERMINATECAUSE
    PARSE_INT64( r->WX_OPEN_ID );       // WX_OPEN_ID
    PARSE_INT64( r->WX_TID );           // WX_TID
    PARSE_INT64( r->MSISDN_FY_TF );     // MSISDN_FY_TF
    PARSE_INT64( r->MSISDN_FY_TIME );   // MSISDN_FY_TIME
    PARSE_INT64( r->IMSI_FY_TF );       // IMSI_FY_TF
    PARSE_INT64( r->IMSI_FY_TIME );     // IMSI_FY_TIME
    PARSE_INT64( r->IMEI_FY_TF );       // IMEI_FY_TF
    PARSE_INT64( r->IMEI_FY_TIME );     // IMEI_FY_TIME
    PARSE_INT64( r->MAC_FY_TF );        // MAC_FY_TF
    PARSE_INT64( r->MAC_FY_TIME );      // MAC_FY_TIME
    PARSE_INT64( r->USERNAME_FY_TF );   // USERNAME_FY_TF
    PARSE_INT64( r->USERNAME_FY_TIME ); // USERNAME_FY_TIME

    strcpy( r->equipment, r->equid );
    r->type[0] = '1';

    sprintf( r->tbname, "rd%s", r->equid );

    r->TIMESTAMP = INT64_NULL;
    r->system = INT64_NULL;
    r->freq = INT64_NULL;
    r->callduration = INT64_NULL;
    r->cause = INT64_NULL;
    r->rlgtime = INT64_NULL;
    r->alerttime = INT64_NULL;
    r->connecttime = INT64_NULL;
    r->disconnecttime = INT64_NULL;
    r->idflag = INT64_NULL;
    r->redirflag = INT64_NULL;
    r->disconnecttype = INT64_NULL;
    r->stated = INT64_NULL;
    r->sendtime = INT64_NULL;

    return 0;
}
