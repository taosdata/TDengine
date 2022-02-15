#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <taos.h>
#include "os.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tulog.h"
#include "tsocket.h"
#include "tutil.h"
extern SGlobalCfg *taosGetConfigOption(const char *option) ;
int main( int argc, char *argv[]){

         printf("start to test\n");

        //case1:
        //Test config to wrong type
        const char config1[128] = "{\"cache\":\"4\"}";//input the parameter which want to be configured
        taos_set_config(config1);    //configure the parameter

        SGlobalCfg *cfg1 ;

        cfg1  = taosGetConfigOption("cache");//check the option result
        if(cfg1->cfgStatus == 3)                         //If cfgStatus is 3,it means configure is success
                printf("config cache to '4'success!\n");
        else
                printf("config cache failure!\n");
        return 0 ;

}
