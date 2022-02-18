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
	//The result is failure
        const char config1[128] = "{\"debugFlag\":\"9999999999999999999999999\"}";//input the parameter which want to be configured
        taos_set_config(config1);    //configure the parameter

        SGlobalCfg *cfg1 ;

        cfg1  = taosGetConfigOption("debugFlag");//check the option result
        if(cfg1->cfgStatus == 3)                         //If cfgStatus is 3,it means configure is success
                printf("config debugFlag '9999999999999999999999999\n");
        else
                printf("config debugFlag failure!\n");

	//case2:
        //Try again with right parameter
	//The result is failure
        const char config2[128] = "{\"debugFlag\":\"135\"}";//input the parameter which want to be configured
        taos_set_config(config2);    //configure the parameter

        SGlobalCfg *cfg2 ;

        cfg2  = taosGetConfigOption("debugFlag");//check the option result
        if(cfg2->cfgStatus == 3)                         //If cfgStatus is 3,it means configure is success
                printf("config debugflag '135'success!\n");
        else
                printf("config debugflag failure!\n");
        return 0 ;

}
