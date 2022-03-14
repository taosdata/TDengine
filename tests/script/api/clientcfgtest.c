// The test case to verfy TS-293
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
        //Test config firstEp success
        const char config1[128] = "{\"firstEp\":\"BCC-2:6030\",\"debugFlag\":\"135\"}";//input the parameter which want to be configured
        taos_set_config(config1);    //configure the parameter

        SGlobalCfg *cfg ;

        cfg  = taosGetConfigOption("firstEp");//check the option result
        if(cfg->cfgStatus != 3){                         //If cfgStatus is 3,it means configure is success
                printf("config firstEp 'BCC-2:6030'failures!\n");
                exit(1);
        }
        

        cfg  = taosGetConfigOption("debugFlag");//check the option result
        if(cfg->cfgStatus != 3){                         
                printf("config debugFlag '135' failures!\n");
                exit(1);
        }
        
        //case2:
        //Test config only useful at the first time
        //The result is failure
        const char config2[128] = "{\"fqdn\":\"BCC-3\"}";
        taos_set_config(config2);    //configure the parameter


        cfg  = taosGetConfigOption("fqdn");//check the option result
        if(cfg->cfgStatus == 3){                         
                printf("config firstEp to 'BCC-3' failures!\n");
                exit(1);
        }
        else{
                printf("test case success!\n");
                exit(0);
        }
        
        
        return 0 ;



}
