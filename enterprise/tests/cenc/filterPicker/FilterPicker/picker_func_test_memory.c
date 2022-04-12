#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "sachdr.h"
#include "ew_bridge.h"
#include "PickData.h"
#include "FilterPicker5_Memory.h"
#include "FilterPicker5.h"
#define DEBUG 1

//void MonthDay(int year, int yearday, int* pmonth, int* pday);

int main(int argc, char *argv[]) {

	
	//1-该部分不需要任何改动，主要用于初始化参数
    int n;
    BOOLEAN_INT useMemory = TRUE_INT; 
    double longTermWindow = 10.0; 
    double threshold1 = 8.0;   
    double threshold2 = 8.0; 
    double tUpEvent = 0.5; 
    double filterWindow = 4.0; 
    double dt = 0.01;

    filterWindow = 300.0 * dt;  
    long iFilterWindow = (long) (0.5 + filterWindow * 1000.0);
    if (iFilterWindow > 1)
        filterWindow = (double) iFilterWindow / 1000.0;
    //
    longTermWindow = 500.0 * dt; 
    long ilongTermWindow = (long) (0.5 + longTermWindow * 1000.0);
    if (ilongTermWindow > 1)
        longTermWindow = (double) ilongTermWindow / 1000.0;

    tUpEvent = 20.0 * dt;
    long itUpEvent = (long) (0.5 + tUpEvent * 1000.0);
    if (itUpEvent > 1)
        tUpEvent = (double) itUpEvent / 1000.0;

    PickData** pick_list_definative = NULL;
    int num_picks_definative = 0;
    FilterPicker5_Memory* mem = NULL;

	//2- 从文本文件读取地震计的采样值，仅为示例，实际应从时序数据库的流数据表获取采样值
    int read_samples = 0;
	FILE *fp2;
    fp2=fopen("./result.txt","r");
    if(!fp2)
    {
        printf("error!");
        exit(1);
    }
    struct data_res
    {
        char time[1000][28];
        float amp[1000];
    };
    struct data_res mydata;
    int myi=0;
	
	//3- 获取每最新的1000个采样值，调用Pick函数。注意示例代码中是while循环读取文本文件中的数据，实际应从时序数据库的流数据表获取采样值
    while (!feof(fp2)) { 
        PickData **pick_list = NULL; // array of num_picks ptrs to PickData structures/objects containing returned picks
        int num_picks = 0;

        for(myi=0;myi<1000;myi++) {
            fscanf(fp2, "%s %f", mydata.time[myi],&mydata.amp[myi]);
        }
		//
        read_samples=1000;
        
		//pick函数用于检测地震波信号。
		//时序数据库某个表（对应某个设备）流入1000个采样点后，构造1000个采样值(float类型) 的数组，作为pick方法的输入(如下面mydata.amp)所示。调用pick方法。
		Pick(
                0.01,
                mydata.amp,
                read_samples,
                filterWindow,   //多少道滤波
                longTermWindow, //长期平均值时间窗
                threshold1,     //平均值阈值
                threshold2,     //积分阈值
                tUpEvent,       //积分时间窗
                &mem,
                useMemory,
                &pick_list,
                &num_picks,
                "TEST"
        );

        if (0) printf("picker_func_test_memory: num_picks: %d\n", num_picks);
		
		//如果pick方法检测到地震波信号，则通过num_picks和pick_list返回。
        for (n = 0; n < num_picks; n++) {
            PickData* pick = *(pick_list + n);
            int index = (int) (pick->indices[0] * 0.5  + pick->indices[1] * 0.5 );
            printf("%s\n",mydata.time[index]);	// 示例程序中检测到地震波信号，打印出该点的时间，在测试时序数据库时，应存入另一张结果表中。
        }
        taosMemoryFree(pick_list); 
    } 
    fclose(fp2);
    free_PickList(pick_list_definative, num_picks_definative); // PickData objects freed here
    free_FilterPicker5_Memory(&mem);

    return (0);
}



/** date functions */
#if 0

static char daytab[2][13] = {
    {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

/** function to set month / day from day of year */

void MonthDay(int year, int yearday, int* pmonth, int* pday) {
    int i, leap;

    leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    for (i = 1; yearday > daytab[leap][i]; i++)
        yearday -= daytab[leap][i];
    *pmonth = i;
    *pday = yearday;

}

#endif
