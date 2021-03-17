1.如果系统上没有编译套件，请事先安装
2.如果系统上没有 TDengine，请事先安装
3.安装 libmseed，地址：https://github.com/iris-edu/libmseed，已经 clone 好，进入 libmseed 目录，执行 make && make install（如果要支持网络功能，请事先安装 libcurl-devel（CentOS）或者 libcurl4-openssl-dev（Unbuntu），然后执行 CFLAGS+=" -DLIBMSEED_URL" make 进行编译）即可，默认代码库安装的目录是 /usr/local/lib，头文件目录是 /usr/local/include
4.编译 fileConvert 相关的代码，执行 make 即可，在运行程序之前，需要执行 source setenv.sh，否则会提示找不到动态库 libmseed.so.3
5.fileConvert 的导入和导出功能是通过两个程序实现的，fileToDatabase 是 file -> database，databaseToFile 是 database -> file，不加任何参数时执行程序会打印使用使用方法，[] 内的为可选参数
