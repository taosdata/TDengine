# STMT 性能波动测试

## 1. 测试环境

192.168.1.98
脚本位置：
/home/yjshe/prk/performance.c

## 2. 结论

1. 对于周期性的写入速度**微弱**下降，是切换tbname时，缓存失效的正常现象，对于之前绑定过的tbname，stmt有缓存候写入速度快；对于之前没有绑定的表，需要重新查catalog并构建相应的vgblock
2. 对于**ws接口**的taos_stmt2_bind_param ，长时间阻塞，ws接口的统计时间（12s）和taosc stmt内部统计时间（0.5s）差距巨大，因此判断是ws过程中耗时问题
3. 对于@肖波发现的阻塞问题没有在物理机上复现，只是在98虚拟机上复现了，原因是硬件资源的瓶颈导致taosd写入阻塞，如果修改bypassFlag=2或者4或者8，短路落盘或者taosd缓存，则会明显减少阻塞情况 发现的阻塞问题没有在物理机上复现，只是在98虚拟机上复现了，原因是硬件资源的瓶颈导致taosd写入阻塞，如果修改bypassFlag=2或者4或者8，短路落盘或者taosd缓存，则会明显减少阻塞情况
4. 和采样率计算方式也有关系，假如单个stmt写入1W条数据，绑定+执行总共3s，则前两秒由于没有返回输出0row/s，第三秒是10000row/s
5. 性能的波动是wal写文件的原因，是硬件IO问题，软件解决不了。测试代码：192.168.1.43:/root/prk/TDinternal

## 3. 正常情况

发现有周期性轻微的波动
```sql
vm98% ./performance 
initEnv
initEnv done
[写入速率统计] 每秒写入速率: 64454.66 行/秒 (成功次数: 12, 总时间: 1.861774秒)
[写入速率统计] 每秒写入速率: 229701.83 行/秒 (成功次数: 23, 总时间: 1.001298秒)
[写入速率统计] 每秒写入速率: 225920.20 行/秒 (成功次数: 22, 总时间: 0.973795秒)
[写入速率统计] 每秒写入速率: 225164.05 行/秒 (成功次数: 23, 总时间: 1.021477秒)
[写入速率统计] 每秒写入速率: 233700.57 行/秒 (成功次数: 18, 总时间: 0.770216秒)
[写入速率统计] 每秒写入速率: 90096.21 行/秒 (成功次数: 20, 总时间: 2.219849秒)
[写入速率统计] 每秒写入速率: 233596.52 行/秒 (成功次数: 23, 总时间: 0.984604秒)
[写入速率统计] 每秒写入速率: 238538.87 行/秒 (成功次数: 24, 总时间: 1.006125秒)
[写入速率统计] 每秒写入速率: 216784.41 行/秒 (成功次数: 8, 总时间: 0.369030秒)
[写入速率统计] 每秒写入速率: 23452.95 行/秒 (成功次数: 2, 总时间: 0.852771秒)
[写入速率统计] 每秒写入速率: 74634.89 行/秒 (成功次数: 13, 总时间: 1.741813秒)
[写入速率统计] 每秒写入速率: 218646.97 行/秒 (成功次数: 8, 总时间: 0.365887秒)
[写入速率统计] 每秒写入速率: 36386.53 行/秒 (成功次数: 6, 总时间: 1.648962秒)
[写入速率统计] 每秒写入速率: 231453.35 行/秒 (成功次数: 23, 总时间: 0.993721秒)
[写入速率统计] 每秒写入速率: 230046.79 行/秒 (成功次数: 23, 总时间: 0.999797秒)
[写入速率统计] 每秒写入速率: 229425.24 行/秒 (成功次数: 23, 总时间: 1.002505秒)
[写入速率统计] 每秒写入速率: 230482.55 行/秒 (成功次数: 23, 总时间: 0.997906秒)
[写入速率统计] 每秒写入速率: 139472.34 行/秒 (成功次数: 1, 总时间: 0.071699秒)
[写入速率统计] 每秒写入速率: 62329.60 行/秒 (成功次数: 12, 总时间: 1.925249秒)
[写入速率统计] 每秒写入速率: 231869.16 行/秒 (成功次数: 23, 总时间: 0.991939秒)
[写入速率统计] 每秒写入速率: 238524.44 行/秒 (成功次数: 11, 总时间: 0.461169秒)
[写入速率统计] 每秒写入速率: 49829.97 行/秒 (成功次数: 5, 总时间: 1.003412秒)
[写入速率统计] 每秒写入速率: 65470.30 行/秒 (成功次数: 10, 总时间: 1.527410秒)
[写入速率统计] 每秒写入速率: 233187.03 行/秒 (成功次数: 23, 总时间: 0.986333秒)
[写入速率统计] 每秒写入速率: 222430.94 行/秒 (成功次数: 14, 总时间: 0.629409秒)
[写入速率统计] 每秒写入速率: 7239.40 行/秒 (成功次数: 1, 总时间: 1.381330秒)
[写入速率统计] 每秒写入速率: 223367.71 行/秒 (成功次数: 22, 总时间: 0.984923秒)
[写入速率统计] 每秒写入速率: 238955.95 行/秒 (成功次数: 24, 总时间: 1.004369秒)
[写入速率统计] 每秒写入速率: 201112.55 行/秒 (成功次数: 20, 总时间: 0.994468秒)
[写入速率统计] 每秒写入速率: 231244.32 行/秒 (成功次数: 23, 总时间: 0.994619秒)
[写入速率统计] 每秒写入速率: 236534.41 行/秒 (成功次数: 9, 总时间: 0.380494秒)
[写入速率统计] 每秒写入速率: 25187.85 行/秒 (成功次数: 4, 总时间: 1.588067秒)
[写入速率统计] 每秒写入速率: 224812.23 行/秒 (成功次数: 14, 总时间: 0.622742秒)
[写入速率统计] 每秒写入速率: 59191.14 行/秒 (成功次数: 6, 总时间: 1.013665秒)
[写入速率统计] 每秒写入速率: 62781.41 行/秒 (成功次数: 8, 总时间: 1.274263秒)
[写入速率统计] 每秒写入速率: 194931.10 行/秒 (成功次数: 21, 总时间: 1.077304秒)
[写入速率统计] 每秒写入速率: 225164.64 行/秒 (成功次数: 23, 总时间: 1.021475秒)
[写入速率统计] 每秒写入速率: 235367.26 行/秒 (成功次数: 22, 总时间: 0.934709秒)
[写入速率统计] 每秒写入速率: 72770.93 行/秒 (成功次数: 15, 总时间: 2.061263秒)
[写入速率统计] 每秒写入速率: 222820.24 行/秒 (成功次数: 22, 总时间: 0.987343秒)
[写入速率统计] 每秒写入速率: 233862.09 行/秒 (成功次数: 23, 总时间: 0.983486秒)
[写入速率统计] 每秒写入速率: 233600.06 行/秒 (成功次数: 23, 总时间: 0.984589秒)
[写入速率统计] 每秒写入速率: 221925.32 行/秒 (成功次数: 15, 总时间: 0.675903秒)
[写入速率统计] 每秒写入速率: 6938.31 行/秒 (成功次数: 1, 总时间: 1.441273秒)
[写入速率统计] 每秒写入速率: 51812.14 行/秒 (成功次数: 9, 总时间: 1.737045秒)
[写入速率统计] 每秒写入速率: 166580.97 行/秒 (成功次数: 19, 总时间: 1.140586秒)
[写入速率统计] 每秒写入速率: 232405.05 行/秒 (成功次数: 23, 总时间: 0.989651秒)
[写入速率统计] 每秒写入速率: 234484.67 行/秒 (成功次数: 24, 总时间: 1.023521秒)
[写入速率统计] 每秒写入速率: 232818.92 行/秒 (成功次数: 23, 总时间: 0.987892秒)
[写入速率统计] 每秒写入速率: 61344.36 行/秒 (成功次数: 12, 总时间: 1.956170秒)
[写入速率统计] 每秒写入速率: 227914.55 行/秒 (成功次数: 23, 总时间: 1.009150秒)
[写入速率统计] 每秒写入速率: 183067.25 行/秒 (成功次数: 14, 总时间: 0.764746秒)
[写入速率统计] 每秒写入速率: 54170.91 行/秒 (成功次数: 12, 总时间: 2.215211秒)
[写入速率统计] 每秒写入速率: 238157.78 行/秒 (成功次数: 24, 总时间: 1.007735秒)
[写入速率统计] 每秒写入速率: 223230.94 行/秒 (成功次数: 13, 总时间: 0.582357秒)
[写入速率统计] 每秒写入速率: 7200.47 行/秒 (成功次数: 1, 总时间: 1.388798秒)
[写入速率统计] 每秒写入速率: 216826.53 行/秒 (成功次数: 22, 总时间: 1.014636秒)
[写入速率统计] 每秒写入速率: 232767.45 行/秒 (成功次数: 23, 总时间: 0.988111秒)
[写入速率统计] 每秒写入速率: 229090.16 行/秒 (成功次数: 23, 总时间: 1.003972秒)
[写入速率统计] 每秒写入速率: 217549.20 行/秒 (成功次数: 16, 总时间: 0.735466秒)
[写入速率统计] 每秒写入速率: 60132.33 行/秒 (成功次数: 6, 总时间: 0.997799秒)
stmt2-bind [insert into power.? using power.meters tags(?,?)values(?,?,?,?)] insert Time used: 33.579891 seconds
stmt2-exec [insert into power.? using power.meters tags(?,?)values(?,?,?,?)] insert Time used: 34.085473 seconds
=== taos_stmt2_bind_param 统计信息 ===
总调用次数: 1000
最小时间: 0.027744 秒
最大时间: 0.052296 秒
平均时间: 0.033580 秒
P99时间: 0.035215 秒
总时间: 33.579891 秒

=== taos_stmt2_exec 统计信息 ===
总调用次数: 1000
最小时间: 0.008097 秒
最大时间: 1.418724 秒
平均时间: 0.034085 秒
P99时间: 1.345071 秒
总时间: 34.085473 秒
```

## 4. bypass=1

设置bypass=1排除taosd的影响，打印日志，发现波动是由于切换tbname缓存导致的，按照每秒检测写入速度，发现在切换缓存的时候写入速度明显变慢，见标注黄色代码位置：
```yaml
//部分日志，正常情况下使用了tbname缓存的绑定执行速度，要比第一次没有使用快2-3个数量级
[first]exec 0 finished,bind_time: 0.011410, exec_time: 1.515647
[not first]exec 0 finished,bind_time: 0.008321, exec_time: 0.003079
[not first]exec 0 finished,bind_time: 0.008206, exec_time: 0.003014
[not first]exec 0 finished,bind_time: 0.008216, exec_time: 0.003187
[not first]exec 0 finished,bind_time: 0.008193, exec_time: 0.003077
[not first]exec 0 finished,bind_time: 0.008046, exec_time: 0.003007
[not first]exec 0 finished,bind_time: 0.008133, exec_time: 0.003026
[not first]exec 0 finished,bind_time: 0.008057, exec_time: 0.002976
[not first]exec 0 finished,bind_time: 0.008048, exec_time: 0.003022
[not first]exec 0 finished,bind_time: 0.008065, exec_time: 0.002974
[not first]exec 0 finished,bind_time: 0.008068, exec_time: 0.002989
[not first]exec 0 finished,bind_time: 0.008071, exec_time: 0.002983
[not first]exec 0 finished,bind_time: 0.008053, exec_time: 0.002937
[not first]exec 0 finished,bind_time: 0.008037, exec_time: 0.002998
[not first]exec 0 finished,bind_time: 0.008033, exec_time: 0.002988
[not first]exec 0 finished,bind_time: 0.008053, exec_time: 0.003008
```

```sql
vm98% grep -E "每秒写入速|\[first\]" performance3.log
[first]exec 0 finished,bind_time: 0.011410, exec_time: 1.515647
[写入速率统计] 每秒写入速率: 203769.67 行/秒 (成功次数: 40, 总时间: 1.963001秒)
[写入速率统计] 每秒写入速率: 893703.71 行/秒 (成功次数: 88, 总时间: 0.984666秒)
[写入速率统计] 每秒写入速率: 899049.69 行/秒 (成功次数: 89, 总时间: 0.989934秒)
[写入速率统计] 每秒写入速率: 890467.23 行/秒 (成功次数: 88, 总时间: 0.988245秒)
[写入速率统计] 每秒写入速率: 852002.62 行/秒 (成功次数: 84, 总时间: 0.985912秒)
[写入速率统计] 每秒写入速率: 888743.77 行/秒 (成功次数: 88, 总时间: 0.990162秒)
[写入速率统计] 每秒写入速率: 887140.80 行/秒 (成功次数: 87, 总时间: 0.980679秒)
[写入速率统计] 每秒写入速率: 890928.74 行/秒 (成功次数: 88, 总时间: 0.987733秒)
[写入速率统计] 每秒写入速率: 892310.12 行/秒 (成功次数: 88, 总时间: 0.986204秒)
[写入速率统计] 每秒写入速率: 887197.19 行/秒 (成功次数: 88, 总时间: 0.991888秒)
[写入速率统计] 每秒写入速率: 882692.00 行/秒 (成功次数: 87, 总时间: 0.985621秒)
[写入速率统计] 每秒写入速率: 885876.18 行/秒 (成功次数: 83, 总时间: 0.936926秒)
[first]exec 10000 finished,bind_time: 0.007408, exec_time: 1.549591
[写入速率统计] 每秒写入速率: 166751.09 行/秒 (成功次数: 34, 总时间: 2.038967秒)
[写入速率统计] 每秒写入速率: 794883.89 行/秒 (成功次数: 79, 总时间: 0.993856秒)
[写入速率统计] 每秒写入速率: 829404.18 行/秒 (成功次数: 82, 总时间: 0.988662秒)
[写入速率统计] 每秒写入速率: 887607.13 行/秒 (成功次数: 87, 总时间: 0.980163秒)
[写入速率统计] 每秒写入速率: 887555.86 行/秒 (成功次数: 88, 总时间: 0.991487秒)
[写入速率统计] 每秒写入速率: 884120.12 行/秒 (成功次数: 87, 总时间: 0.984029秒)
[写入速率统计] 每秒写入速率: 877911.50 行/秒 (成功次数: 87, 总时间: 0.990988秒)
[写入速率统计] 每秒写入速率: 882238.62 行/秒 (成功次数: 87, 总时间: 0.986128秒)
[写入速率统计] 每秒写入速率: 884090.67 行/秒 (成功次数: 87, 总时间: 0.984062秒)
[写入速率统计] 每秒写入速率: 888015.59 行/秒 (成功次数: 88, 总时间: 0.990974秒)
[写入速率统计] 每秒写入速率: 876845.51 行/秒 (成功次数: 86, 总时间: 0.980789秒)
[写入速率统计] 每秒写入速率: 881168.63 行/秒 (成功次数: 87, 总时间: 0.987325秒)
[写入速率统计] 每秒写入速率: 860680.33 行/秒 (成功次数: 20, 总时间: 0.232374秒)
[first]exec 20000 finished,bind_time: 0.007542, exec_time: 1.514549
[写入速率统计] 每秒写入速率: 124641.54 行/秒 (成功次数: 22, 总时间: 1.765062秒)
[写入速率统计] 每秒写入速率: 890372.98 行/秒 (成功次数: 88, 总时间: 0.988350秒)
[写入速率统计] 每秒写入速率: 873114.64 行/秒 (成功次数: 86, 总时间: 0.984979秒)
[写入速率统计] 每秒写入速率: 881486.14 行/秒 (成功次数: 87, 总时间: 0.986970秒)
[写入速率统计] 每秒写入速率: 881462.47 行/秒 (成功次数: 87, 总时间: 0.986996秒)
[写入速率统计] 每秒写入速率: 880397.84 行/秒 (成功次数: 87, 总时间: 0.988190秒)
[写入速率统计] 每秒写入速率: 883088.57 行/秒 (成功次数: 87, 总时间: 0.985179秒)
[写入速率统计] 每秒写入速率: 880091.60 行/秒 (成功次数: 87, 总时间: 0.988533秒)
[写入速率统计] 每秒写入速率: 882022.12 行/秒 (成功次数: 87, 总时间: 0.986370秒)
[写入速率统计] 每秒写入速率: 882345.87 行/秒 (成功次数: 87, 总时间: 0.986008秒)
[写入速率统计] 每秒写入速率: 882329.02 行/秒 (成功次数: 87, 总时间: 0.986027秒)
[写入速率统计] 每秒写入速率: 869278.55 行/秒 (成功次数: 86, 总时间: 0.989326秒)
[写入速率统计] 每秒写入速率: 844768.73 行/秒 (成功次数: 20, 总时间: 0.236751秒)
[first]exec 30000 finished,bind_time: 0.007342, exec_time: 1.546189
[写入速率统计] 每秒写入速率: 102810.19 行/秒 (成功次数: 18, 总时间: 1.750799秒)
[写入速率统计] 每秒写入速率: 886824.72 行/秒 (成功次数: 88, 总时间: 0.992304秒)
[写入速率统计] 每秒写入速率: 885255.17 行/秒 (成功次数: 87, 总时间: 0.982767秒)
[写入速率统计] 每秒写入速率: 892670.45 行/秒 (成功次数: 88, 总时间: 0.985806秒)
[写入速率统计] 每秒写入速率: 892472.86 行/秒 (成功次数: 88, 总时间: 0.986024秒)
[写入速率统计] 每秒写入速率: 888084.63 行/秒 (成功次数: 88, 总时间: 0.990897秒)
[写入速率统计] 每秒写入速率: 883487.15 行/秒 (成功次数: 87, 总时间: 0.984734秒)
[写入速率统计] 每秒写入速率: 884051.24 行/秒 (成功次数: 87, 总时间: 0.984106秒)
[写入速率统计] 每秒写入速率: 885363.95 行/秒 (成功次数: 88, 总时间: 0.993942秒)
[写入速率统计] 每秒写入速率: 885310.17 行/秒 (成功次数: 87, 总时间: 0.982706秒)
[写入速率统计] 每秒写入速率: 890236.32 行/秒 (成功次数: 88, 总时间: 0.988502秒)
[写入速率统计] 每秒写入速率: 891958.30 行/秒 (成功次数: 88, 总时间: 0.986593秒)
[写入速率统计] 每秒写入速率: 860578.87 行/秒 (成功次数: 16, 总时间: 0.185921秒)
[first]exec 40000 finished,bind_time: 0.007419, exec_time: 1.499015
[写入速率统计] 每秒写入速率: 149351.55 行/秒 (成功次数: 27, 总时间: 1.807815秒)
[写入速率统计] 每秒写入速率: 874243.28 行/秒 (成功次数: 86, 总时间: 0.983708秒)
[写入速率统计] 每秒写入速率: 872828.37 行/秒 (成功次数: 86, 总时间: 0.985303秒)
[写入速率统计] 每秒写入速率: 877792.75 行/秒 (成功次数: 87, 总时间: 0.991122秒)
[写入速率统计] 每秒写入速率: 871060.14 行/秒 (成功次数: 86, 总时间: 0.987303秒)
[写入速率统计] 每秒写入速率: 881426.21 行/秒 (成功次数: 87, 总时间: 0.987037秒)
[写入速率统计] 每秒写入速率: 880048.91 行/秒 (成功次数: 87, 总时间: 0.988581秒)
[写入速率统计] 每秒写入速率: 882310.20 行/秒 (成功次数: 87, 总时间: 0.986048秒)
[写入速率统计] 每秒写入速率: 876319.96 行/秒 (成功次数: 86, 总时间: 0.981377秒)
[写入速率统计] 每秒写入速率: 879643.11 行/秒 (成功次数: 87, 总时间: 0.989037秒)
[写入速率统计] 每秒写入速率: 868865.63 行/秒 (成功次数: 86, 总时间: 0.989796秒)
[写入速率统计] 每秒写入速率: 873446.17 行/秒 (成功次数: 86, 总时间: 0.984606秒)
[写入速率统计] 每秒写入速率: 853431.06 行/秒 (成功次数: 19, 总时间: 0.222631秒)
[first]exec 50000 finished,bind_time: 0.008268, exec_time: 1.513709
[写入速率统计] 每秒写入速率: 129638.73 行/秒 (成功次数: 23, 总时间: 1.774161秒)
[写入速率统计] 每秒写入速率: 871990.98 行/秒 (成功次数: 86, 总时间: 0.986249秒)
[写入速率统计] 每秒写入速率: 873626.36 行/秒 (成功次数: 86, 总时间: 0.984403秒)
[写入速率统计] 每秒写入速率: 868811.95 行/秒 (成功次数: 86, 总时间: 0.989857秒)
[写入速率统计] 每秒写入速率: 872453.26 行/秒 (成功次数: 86, 总时间: 0.985726秒)
[写入速率统计] 每秒写入速率: 871398.53 行/秒 (成功次数: 86, 总时间: 0.986919秒)
[写入速率统计] 每秒写入速率: 860052.22 行/秒 (成功次数: 85, 总时间: 0.988312秒)
[写入速率统计] 每秒写入速率: 880881.51 行/秒 (成功次数: 87, 总时间: 0.987647秒)
[写入速率统计] 每秒写入速率: 833864.47 行/秒 (成功次数: 82, 总时间: 0.983373秒)
[写入速率统计] 每秒写入速率: 790080.87 行/秒 (成功次数: 78, 总时间: 0.987241秒)
[写入速率统计] 每秒写入速率: 811289.89 行/秒 (成功次数: 80, 总时间: 0.986084秒)
[写入速率统计] 每秒写入速率: 868836.03 行/秒 (成功次数: 86, 总时间: 0.989830秒)
[写入速率统计] 每秒写入速率: 878839.25 行/秒 (成功次数: 46, 总时间: 0.523418秒)
[first]exec 60000 finished,bind_time: 0.008579, exec_time: 1.533783
[写入速率统计] 每秒写入速率: 333482.52 行/秒 (成功次数: 82, 总时间: 2.458900秒)
[写入速率统计] 每秒写入速率: 883760.22 行/秒 (成功次数: 87, 总时间: 0.984430秒)
[写入速率统计] 每秒写入速率: 873679.49 行/秒 (成功次数: 86, 总时间: 0.984343秒)
[写入速率统计] 每秒写入速率: 882687.94 行/秒 (成功次数: 87, 总时间: 0.985626秒)
[写入速率统计] 每秒写入速率: 876380.68 行/秒 (成功次数: 87, 总时间: 0.992719秒)
[写入速率统计] 每秒写入速率: 852921.45 行/秒 (成功次数: 84, 总时间: 0.984850秒)
[写入速率统计] 每秒写入速率: 870104.41 行/秒 (成功次数: 86, 总时间: 0.988387秒)
[写入速率统计] 每秒写入速率: 871858.63 行/秒 (成功次数: 86, 总时间: 0.986398秒)
[写入速率统计] 每秒写入速率: 873327.95 行/秒 (成功次数: 86, 总时间: 0.984739秒)
[写入速率统计] 每秒写入速率: 868680.47 行/秒 (成功次数: 86, 总时间: 0.990007秒)
[写入速率统计] 每秒写入速率: 873287.20 行/秒 (成功次数: 86, 总时间: 0.984785秒)
[写入速率统计] 每秒写入速率: 869953.06 行/秒 (成功次数: 54, 总时间: 0.620723秒)
[first]exec 70000 finished,bind_time: 0.007332, exec_time: 1.541631
[写入速率统计] 每秒写入速率: 296405.24 行/秒 (成功次数: 70, 总时间: 2.361632秒)
[写入速率统计] 每秒写入速率: 871442.96 行/秒 (成功次数: 86, 总时间: 0.986869秒)
[写入速率统计] 每秒写入速率: 871125.23 行/秒 (成功次数: 86, 总时间: 0.987229秒)
[写入速率统计] 每秒写入速率: 868090.82 行/秒 (成功次数: 85, 总时间: 0.979160秒)
[写入速率统计] 每秒写入速率: 878069.22 行/秒 (成功次数: 87, 总时间: 0.990810秒)
[写入速率统计] 每秒写入速率: 890519.66 行/秒 (成功次数: 88, 总时间: 0.988187秒)
[写入速率统计] 每秒写入速率: 886041.28 行/秒 (成功次数: 87, 总时间: 0.981896秒)
[写入速率统计] 每秒写入速率: 875793.93 行/秒 (成功次数: 87, 总时间: 0.993384秒)
[写入速率统计] 每秒写入速率: 885448.63 行/秒 (成功次数: 87, 总时间: 0.982553秒)
[写入速率统计] 每秒写入速率: 877409.57 行/秒 (成功次数: 87, 总时间: 0.991555秒)
[写入速率统计] 每秒写入速率: 874787.52 行/秒 (成功次数: 86, 总时间: 0.983096秒)
[写入速率统计] 每秒写入速率: 870343.94 行/秒 (成功次数: 63, 总时间: 0.723852秒)
[first]exec 80000 finished,bind_time: 0.007652, exec_time: 1.506575
[写入速率统计] 每秒写入速率: 274827.46 行/秒 (成功次数: 62, 总时间: 2.255961秒)
[写入速率统计] 每秒写入速率: 851495.42 行/秒 (成功次数: 84, 总时间: 0.986500秒)
[写入速率统计] 每秒写入速率: 871295.18 行/秒 (成功次数: 86, 总时间: 0.987036秒)
[写入速率统计] 每秒写入速率: 870681.01 行/秒 (成功次数: 86, 总时间: 0.987733秒)
[写入速率统计] 每秒写入速率: 878047.91 行/秒 (成功次数: 87, 总时间: 0.990834秒)
[写入速率统计] 每秒写入速率: 872532.93 行/秒 (成功次数: 86, 总时间: 0.985636秒)
[写入速率统计] 每秒写入速率: 872377.64 行/秒 (成功次数: 86, 总时间: 0.985812秒)
[写入速率统计] 每秒写入速率: 872216.27 行/秒 (成功次数: 86, 总时间: 0.985994秒)
[写入速率统计] 每秒写入速率: 869352.72 行/秒 (成功次数: 86, 总时间: 0.989242秒)
[写入速率统计] 每秒写入速率: 862587.55 行/秒 (成功次数: 85, 总时间: 0.985407秒)
[写入速率统计] 每秒写入速率: 875026.49 行/秒 (成功次数: 86, 总时间: 0.982827秒)
[写入速率统计] 每秒写入速率: 863666.12 行/秒 (成功次数: 77, 总时间: 0.891548秒)
[first]exec 90000 finished,bind_time: 0.007429, exec_time: 1.514338
[写入速率统计] 每秒写入速率: 229159.44 行/秒 (成功次数: 48, 总时间: 2.094612秒)
[写入速率统计] 每秒写入速率: 856875.17 行/秒 (成功次数: 84, 总时间: 0.980306秒)
[写入速率统计] 每秒写入速率: 860427.98 行/秒 (成功次数: 85, 总时间: 0.987880秒)
[写入速率统计] 每秒写入速率: 856797.95 行/秒 (成功次数: 85, 总时间: 0.992066秒)
[写入速率统计] 每秒写入速率: 859206.91 行/秒 (成功次数: 85, 总时间: 0.989284秒)
[写入速率统计] 每秒写入速率: 850993.83 行/秒 (成功次数: 84, 总时间: 0.987081秒)
[写入速率统计] 每秒写入速率: 856455.03 行/秒 (成功次数: 84, 总时间: 0.980787秒)
[写入速率统计] 每秒写入速率: 857787.68 行/秒 (成功次数: 85, 总时间: 0.990921秒)
[写入速率统计] 每秒写入速率: 862411.55 行/秒 (成功次数: 85, 总时间: 0.985608秒)
[写入速率统计] 每秒写入速率: 850252.27 行/秒 (成功次数: 84, 总时间: 0.987942秒)
[写入速率统计] 每秒写入速率: 855502.60 行/秒 (成功次数: 84, 总时间: 0.981879秒)
[写入速率统计] 每秒写入速率: 854631.90 行/秒 (成功次数: 85, 总时间: 0.994580秒)
```

## 5. 加大线程数=10，测native

每次必现几个写入为0的测试时间段，都卡在执行
```yaml
[2025-09-16 10:44:03] after bind param
[2025-09-16 10:44:03] before exec
[2025-09-16 10:44:03] before bind param
[2025-09-16 10:44:03] after bind param
[2025-09-16 10:44:03] before exec
[2025-09-16 10:44:03] before bind param
[2025-09-16 10:44:03] after bind param
[2025-09-16 10:44:03] before exec
[2025-09-16 10:44:03] Runtime:   11s | Rate:    89984 rows/s | Total:  2920000 rows | Queue:   0 items | CPU Usage:  128.98% | Memory Usage: 509.11 MB | Thread Count:  75
[2025-09-16 10:44:03] Runtime:   12s | Rate:        0 rows/s | Total:  2920000 rows | Queue:   0 items | CPU Usage:   90.99% | Memory Usage: 509.11 MB | Thread Count:  75
[2025-09-16 10:44:03] Runtime:   13s | Rate:        0 rows/s | Total:  2920000 rows | Queue:   0 items | CPU Usage:  100.98% | Memory Usage: 509.11 MB | Thread Count:  75
[2025-09-16 10:44:03] after exec
[2025-09-16 10:44:03] after exec
[2025-09-16 10:44:03] before bind param

[2025-09-16 10:44:24] Write Latency Distribution: min: 19.8142ms, avg: 171.3551ms, p90: 646.4780ms, p95: 711.4185ms, p99: 2692.1988ms, max: 3268.0305ms

 close finished, stbInterlaceMode:1, statInfo: ctgGetTbMetaNum=>1, getCacheTbInfo=>0, 
 prepareNum=>1, getFiledsNum=>0, bindNum=>85, parseSqlNum=>1, bindTableNum=>850000, 
 bindRowNum=>850000, execNum=>85, settbnameAPI:850000, bindAPI:850000, addbatchAPI:85, 
 execAPI:85, prepareUs:21, getFieldsUs:0, setTbNameAllUs:49725, setTbNameMaxUs:521, 
 setTagAllUs:17139, setTagMaxUs:430, bindDataAllUs:789416, bindDataMaxUs:6480, 
 execWaitAllUs:6454223, execWaitMaxUs:782111, execUseAllUs:6454223, execUseMaxUs:3225763
```

设置bypassflag=1，测试多次没有出现过
```javascript
[2025-09-16 10:52:10] Write Latency Distribution: min: 15.9270ms, avg: 83.0207ms, p90: 258.3526ms, p95: 637.0691ms, p99: 704.4890ms, max: 796.0063ms
[2025-09-16 10:53:38] Write Latency Distribution: min: 16.3827ms, avg: 96.7448ms, p90: 484.1218ms, p95: 676.5016ms, p99: 767.0095ms, max: 830.3454ms
 [2025-09-16 10:55:18] Write Latency Distribution: min: 16.1009ms, avg: 86.4418ms, p90: 325.6887ms, p95: 653.0630ms, p99: 716.5483ms, max: 791.4841ms
```

## 6. ws+bypass=1

出现大量的rate=0情况，ws接口的统计时间和stmt内部统计时间差距巨大，因此判断是ws过程中耗时问题
```yaml
Write Latency Distribution: min: 707.5303ms, avg: 4209.3688ms, p90: 11431.3540ms, 
p95: 11882.3605ms, p99: 12599.3796ms, max: 12859.1026ms

close finished, stbInterlaceMode:1, statInfo: ctgGetTbMetaNum=>1, getCacheTbInfo=>0, 
prepareNum=>1, getFiledsNum=>1, bindNum=>92, parseSqlNum=>2, bindTableNum=>920000, 
bindRowNum=>920000, execNum=>92, settbnameAPI:920000, bindAPI:920000, addbatchAPI:92, 
execAPI:92, prepareUs:15, getFieldsUs:2, setTbNameAllUs:55419, setTbNameMaxUs:384, 
setTagAllUs:16870, setTagMaxUs:10, bindDataAllUs:912379, bindDataMaxUs:4019, 
execWaitAllUs:3632801, execWaitMaxUs:526344, execUseAllUs:3632801, execUseMaxUs:533584
```


```yaml
:  35
[2025-09-16 11:12:42] after exec
[2025-09-16 11:12:42] before bind param
[2025-09-16 11:12:42] after bind param
[2025-09-16 11:12:42] before exec
[2025-09-16 11:12:42] Runtime:    6s | Rate:     9998 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:  140.97% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:    7s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   83.98% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:    8s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   80.99% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:    9s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   95.98% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:   10s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   87.97% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:   11s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   95.97% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:   12s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:   13s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:   14s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:   15s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] Runtime:   16s | Rate:        0 rows/s | Total:   210000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 409.02 MB | Thread Count:  35
[2025-09-16 11:12:42] after exec
[2025-09-16 11:12:42] before bind param
[2025-09-16 11:12:42] after bind param
[2025-09-16 11:12:42] before exec


[2025-09-16 11:13:30] before exec
[2025-09-16 11:13:30] after exec
[2025-09-16 11:13:30] before bind param
[2025-09-16 11:13:30] Runtime:   65s | Rate:    29994 rows/s | Total:  1170000 rows | Queue:   0 items | CPU Usage:  214.96% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:13:30] Runtime:   66s | Rate:        0 rows/s | Total:  1170000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:13:46] Runtime:   67s | Rate:        0 rows/s | Total:  1170000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:13:46] Runtime:   68s | Rate:        0 rows/s | Total:  1170000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:13:46] Runtime:   69s | Rate:        0 rows/s | Total:  1170000 rows | Queue:   0 items | CPU Usage:  100.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:13:46] Runtime:   70s | Rate:        0 rows/s | Total:  1170000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:13:46] Runtime:   71s | Rate:        0 rows/s | Total:  1170000 rows | Queue:   0 items | CPU Usage:   99.99% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:13:46] Runtime:   72s | Rate:        0 rows/s | Total:  1170000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:13:46] Runtime:   73s | Rate:        0 rows/s | Total:  1170000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:13:46] after bind param
[2025-09-16 11:13:46] before exec
[2025-09-16 11:13:46] after exec


2025-09-16 11:15:14] before bind param
[2025-09-16 11:15:14] after bind param
[2025-09-16 11:15:14] before exec
[2025-09-16 11:15:14] after exec
[2025-09-16 11:15:14] before bind param
[2025-09-16 11:15:14] after bind param
[2025-09-16 11:15:30] before exec
[2025-09-16 11:15:30] after exec
[2025-09-16 11:15:30] before bind param
[2025-09-16 11:15:30] after bind param
[2025-09-16 11:15:30] before exec
[2025-09-16 11:15:30] after exec
[2025-09-16 11:15:30] before bind param
[2025-09-16 11:15:30] Runtime:  171s | Rate:    49992 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:  388.94% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] Runtime:  172s | Rate:        0 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:  100.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] Runtime:  173s | Rate:        0 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] Runtime:  174s | Rate:        0 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] Runtime:  175s | Rate:        0 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:  100.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] Runtime:  176s | Rate:        0 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] Runtime:  177s | Rate:        0 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] Runtime:  178s | Rate:        0 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] Runtime:  179s | Rate:        0 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] Runtime:  180s | Rate:        0 rows/s | Total:  3480000 rows | Queue:   0 items | CPU Usage:  102.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:15:30] after bind param
[2025-09-16 11:15:30] before exec
[2025-09-16 11:15:30] after exec
[2025-09-16 11:15:30] before bind param
[2025-09-16 11:15:30] after bind param

[2025-09-16 11:16:00] after bind param
[2025-09-16 11:16:00] before exec
[2025-09-16 11:16:00] after exec
[2025-09-16 11:16:00] before bind param
[2025-09-16 11:16:00] Runtime:  214s | Rate:    39993 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:  332.95% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:16:00] Runtime:  215s | Rate:        0 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:16:00] Runtime:  216s | Rate:        0 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51


[2025-09-16 11:16:15] Runtime:  217s | Rate:        0 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:   98.97% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:16:15] Runtime:  218s | Rate:        0 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:16:15] Runtime:  219s | Rate:        0 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:   99.99% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:16:15] Runtime:  220s | Rate:        0 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:   99.99% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:16:15] Runtime:  221s | Rate:        0 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:16:15] Runtime:  222s | Rate:        0 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:16:15] Runtime:  223s | Rate:        0 rows/s | Total:  4400000 rows | Queue:   0 items | CPU Usage:   99.98% | Memory Usage: 416.81 MB | Thread Count:  51
[2025-09-16 11:16:15] after bind param
[2025-09-16 11:16:15] before exec
[2025-09-16 11:16:15] after exec
[2025-09-16 11:16:15] before bind param
[2025-09-16 11:16:15] after bind param
[2025-09-16 11:16:15] before exec
[2025-09-16 11:16:15] after exec
```

## 7. stt_trigger=1，所有bypass情况（只有bypassFlag=4复现了）

bypass
0：正常写入
1：写入消息在 taos 客户端发送 RPC 消息前返回
2：写入消息在 taosd 服务端收到 RPC 消息后返回
4：写入消息在 taosd 服务端写入内存缓存前返回
8：写入消息在 taosd 服务端数据落盘前返回

bypass=0，复现，周期性写入阻塞
```yaml
Runtime:    1s | Rate:  1819851 rows/s | Total:   1820000 rows | Queue: 141 items | CPU Usage:  274.80% | Memory Usage:   4.80 GB | Thread Count:  99
Runtime:    2s | Rate:    29980 rows/s | Total:   1850000 rows | Queue: 141 items | CPU Usage:   13.99% | Memory Usage:   4.81 GB | Thread Count:  99
Runtime:    3s | Rate:        0 rows/s | Total:   1850000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.81 GB | Thread Count:  99
Runtime:    4s | Rate:   429932 rows/s | Total:   2280000 rows | Queue: 139 items | CPU Usage:  536.91% | Memory Usage:   4.83 GB | Thread Count:  99
Runtime:    5s | Rate:   779844 rows/s | Total:   3060000 rows | Queue: 137 items | CPU Usage:  372.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    6s | Rate:   289938 rows/s | Total:   3350000 rows | Queue: 142 items | CPU Usage:  102.98% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    7s | Rate:   769889 rows/s | Total:   4120000 rows | Queue: 139 items | CPU Usage:  280.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    8s | Rate:   719841 rows/s | Total:   4840000 rows | Queue: 140 items | CPU Usage:  283.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    9s | Rate:   389931 rows/s | Total:   5230000 rows | Queue: 138 items | CPU Usage:  149.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   10s | Rate:   539879 rows/s | Total:   5770000 rows | Queue: 138 items | CPU Usage:  188.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   11s | Rate:   709862 rows/s | Total:   6480000 rows | Queue: 140 items | CPU Usage:  271.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   12s | Rate:   849811 rows/s | Total:   7330000 rows | Queue: 138 items | CPU Usage:  335.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   13s | Rate:   559919 rows/s | Total:   7890000 rows | Queue: 138 items | CPU Usage:  223.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   14s | Rate:   309931 rows/s | Total:   8200000 rows | Queue: 139 items | CPU Usage:  108.98% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   15s | Rate:   609904 rows/s | Total:   8810000 rows | Queue: 138 items | CPU Usage:  224.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   16s | Rate:   719817 rows/s | Total:   9530000 rows | Queue: 141 items | CPU Usage:  280.94% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   17s | Rate:   189972 rows/s | Total:   9720000 rows | Queue: 139 items | CPU Usage:   67.99% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   18s | Rate:        0 rows/s | Total:   9720000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   19s | Rate:   569764 rows/s | Total:  10290000 rows | Queue: 141 items | CPU Usage:  223.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   20s | Rate:   629899 rows/s | Total:  10920000 rows | Queue: 137 items | CPU Usage:  240.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   21s | Rate:   539896 rows/s | Total:  11460000 rows | Queue: 138 items | CPU Usage:  194.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   22s | Rate:   429907 rows/s | Total:  11890000 rows | Queue: 139 items | CPU Usage:  169.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   23s | Rate:   489933 rows/s | Total:  12380000 rows | Queue: 139 items | CPU Usage:  186.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   24s | Rate:   499866 rows/s | Total:  12880000 rows | Queue: 139 items | CPU Usage:  188.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   25s | Rate:   789537 rows/s | Total:  13670000 rows | Queue: 139 items | CPU Usage:  320.83% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   26s | Rate:   619863 rows/s | Total:  14290000 rows | Queue: 138 items | CPU Usage:  229.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   27s | Rate:   609716 rows/s | Total:  14900000 rows | Queue: 137 items | CPU Usage:  225.89% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   28s | Rate:   349848 rows/s | Total:  15250000 rows | Queue: 140 items | CPU Usage:  142.94% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   29s | Rate:   509917 rows/s | Total:  15760000 rows | Queue: 138 items | CPU Usage:  205.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   30s | Rate:   279946 rows/s | Total:  16040000 rows | Queue: 140 items | CPU Usage:   99.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   31s | Rate:   799776 rows/s | Total:  16840000 rows | Queue: 141 items | CPU Usage:  310.94% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   32s | Rate:   739890 rows/s | Total:  17580000 rows | Queue: 138 items | CPU Usage:  287.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   33s | Rate:   709654 rows/s | Total:  18290000 rows | Queue: 138 items | CPU Usage:  259.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   34s | Rate:   119969 rows/s | Total:  18410000 rows | Queue: 138 items | CPU Usage:   47.98% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   35s | Rate:   739699 rows/s | Total:  19150000 rows | Queue: 142 items | CPU Usage:  296.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   36s | Rate:   109983 rows/s | Total:  19260000 rows | Queue: 137 items | CPU Usage:   56.99% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   37s | Rate:   729737 rows/s | Total:  19990000 rows | Queue: 139 items | CPU Usage:  269.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   38s | Rate:   789848 rows/s | Total:  20780000 rows | Queue: 140 items | CPU Usage:  288.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   39s | Rate:   669662 rows/s | Total:  21450000 rows | Queue: 141 items | CPU Usage:  247.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   40s | Rate:   719819 rows/s | Total:  22170000 rows | Queue: 141 items | CPU Usage:  279.94% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   41s | Rate:        0 rows/s | Total:  22170000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   42s | Rate:   379926 rows/s | Total:  22550000 rows | Queue: 139 items | CPU Usage:  149.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   43s | Rate:   589828 rows/s | Total:  23140000 rows | Queue: 142 items | CPU Usage:  227.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   44s | Rate:   669717 rows/s | Total:  23810000 rows | Queue: 140 items | CPU Usage:  262.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   45s | Rate:   649919 rows/s | Total:  24460000 rows | Queue: 137 items | CPU Usage:  264.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   46s | Rate:   819863 rows/s | Total:  25280000 rows | Queue: 139 items | CPU Usage:  320.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   47s | Rate:        0 rows/s | Total:  25280000 rows | Queue: 139 items | CPU Usage:    0.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   48s | Rate:    79969 rows/s | Total:  25360000 rows | Queue: 134 items | CPU Usage:   36.99% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   49s | Rate:   749866 rows/s | Total:  26110000 rows | Queue: 139 items | CPU Usage:  273.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   50s | Rate:   559880 rows/s | Total:  26670000 rows | Queue: 135 items | CPU Usage:  222.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   51s | Rate:   619870 rows/s | Total:  27290000 rows | Queue: 134 items | CPU Usage:  233.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   52s | Rate:   589876 rows/s | Total:  27880000 rows | Queue: 138 items | CPU Usage:  216.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   53s | Rate:   389902 rows/s | Total:  28270000 rows | Queue: 140 items | CPU Usage:  144.94% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   54s | Rate:   159917 rows/s | Total:  28430000 rows | Queue: 140 items | CPU Usage:   55.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   55s | Rate:   239887 rows/s | Total:  28670000 rows | Queue: 139 items | CPU Usage:   86.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   56s | Rate:   639893 rows/s | Total:  29310000 rows | Queue: 138 items | CPU Usage:  264.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   57s | Rate:   499798 rows/s | Total:  29810000 rows | Queue: 137 items | CPU Usage:  183.94% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   58s | Rate:   939836 rows/s | Total:  30750000 rows | Queue: 139 items | CPU Usage:  348.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   59s | Rate:   429920 rows/s | Total:  31180000 rows | Queue: 136 items | CPU Usage:  158.97% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   60s | Rate:   519850 rows/s | Total:  31700000 rows | Queue: 140 items | CPU Usage:  200.93% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   61s | Rate:        0 rows/s | Total:  31700000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   62s | Rate:   129945 rows/s | Total:  31830000 rows | Queue: 139 items | CPU Usage:   47.98% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   63s | Rate:   659718 rows/s | Total:  32490000 rows | Queue: 138 items | CPU Usage:  267.88% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   64s | Rate:   789648 rows/s | Total:  33280000 rows | Queue: 138 items | CPU Usage:  313.87% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   65s | Rate:   739714 rows/s | Total:  34020000 rows | Queue: 140 items | CPU Usage:  284.92% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   66s | Rate:   588979 rows/s | Total:  34610000 rows | Queue: 137 items | CPU Usage:  241.57% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   67s | Rate:   529879 rows/s | Total:  35140000 rows | Queue: 137 items | CPU Usage:  195.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   68s | Rate:   639847 rows/s | Total:  35780000 rows | Queue: 139 items | CPU Usage:  246.92% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   69s | Rate:        0 rows/s | Total:  35780000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   70s | Rate:        0 rows/s | Total:  35780000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   71s | Rate:   789678 rows/s | Total:  36570000 rows | Queue: 137 items | CPU Usage:  308.91% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   72s | Rate:   619892 rows/s | Total:  37190000 rows | Queue: 137 items | CPU Usage:  239.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   73s | Rate:   819859 rows/s | Total:  38010000 rows | Queue: 139 items | CPU Usage:  304.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   74s | Rate:   179974 rows/s | Total:  38190000 rows | Queue: 138 items | CPU Usage:   62.99% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   75s | Rate:        0 rows/s | Total:  38190000 rows | Queue: 138 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   76s | Rate:        0 rows/s | Total:  38190000 rows | Queue: 138 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   77s | Rate:   489882 rows/s | Total:  38680000 rows | Queue: 136 items | CPU Usage:  192.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   78s | Rate:   839836 rows/s | Total:  39520000 rows | Queue: 140 items | CPU Usage:  322.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   79s | Rate:   829856 rows/s | Total:  40350000 rows | Queue: 138 items | CPU Usage:  313.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   80s | Rate:   269958 rows/s | Total:  40620000 rows | Queue: 137 items | CPU Usage:   95.98% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   81s | Rate:   689858 rows/s | Total:  41310000 rows | Queue: 137 items | CPU Usage:  312.91% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   82s | Rate:   689687 rows/s | Total:  42000000 rows | Queue: 139 items | CPU Usage:  261.88% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   83s | Rate:        0 rows/s | Total:  42000000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   84s | Rate:        0 rows/s | Total:  42000000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   85s | Rate:    99952 rows/s | Total:  42100000 rows | Queue: 140 items | CPU Usage:   35.98% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   86s | Rate:   739841 rows/s | Total:  42840000 rows | Queue: 136 items | CPU Usage:  304.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   87s | Rate:   839833 rows/s | Total:  43680000 rows | Queue: 137 items | CPU Usage:  308.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   88s | Rate:   549898 rows/s | Total:  44230000 rows | Queue: 138 items | CPU Usage:  210.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   89s | Rate:  1029853 rows/s | Total:  45260000 rows | Queue: 141 items | CPU Usage:  390.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   90s | Rate:        0 rows/s | Total:  45260000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   91s | Rate:        0 rows/s | Total:  45260000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   92s | Rate:        0 rows/s | Total:  45260000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   93s | Rate:   199964 rows/s | Total:  45460000 rows | Queue: 135 items | CPU Usage:   90.98% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   94s | Rate:   589881 rows/s | Total:  46050000 rows | Queue: 138 items | CPU Usage:  216.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   95s | Rate:   719849 rows/s | Total:  46770000 rows | Queue: 138 items | CPU Usage:  268.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   96s | Rate:   659857 rows/s | Total:  47430000 rows | Queue: 140 items | CPU Usage:  253.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   97s | Rate:   699844 rows/s | Total:  48130000 rows | Queue: 140 items | CPU Usage:  260.91% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   98s | Rate:        0 rows/s | Total:  48130000 rows | Queue: 140 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   99s | Rate:        0 rows/s | Total:  48130000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  100s | Rate:        0 rows/s | Total:  48130000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  101s | Rate:   989854 rows/s | Total:  49120000 rows | Queue: 141 items | CPU Usage:  395.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  102s | Rate:   419928 rows/s | Total:  49540000 rows | Queue: 141 items | CPU Usage:  158.97% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  103s | Rate:   539899 rows/s | Total:  50080000 rows | Queue: 140 items | CPU Usage:  186.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  104s | Rate:   709886 rows/s | Total:  50790000 rows | Queue: 138 items | CPU Usage:  260.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  105s | Rate:   789845 rows/s | Total:  51580000 rows | Queue: 138 items | CPU Usage:  296.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  106s | Rate:        0 rows/s | Total:  51580000 rows | Queue: 138 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  107s | Rate:        0 rows/s | Total:  51580000 rows | Queue: 138 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  108s | Rate:        0 rows/s | Total:  51580000 rows | Queue: 138 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  109s | Rate:        0 rows/s | Total:  51580000 rows | Queue: 138 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  110s | Rate:   619741 rows/s | Total:  52200000 rows | Queue: 139 items | CPU Usage:  248.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  111s | Rate:   699737 rows/s | Total:  52900000 rows | Queue: 139 items | CPU Usage:  267.93% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  112s | Rate:   548276 rows/s | Total:  53450000 rows | Queue: 141 items | CPU Usage:  207.34% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  113s | Rate:   499883 rows/s | Total:  53950000 rows | Queue: 137 items | CPU Usage:  189.93% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  114s | Rate:   699642 rows/s | Total:  54650000 rows | Queue: 137 items | CPU Usage:  284.89% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  115s | Rate:   879714 rows/s | Total:  55530000 rows | Queue: 139 items | CPU Usage:  328.86% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  116s | Rate:        0 rows/s | Total:  55530000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  117s | Rate:        0 rows/s | Total:  55530000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  118s | Rate:        0 rows/s | Total:  55530000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  119s | Rate:        0 rows/s | Total:  55530000 rows | Queue: 139 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  120s | Rate:   789664 rows/s | Total:  56320000 rows | Queue: 138 items | CPU Usage:  306.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  121s | Rate:   579906 rows/s | Total:  56900000 rows | Queue: 139 items | CPU Usage:  224.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  122s | Rate:   999826 rows/s | Total:  57900000 rows | Queue: 141 items | CPU Usage:  376.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  123s | Rate:        0 rows/s | Total:  57900000 rows | Queue: 141 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  124s | Rate:        0 rows/s | Total:  57900000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  125s | Rate:        0 rows/s | Total:  57900000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  126s | Rate:        0 rows/s | Total:  57900000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  127s | Rate:        0 rows/s | Total:  57900000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  128s | Rate:        0 rows/s | Total:  57900000 rows | Queue: 141 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  129s | Rate:   709670 rows/s | Total:  58610000 rows | Queue: 139 items | CPU Usage:  283.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  130s | Rate:   689860 rows/s | Total:  59300000 rows | Queue: 140 items | CPU Usage:  253.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  131s | Rate:   649907 rows/s | Total:  59950000 rows | Queue: 140 items | CPU Usage:  241.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  132s | Rate:   509881 rows/s | Total:  60460000 rows | Queue: 137 items | CPU Usage:  188.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  133s | Rate:   639841 rows/s | Total:  61100000 rows | Queue: 139 items | CPU Usage:  235.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  134s | Rate:   749893 rows/s | Total:  61850000 rows | Queue: 141 items | CPU Usage:  288.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  135s | Rate:        0 rows/s | Total:  61850000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  136s | Rate:        0 rows/s | Total:  61850000 rows | Queue: 141 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  137s | Rate:        0 rows/s | Total:  61850000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  138s | Rate:        0 rows/s | Total:  61850000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  139s | Rate:   599831 rows/s | Total:  62450000 rows | Queue: 139 items | CPU Usage:  233.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  140s | Rate:   819884 rows/s | Total:  63270000 rows | Queue: 141 items | CPU Usage:  307.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  141s | Rate:   929844 rows/s | Total:  64200000 rows | Queue: 137 items | CPU Usage:  357.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  142s | Rate:    19995 rows/s | Total:  64220000 rows | Queue: 138 items | CPU Usage:    5.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  143s | Rate:        0 rows/s | Total:  64220000 rows | Queue: 138 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  144s | Rate:        0 rows/s | Total:  64220000 rows | Queue: 138 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  145s | Rate:        0 rows/s | Total:  64220000 rows | Queue: 138 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  146s | Rate:        0 rows/s | Total:  64220000 rows | Queue: 138 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  147s | Rate:        0 rows/s | Total:  64220000 rows | Queue: 138 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  148s | Rate:    39914 rows/s | Total:  64260000 rows | Queue: 134 items | CPU Usage:   27.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  149s | Rate:   749771 rows/s | Total:  65010000 rows | Queue: 141 items | CPU Usage:  276.93% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  150s | Rate:   629910 rows/s | Total:  65640000 rows | Queue: 139 items | CPU Usage:  240.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  151s | Rate:   809836 rows/s | Total:  66450000 rows | Queue: 140 items | CPU Usage:  310.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  152s | Rate:   559898 rows/s | Total:  67010000 rows | Queue: 140 items | CPU Usage:  228.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  153s | Rate:   849804 rows/s | Total:  67860000 rows | Queue: 140 items | CPU Usage:  336.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  154s | Rate:   309950 rows/s | Total:  68170000 rows | Queue: 141 items | CPU Usage:  111.97% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  155s | Rate:        0 rows/s | Total:  68170000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  156s | Rate:        0 rows/s | Total:  68170000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  157s | Rate:        0 rows/s | Total:  68170000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  158s | Rate:        0 rows/s | Total:  68170000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  159s | Rate:   729706 rows/s | Total:  68900000 rows | Queue: 140 items | CPU Usage:  278.92% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  160s | Rate:   849879 rows/s | Total:  69750000 rows | Queue: 137 items | CPU Usage:  323.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  161s | Rate:   519895 rows/s | Total:  70270000 rows | Queue: 135 items | CPU Usage:  200.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  162s | Rate:   269948 rows/s | Total:  70540000 rows | Queue: 139 items | CPU Usage:  102.98% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  163s | Rate:        0 rows/s | Total:  70540000 rows | Queue: 139 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  164s | Rate:        0 rows/s | Total:  70540000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  165s | Rate:        0 rows/s | Total:  70540000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  166s | Rate:        0 rows/s | Total:  70540000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  167s | Rate:        0 rows/s | Total:  70540000 rows | Queue: 139 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  168s | Rate:        0 rows/s | Total:  70540000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  169s | Rate:   709719 rows/s | Total:  71250000 rows | Queue: 140 items | CPU Usage:  283.91% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  170s | Rate:   469912 rows/s | Total:  71720000 rows | Queue: 137 items | CPU Usage:  174.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  171s | Rate:   649769 rows/s | Total:  72370000 rows | Queue: 140 items | CPU Usage:  240.92% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  172s | Rate:   679867 rows/s | Total:  73050000 rows | Queue: 140 items | CPU Usage:  252.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  173s | Rate:   719835 rows/s | Total:  73770000 rows | Queue: 140 items | CPU Usage:  288.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  174s | Rate:   719875 rows/s | Total:  74490000 rows | Queue: 138 items | CPU Usage:  273.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  175s | Rate:   789868 rows/s | Total:  75280000 rows | Queue: 139 items | CPU Usage:  306.91% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  176s | Rate:        0 rows/s | Total:  75280000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  177s | Rate:        0 rows/s | Total:  75280000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  178s | Rate:        0 rows/s | Total:  75280000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  179s | Rate:        0 rows/s | Total:  75280000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  180s | Rate:        0 rows/s | Total:  75280000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  181s | Rate:   599913 rows/s | Total:  75880000 rows | Queue: 139 items | CPU Usage:  236.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  182s | Rate:   549916 rows/s | Total:  76430000 rows | Queue: 135 items | CPU Usage:  211.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  183s | Rate:   429866 rows/s | Total:  76860000 rows | Queue: 141 items | CPU Usage:  158.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  184s | Rate:        0 rows/s | Total:  76860000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  185s | Rate:        0 rows/s | Total:  76860000 rows | Queue: 141 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  186s | Rate:        0 rows/s | Total:  76860000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  187s | Rate:        0 rows/s | Total:  76860000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  188s | Rate:        0 rows/s | Total:  76860000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  189s | Rate:        0 rows/s | Total:  76860000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  190s | Rate:        0 rows/s | Total:  76860000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  191s | Rate:    79959 rows/s | Total:  76940000 rows | Queue: 138 items | CPU Usage:   37.99% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  192s | Rate:   709802 rows/s | Total:  77650000 rows | Queue: 140 items | CPU Usage:  260.94% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  193s | Rate:   789890 rows/s | Total:  78440000 rows | Queue: 140 items | CPU Usage:  293.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  194s | Rate:   579895 rows/s | Total:  79020000 rows | Queue: 137 items | CPU Usage:  223.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  195s | Rate:   769878 rows/s | Total:  79790000 rows | Queue: 138 items | CPU Usage:  288.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  196s | Rate:   759770 rows/s | Total:  80550000 rows | Queue: 136 items | CPU Usage:  298.91% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  197s | Rate:   259943 rows/s | Total:  80810000 rows | Queue: 138 items | CPU Usage:   93.98% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  198s | Rate:   789862 rows/s | Total:  81600000 rows | Queue: 139 items | CPU Usage:  315.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  199s | Rate:        0 rows/s | Total:  81600000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  200s | Rate:        0 rows/s | Total:  81600000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  201s | Rate:        0 rows/s | Total:  81600000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  202s | Rate:        0 rows/s | Total:  81600000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  203s | Rate:        0 rows/s | Total:  81600000 rows | Queue: 139 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  204s | Rate:   289944 rows/s | Total:  81890000 rows | Queue: 139 items | CPU Usage:  110.98% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  205s | Rate:   809814 rows/s | Total:  82700000 rows | Queue: 138 items | CPU Usage:  309.93% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  206s | Rate:   479887 rows/s | Total:  83180000 rows | Queue: 140 items | CPU Usage:  188.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  207s | Rate:        0 rows/s | Total:  83180000 rows | Queue: 140 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  208s | Rate:        0 rows/s | Total:  83180000 rows | Queue: 140 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  209s | Rate:        0 rows/s | Total:  83180000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  210s | Rate:        0 rows/s | Total:  83180000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  211s | Rate:        0 rows/s | Total:  83180000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  212s | Rate:        0 rows/s | Total:  83180000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  213s | Rate:        0 rows/s | Total:  83180000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  214s | Rate:        0 rows/s | Total:  83180000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  215s | Rate:   639752 rows/s | Total:  83820000 rows | Queue: 139 items | CPU Usage:  251.92% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  216s | Rate:   418058 rows/s | Total:  84240000 rows | Queue: 139 items | CPU Usage:  158.27% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  217s | Rate:   519897 rows/s | Total:  84760000 rows | Queue: 139 items | CPU Usage:  198.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  218s | Rate:   669889 rows/s | Total:  85430000 rows | Queue: 138 items | CPU Usage:  253.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  219s | Rate:   779891 rows/s | Total:  86210000 rows | Queue: 139 items | CPU Usage:  300.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  220s | Rate:   759892 rows/s | Total:  86970000 rows | Queue: 139 items | CPU Usage:  289.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  221s | Rate:   949868 rows/s | Total:  87920000 rows | Queue: 140 items | CPU Usage:  362.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  222s | Rate:        0 rows/s | Total:  87920000 rows | Queue: 140 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  223s | Rate:        0 rows/s | Total:  87920000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  224s | Rate:        0 rows/s | Total:  87920000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  225s | Rate:        0 rows/s | Total:  87920000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  226s | Rate:        0 rows/s | Total:  87920000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  227s | Rate:        0 rows/s | Total:  87920000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  228s | Rate:        0 rows/s | Total:  87920000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  229s | Rate:   589877 rows/s | Total:  88510000 rows | Queue: 140 items | CPU Usage:  227.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  230s | Rate:   819883 rows/s | Total:  89330000 rows | Queue: 138 items | CPU Usage:  356.93% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  231s | Rate:   169956 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:   64.99% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  232s | Rate:        0 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  233s | Rate:        0 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  234s | Rate:        0 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  235s | Rate:        0 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  236s | Rate:        0 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  237s | Rate:        0 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  238s | Rate:        0 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  239s | Rate:        0 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  240s | Rate:        0 rows/s | Total:  89500000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  241s | Rate:   599869 rows/s | Total:  90100000 rows | Queue: 141 items | CPU Usage:  235.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  242s | Rate:   549917 rows/s | Total:  90650000 rows | Queue: 136 items | CPU Usage:  212.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  243s | Rate:   498047 rows/s | Total:  91150000 rows | Queue: 137 items | CPU Usage:  187.26% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  244s | Rate:   719849 rows/s | Total:  91870000 rows | Queue: 140 items | CPU Usage:  268.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  245s | Rate:   789864 rows/s | Total:  92660000 rows | Queue: 138 items | CPU Usage:  296.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  246s | Rate:   789862 rows/s | Total:  93450000 rows | Queue: 140 items | CPU Usage:  303.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  247s | Rate:   719897 rows/s | Total:  94170000 rows | Queue: 140 items | CPU Usage:  276.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  248s | Rate:    69981 rows/s | Total:  94240000 rows | Queue: 142 items | CPU Usage:   26.99% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  249s | Rate:        0 rows/s | Total:  94240000 rows | Queue: 142 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  250s | Rate:        0 rows/s | Total:  94240000 rows | Queue: 142 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  251s | Rate:        0 rows/s | Total:  94240000 rows | Queue: 142 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  252s | Rate:        0 rows/s | Total:  94240000 rows | Queue: 142 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  253s | Rate:        0 rows/s | Total:  94240000 rows | Queue: 142 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  254s | Rate:        0 rows/s | Total:  94240000 rows | Queue: 142 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  255s | Rate:   559897 rows/s | Total:  94800000 rows | Queue: 139 items | CPU Usage:  215.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  256s | Rate:   629892 rows/s | Total:  95430000 rows | Queue: 137 items | CPU Usage:  236.95% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  257s | Rate:   389920 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:  150.97% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  258s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  259s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  260s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  261s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  262s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  263s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  264s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  265s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  266s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  267s | Rate:        0 rows/s | Total:  95820000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  268s | Rate:   629878 rows/s | Total:  96450000 rows | Queue: 139 items | CPU Usage:  240.96% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  269s | Rate:   409931 rows/s | Total:  96860000 rows | Queue: 139 items | CPU Usage:  151.98% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  270s | Rate:   539903 rows/s | Total:  97400000 rows | Queue: 138 items | CPU Usage:  200.93% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  271s | Rate:   789603 rows/s | Total:  98190000 rows | Queue: 139 items | CPU Usage:  315.88% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  272s | Rate:   789840 rows/s | Total:  98980000 rows | Queue: 140 items | CPU Usage:  290.92% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  273s | Rate:   599750 rows/s | Total:  99580000 rows | Queue: 140 items | CPU Usage:  233.92% | Memory Usage:   4.88 GB | Thread Count:  98
Runtime:  274s | Rate:   419915 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:  295.94% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  275s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  276s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  277s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:    0.00% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  278s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:    2.00% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  279s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  280s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  281s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  282s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  283s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  85 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:  284s | Rate:        0 rows/s | Total: 100000000 rows | Queue:  14 items | CPU Usage:  177.97% | Memory Usage:   4.88 GB | Thread Count:  83
```

bypass=1，没有复现，写入均匀
```yaml
Runtime:    1s | Rate:  3359798 rows/s | Total:   3360000 rows | Queue: 136 items | CPU Usage:  456.20% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    2s | Rate:  3389240 rows/s | Total:   6750000 rows | Queue: 138 items | CPU Usage: 1577.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    3s | Rate:  3342953 rows/s | Total:  10130000 rows | Queue: 133 items | CPU Usage: 1578.49% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    4s | Rate:  3339258 rows/s | Total:  13470000 rows | Queue: 131 items | CPU Usage: 1570.64% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    5s | Rate:  3305113 rows/s | Total:  16780000 rows | Queue: 135 items | CPU Usage: 1567.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    6s | Rate:  3239259 rows/s | Total:  20020000 rows | Queue: 138 items | CPU Usage: 1570.65% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    7s | Rate:  3239237 rows/s | Total:  23260000 rows | Queue: 137 items | CPU Usage: 1569.60% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:    8s | Rate:  3239137 rows/s | Total:  26500000 rows | Queue: 136 items | CPU Usage: 1579.58% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:    9s | Rate:  3259049 rows/s | Total:  29760000 rows | Queue: 136 items | CPU Usage: 1578.52% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   10s | Rate:  3249046 rows/s | Total:  33010000 rows | Queue: 136 items | CPU Usage: 1573.58% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   11s | Rate:  3253455 rows/s | Total:  36280000 rows | Queue: 135 items | CPU Usage: 1576.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   12s | Rate:  3239307 rows/s | Total:  39520000 rows | Queue: 136 items | CPU Usage: 1574.65% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   13s | Rate:  3245061 rows/s | Total:  42790000 rows | Queue: 134 items | CPU Usage: 1573.86% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   14s | Rate:  3279131 rows/s | Total:  46070000 rows | Queue: 136 items | CPU Usage: 1580.62% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   15s | Rate:  3239223 rows/s | Total:  49310000 rows | Queue: 137 items | CPU Usage: 1569.62% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   16s | Rate:  3239229 rows/s | Total:  52550000 rows | Queue: 135 items | CPU Usage: 1564.62% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   17s | Rate:  3246039 rows/s | Total:  55810000 rows | Queue: 137 items | CPU Usage: 1578.23% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   18s | Rate:  3259145 rows/s | Total:  59070000 rows | Queue: 136 items | CPU Usage: 1577.59% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   19s | Rate:  3248952 rows/s | Total:  62320000 rows | Queue: 136 items | CPU Usage: 1573.53% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   20s | Rate:  3199060 rows/s | Total:  65520000 rows | Queue: 133 items | CPU Usage: 1572.50% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   21s | Rate:  3269239 rows/s | Total:  68790000 rows | Queue: 136 items | CPU Usage: 1576.64% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   22s | Rate:  3238937 rows/s | Total:  72030000 rows | Queue: 133 items | CPU Usage: 1579.49% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   23s | Rate:  3258959 rows/s | Total:  75290000 rows | Queue: 133 items | CPU Usage: 1573.46% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   24s | Rate:  3259214 rows/s | Total:  78550000 rows | Queue: 135 items | CPU Usage: 1577.64% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   25s | Rate:  3248926 rows/s | Total:  81800000 rows | Queue: 135 items | CPU Usage: 1568.49% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   26s | Rate:  3219273 rows/s | Total:  85020000 rows | Queue: 134 items | CPU Usage: 1569.64% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   27s | Rate:  3259211 rows/s | Total:  88280000 rows | Queue: 134 items | CPU Usage: 1574.64% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   28s | Rate:  3249093 rows/s | Total:  91530000 rows | Queue: 134 items | CPU Usage: 1580.55% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   29s | Rate:  3239001 rows/s | Total:  94770000 rows | Queue: 135 items | CPU Usage: 1573.54% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   30s | Rate:  3189345 rows/s | Total:  97960000 rows | Queue: 125 items | CPU Usage: 1569.67% | Memory Usage:   4.88 GB | Thread Count:  96
Runtime:   31s | Rate:  1979505 rows/s | Total:  99940000 rows | Queue:  17 items | CPU Usage: 1101.76% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:   32s | Rate:    59989 rows/s | Total: 100000000 rows | Queue:   0 items | CPU Usage:   45.99% | Memory Usage:   4.88 GB | Thread Count:  83
```

bypass=2，没有复现
```yaml
Runtime:    1s | Rate:  2409854 rows/s | Total:   2410000 rows | Queue: 129 items | CPU Usage:  606.68% | Memory Usage:   4.84 GB | Thread Count:  99
Runtime:    2s | Rate:  1479676 rows/s | Total:   3890000 rows | Queue:   0 items | CPU Usage: 1563.67% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    3s | Rate:  1749632 rows/s | Total:   5640000 rows | Queue:   0 items | CPU Usage: 1575.65% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    4s | Rate:  1729592 rows/s | Total:   7370000 rows | Queue:   0 items | CPU Usage: 1577.66% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    5s | Rate:  1759491 rows/s | Total:   9130000 rows | Queue:   1 items | CPU Usage: 1575.53% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    6s | Rate:  1729483 rows/s | Total:  10860000 rows | Queue:   0 items | CPU Usage: 1569.53% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    7s | Rate:  1719478 rows/s | Total:  12580000 rows | Queue:   0 items | CPU Usage: 1570.53% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    8s | Rate:  1739612 rows/s | Total:  14320000 rows | Queue:   0 items | CPU Usage: 1569.65% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    9s | Rate:  1749620 rows/s | Total:  16070000 rows | Queue:   0 items | CPU Usage: 1571.65% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   10s | Rate:  1739611 rows/s | Total:  17810000 rows | Queue:   0 items | CPU Usage: 1572.63% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   11s | Rate:  1739582 rows/s | Total:  19550000 rows | Queue:   0 items | CPU Usage: 1572.63% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   12s | Rate:  1739460 rows/s | Total:  21290000 rows | Queue:   0 items | CPU Usage: 1566.51% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   13s | Rate:  1719472 rows/s | Total:  23010000 rows | Queue:   0 items | CPU Usage: 1574.52% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   14s | Rate:  1759439 rows/s | Total:  24770000 rows | Queue:   0 items | CPU Usage: 1575.50% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   15s | Rate:  1709470 rows/s | Total:  26480000 rows | Queue:   0 items | CPU Usage: 1565.52% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   16s | Rate:  1749611 rows/s | Total:  28230000 rows | Queue:   0 items | CPU Usage: 1571.65% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   17s | Rate:  1739602 rows/s | Total:  29970000 rows | Queue:   0 items | CPU Usage: 1570.63% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   18s | Rate:  1749453 rows/s | Total:  31720000 rows | Queue:   0 items | CPU Usage: 1579.49% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   19s | Rate:  1749563 rows/s | Total:  33470000 rows | Queue:   0 items | CPU Usage: 1574.59% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   20s | Rate:  1748318 rows/s | Total:  35240000 rows | Queue:   0 items | CPU Usage: 1573.51% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   21s | Rate:  1758442 rows/s | Total:  37000000 rows | Queue:   0 items | CPU Usage: 1571.62% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   22s | Rate:  1719587 rows/s | Total:  38720000 rows | Queue:   0 items | CPU Usage: 1566.63% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   23s | Rate:  1744876 rows/s | Total:  40470000 rows | Queue:   0 items | CPU Usage: 1571.38% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   24s | Rate:  1739590 rows/s | Total:  42210000 rows | Queue:   0 items | CPU Usage: 1575.64% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   25s | Rate:  1714646 rows/s | Total:  43930000 rows | Queue:   0 items | CPU Usage: 1573.08% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   26s | Rate:  1739612 rows/s | Total:  45670000 rows | Queue:   0 items | CPU Usage: 1573.65% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   27s | Rate:  1749607 rows/s | Total:  47420000 rows | Queue:   0 items | CPU Usage: 1572.66% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   28s | Rate:  1729628 rows/s | Total:  49150000 rows | Queue:   0 items | CPU Usage: 1574.65% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   29s | Rate:  1749480 rows/s | Total:  50900000 rows | Queue:   0 items | CPU Usage: 1575.53% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   30s | Rate:  1729322 rows/s | Total:  52630000 rows | Queue:   0 items | CPU Usage: 1572.38% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   31s | Rate:  1729468 rows/s | Total:  54360000 rows | Queue:   0 items | CPU Usage: 1572.53% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   32s | Rate:  1739601 rows/s | Total:  56100000 rows | Queue:   0 items | CPU Usage: 1565.63% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   33s | Rate:  1735070 rows/s | Total:  57840000 rows | Queue:   0 items | CPU Usage: 1575.52% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   34s | Rate:  1749591 rows/s | Total:  59590000 rows | Queue:   0 items | CPU Usage: 1570.64% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   35s | Rate:  1729613 rows/s | Total:  61320000 rows | Queue:   0 items | CPU Usage: 1572.66% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   36s | Rate:  1739626 rows/s | Total:  63060000 rows | Queue:   0 items | CPU Usage: 1571.65% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   37s | Rate:  1709646 rows/s | Total:  64770000 rows | Queue:   0 items | CPU Usage: 1568.68% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   38s | Rate:  1739585 rows/s | Total:  66510000 rows | Queue:   0 items | CPU Usage: 1570.62% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   39s | Rate:  1739575 rows/s | Total:  68250000 rows | Queue:   0 items | CPU Usage: 1574.62% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   40s | Rate:  1749592 rows/s | Total:  70000000 rows | Queue:   0 items | CPU Usage: 1574.63% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   41s | Rate:  1729597 rows/s | Total:  71730000 rows | Queue:   0 items | CPU Usage: 1571.64% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   42s | Rate:  1729602 rows/s | Total:  73460000 rows | Queue:   0 items | CPU Usage: 1568.65% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   43s | Rate:  1729619 rows/s | Total:  75190000 rows | Queue:   0 items | CPU Usage: 1573.64% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   44s | Rate:  1759467 rows/s | Total:  76950000 rows | Queue:   0 items | CPU Usage: 1573.53% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   45s | Rate:  1709475 rows/s | Total:  78660000 rows | Queue:   0 items | CPU Usage: 1576.53% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   46s | Rate:  1729629 rows/s | Total:  80390000 rows | Queue:   0 items | CPU Usage: 1559.64% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   47s | Rate:  1719605 rows/s | Total:  82110000 rows | Queue:   0 items | CPU Usage: 1563.66% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   48s | Rate:  1749612 rows/s | Total:  83860000 rows | Queue:   0 items | CPU Usage: 1572.66% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   49s | Rate:  1719628 rows/s | Total:  85580000 rows | Queue:   0 items | CPU Usage: 1574.65% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   50s | Rate:  1719488 rows/s | Total:  87300000 rows | Queue:   0 items | CPU Usage: 1574.54% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   51s | Rate:  1749618 rows/s | Total:  89050000 rows | Queue:   0 items | CPU Usage: 1571.65% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   52s | Rate:  1729613 rows/s | Total:  90780000 rows | Queue:   0 items | CPU Usage: 1565.65% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   53s | Rate:  1749592 rows/s | Total:  92530000 rows | Queue:   0 items | CPU Usage: 1570.63% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   54s | Rate:  1709618 rows/s | Total:  94240000 rows | Queue:   0 items | CPU Usage: 1570.65% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   55s | Rate:  1669630 rows/s | Total:  95910000 rows | Queue:  11 items | CPU Usage: 1463.67% | Memory Usage:   4.88 GB | Thread Count:  98
Runtime:   56s | Rate:  1623550 rows/s | Total:  97540000 rows | Queue:   2 items | CPU Usage: 1464.17% | Memory Usage:   4.88 GB | Thread Count:  96
Runtime:   57s | Rate:  1479653 rows/s | Total:  99020000 rows | Queue:   0 items | CPU Usage: 1214.72% | Memory Usage:   4.88 GB | Thread Count:  90
Runtime:   58s | Rate:   849785 rows/s | Total:  99870000 rows | Queue:   0 items | CPU Usage:  552.88% | Memory Usage:   4.88 GB | Thread Count:  84
Runtime:   59s | Rate:   129968 rows/s | Total: 100000000 rows | Queue:   0 items | CPU Usage:   66.97% | Memory Usage:   4.88 GB | Thread Count:  83
```

bypass=4，复现了
```yaml
Runtime:    1s | Rate:  2285678 rows/s | Total:   2290000 rows | Queue: 130 items | CPU Usage:  694.51% | Memory Usage:   4.83 GB | Thread Count:  99
Runtime:    2s | Rate:  1219668 rows/s | Total:   3510000 rows | Queue:  24 items | CPU Usage: 1350.64% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    3s | Rate:  1569670 rows/s | Total:   5080000 rows | Queue:   6 items | CPU Usage: 1398.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    4s | Rate:  2019271 rows/s | Total:   7100000 rows | Queue: 141 items | CPU Usage: 1401.35% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    5s | Rate:        0 rows/s | Total:   7100000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    6s | Rate:        0 rows/s | Total:   7100000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    7s | Rate:        0 rows/s | Total:   7100000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    8s | Rate:   799600 rows/s | Total:   7900000 rows | Queue:  76 items | CPU Usage:  845.72% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    9s | Rate:   649801 rows/s | Total:   8550000 rows | Queue: 137 items | CPU Usage:  283.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   10s | Rate:   488993 rows/s | Total:   9040000 rows | Queue:  55 items | CPU Usage:  643.75% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   11s | Rate:  1639613 rows/s | Total:  10680000 rows | Queue:  19 items | CPU Usage: 1389.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   12s | Rate:  1709647 rows/s | Total:  12390000 rows | Queue:  15 items | CPU Usage: 1409.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   13s | Rate:  1669326 rows/s | Total:  14060000 rows | Queue: 139 items | CPU Usage: 1110.40% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   14s | Rate:        0 rows/s | Total:  14060000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   15s | Rate:        0 rows/s | Total:  14060000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   16s | Rate:    79959 rows/s | Total:  14140000 rows | Queue: 140 items | CPU Usage:   55.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   17s | Rate:   449786 rows/s | Total:  14590000 rows | Queue:  60 items | CPU Usage:  615.79% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   18s | Rate:   959681 rows/s | Total:  15550000 rows | Queue: 137 items | CPU Usage:  490.77% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   19s | Rate:   689666 rows/s | Total:  16240000 rows | Queue:  64 items | CPU Usage:  783.73% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   20s | Rate:  1609529 rows/s | Total:  17850000 rows | Queue:  20 items | CPU Usage: 1382.60% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   21s | Rate:  1639563 rows/s | Total:  19490000 rows | Queue:  13 items | CPU Usage: 1411.63% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   22s | Rate:  1529563 rows/s | Total:  21020000 rows | Queue: 139 items | CPU Usage:  968.75% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   23s | Rate:        0 rows/s | Total:  21020000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   24s | Rate:        0 rows/s | Total:  21020000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   25s | Rate:   109942 rows/s | Total:  21130000 rows | Queue: 138 items | CPU Usage:   75.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   26s | Rate:   709678 rows/s | Total:  21840000 rows | Queue: 115 items | CPU Usage:  646.78% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   27s | Rate:   709826 rows/s | Total:  22550000 rows | Queue: 137 items | CPU Usage:  464.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   28s | Rate:   554102 rows/s | Total:  23110000 rows | Queue:  49 items | CPU Usage:  722.34% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   29s | Rate:  1708154 rows/s | Total:  24820000 rows | Queue:  27 items | CPU Usage: 1383.51% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   30s | Rate:  1629620 rows/s | Total:  26450000 rows | Queue:  16 items | CPU Usage: 1409.67% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   31s | Rate:  1529476 rows/s | Total:  27980000 rows | Queue: 140 items | CPU Usage:  972.62% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   32s | Rate:        0 rows/s | Total:  27980000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   33s | Rate:        0 rows/s | Total:  27980000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   34s | Rate:   199943 rows/s | Total:  28180000 rows | Queue: 141 items | CPU Usage:  150.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   35s | Rate:   569724 rows/s | Total:  28750000 rows | Queue:  77 items | CPU Usage:  656.75% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   36s | Rate:   799641 rows/s | Total:  29550000 rows | Queue: 138 items | CPU Usage:  414.77% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   37s | Rate:   639693 rows/s | Total:  30190000 rows | Queue:  31 items | CPU Usage:  846.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   38s | Rate:  1749586 rows/s | Total:  31940000 rows | Queue:  15 items | CPU Usage: 1407.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   39s | Rate:  1559692 rows/s | Total:  33500000 rows | Queue:  15 items | CPU Usage: 1403.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   40s | Rate:  1439665 rows/s | Total:  34940000 rows | Queue: 139 items | CPU Usage:  886.68% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   41s | Rate:   189902 rows/s | Total:  35130000 rows | Queue: 141 items | CPU Usage:  136.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   42s | Rate:        0 rows/s | Total:  35130000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   43s | Rate:        0 rows/s | Total:  35130000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   44s | Rate:   929567 rows/s | Total:  36060000 rows | Queue:  92 items | CPU Usage:  905.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   45s | Rate:   489844 rows/s | Total:  36550000 rows | Queue: 138 items | CPU Usage:  211.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   46s | Rate:   869608 rows/s | Total:  37420000 rows | Queue:  34 items | CPU Usage: 1029.66% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   47s | Rate:  1779619 rows/s | Total:  39200000 rows | Queue:  20 items | CPU Usage: 1407.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   48s | Rate:  1669651 rows/s | Total:  40870000 rows | Queue:  38 items | CPU Usage: 1422.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   49s | Rate:  1029633 rows/s | Total:  41900000 rows | Queue: 139 items | CPU Usage:  577.72% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   50s | Rate:   219882 rows/s | Total:  42120000 rows | Queue: 139 items | CPU Usage:  167.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   51s | Rate:        0 rows/s | Total:  42120000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   52s | Rate:        0 rows/s | Total:  42120000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   53s | Rate:   819621 rows/s | Total:  42940000 rows | Queue:  81 items | CPU Usage:  863.72% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   54s | Rate:   609862 rows/s | Total:  43550000 rows | Queue: 137 items | CPU Usage:  272.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   55s | Rate:   829798 rows/s | Total:  44380000 rows | Queue:  40 items | CPU Usage:  974.82% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   56s | Rate:  1729507 rows/s | Total:  46110000 rows | Queue:  20 items | CPU Usage: 1402.60% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   57s | Rate:  1659552 rows/s | Total:  47770000 rows | Queue:  32 items | CPU Usage: 1413.61% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   58s | Rate:  1089508 rows/s | Total:  48860000 rows | Queue: 139 items | CPU Usage:  613.73% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   59s | Rate:   249925 rows/s | Total:  49110000 rows | Queue: 140 items | CPU Usage:  188.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   60s | Rate:        0 rows/s | Total:  49110000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   61s | Rate:        0 rows/s | Total:  49110000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   62s | Rate:    99727 rows/s | Total:  49210000 rows | Queue: 115 items | CPU Usage:  165.56% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   63s | Rate:  1525322 rows/s | Total:  50740000 rows | Queue:  62 items | CPU Usage: 1376.77% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   64s | Rate:  1642532 rows/s | Total:  52390000 rows | Queue:  28 items | CPU Usage: 1382.73% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   65s | Rate:  1638916 rows/s | Total:  54030000 rows | Queue:  26 items | CPU Usage: 1416.04% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   66s | Rate:  1789568 rows/s | Total:  55820000 rows | Queue: 138 items | CPU Usage: 1224.76% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   67s | Rate:   289952 rows/s | Total:  56110000 rows | Queue: 140 items | CPU Usage:  216.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   68s | Rate:        0 rows/s | Total:  56110000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   69s | Rate:        0 rows/s | Total:  56110000 rows | Queue: 140 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   70s | Rate:   519904 rows/s | Total:  56630000 rows | Queue:  54 items | CPU Usage:  686.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   71s | Rate:   919745 rows/s | Total:  57550000 rows | Queue: 136 items | CPU Usage:  445.86% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   72s | Rate:   658391 rows/s | Total:  58210000 rows | Queue:  45 items | CPU Usage:  819.02% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   73s | Rate:  1689606 rows/s | Total:  59900000 rows | Queue:  19 items | CPU Usage: 1397.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   74s | Rate:  1589646 rows/s | Total:  61490000 rows | Queue:  14 items | CPU Usage: 1408.67% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   75s | Rate:  1289514 rows/s | Total:  62780000 rows | Queue: 140 items | CPU Usage:  762.62% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   76s | Rate:   329827 rows/s | Total:  63110000 rows | Queue: 140 items | CPU Usage:  257.89% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   77s | Rate:        0 rows/s | Total:  63110000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   78s | Rate:        0 rows/s | Total:  63110000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   79s | Rate:   909837 rows/s | Total:  64020000 rows | Queue:  88 items | CPU Usage:  899.84% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   80s | Rate:   529851 rows/s | Total:  64550000 rows | Queue: 136 items | CPU Usage:  239.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   81s | Rate:   809618 rows/s | Total:  65360000 rows | Queue:  51 items | CPU Usage:  912.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   82s | Rate:  1653851 rows/s | Total:  67020000 rows | Queue:  17 items | CPU Usage: 1390.83% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   83s | Rate:  1679644 rows/s | Total:  68700000 rows | Queue:  38 items | CPU Usage: 1424.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   84s | Rate:  1039758 rows/s | Total:  69740000 rows | Queue: 139 items | CPU Usage:  578.79% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   85s | Rate:   369799 rows/s | Total:  70110000 rows | Queue: 140 items | CPU Usage:  287.83% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   86s | Rate:        0 rows/s | Total:  70110000 rows | Queue: 140 items | CPU Usage:    0.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   87s | Rate:        0 rows/s | Total:  70110000 rows | Queue: 140 items | CPU Usage:    3.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   88s | Rate:  1439237 rows/s | Total:  71550000 rows | Queue: 138 items | CPU Usage: 1133.36% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   89s | Rate:        0 rows/s | Total:  71550000 rows | Queue: 138 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   90s | Rate:  1029812 rows/s | Total:  72580000 rows | Queue:  36 items | CPU Usage: 1146.77% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   91s | Rate:  1769595 rows/s | Total:  74350000 rows | Queue:  25 items | CPU Usage: 1396.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   92s | Rate:  1599501 rows/s | Total:  75950000 rows | Queue:  67 items | CPU Usage: 1346.57% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   93s | Rate:   749767 rows/s | Total:  76700000 rows | Queue: 136 items | CPU Usage:  414.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   94s | Rate:   409911 rows/s | Total:  77110000 rows | Queue: 141 items | CPU Usage:  316.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   95s | Rate:        0 rows/s | Total:  77110000 rows | Queue: 141 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   96s | Rate:        0 rows/s | Total:  77110000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   97s | Rate:   139928 rows/s | Total:  77250000 rows | Queue: 140 items | CPU Usage:  101.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   98s | Rate:  1139634 rows/s | Total:  78390000 rows | Queue:  92 items | CPU Usage: 1071.78% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   99s | Rate:  1489652 rows/s | Total:  79880000 rows | Queue:  28 items | CPU Usage: 1364.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  100s | Rate:  1659665 rows/s | Total:  81540000 rows | Queue:  23 items | CPU Usage: 1412.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  101s | Rate:  1889433 rows/s | Total:  83430000 rows | Queue: 117 items | CPU Usage: 1419.58% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  102s | Rate:   679699 rows/s | Total:  84110000 rows | Queue: 139 items | CPU Usage:  452.80% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  103s | Rate:        0 rows/s | Total:  84110000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  104s | Rate:        0 rows/s | Total:  84110000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  105s | Rate:        0 rows/s | Total:  84110000 rows | Queue: 119 items | CPU Usage:   73.97% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  106s | Rate:  1439619 rows/s | Total:  85550000 rows | Queue: 137 items | CPU Usage: 1063.61% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  107s | Rate:   139375 rows/s | Total:  85690000 rows | Queue:  93 items | CPU Usage:  276.78% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  108s | Rate:  1429608 rows/s | Total:  87120000 rows | Queue:  11 items | CPU Usage: 1361.67% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  109s | Rate:  1689608 rows/s | Total:  88810000 rows | Queue:  13 items | CPU Usage: 1409.66% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  110s | Rate:  1809450 rows/s | Total:  90620000 rows | Queue: 139 items | CPU Usage: 1229.50% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  111s | Rate:   489769 rows/s | Total:  91110000 rows | Queue: 141 items | CPU Usage:  382.82% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  112s | Rate:        0 rows/s | Total:  91110000 rows | Queue: 141 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  113s | Rate:   159958 rows/s | Total:  91270000 rows | Queue: 141 items | CPU Usage:  117.95% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  114s | Rate:   269880 rows/s | Total:  91540000 rows | Queue:  82 items | CPU Usage:  396.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  115s | Rate:  1009755 rows/s | Total:  92550000 rows | Queue: 137 items | CPU Usage:  613.83% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  116s | Rate:   709813 rows/s | Total:  93260000 rows | Queue:  49 items | CPU Usage:  848.81% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  117s | Rate:  1709526 rows/s | Total:  94970000 rows | Queue:  22 items | CPU Usage: 1390.60% | Memory Usage:   4.87 GB | Thread Count:  98
Runtime:  118s | Rate:  1569555 rows/s | Total:  96540000 rows | Queue:  42 items | CPU Usage: 1323.65% | Memory Usage:   4.87 GB | Thread Count:  94
Runtime:  119s | Rate:   829740 rows/s | Total:  97370000 rows | Queue: 114 items | CPU Usage:  419.80% | Memory Usage:   4.87 GB | Thread Count:  94
Runtime:  120s | Rate:   399792 rows/s | Total:  97770000 rows | Queue: 105 items | CPU Usage:  327.84% | Memory Usage:   4.88 GB | Thread Count:  91
Runtime:  121s | Rate:   109949 rows/s | Total:  97880000 rows | Queue: 101 items | CPU Usage:   97.97% | Memory Usage:   4.88 GB | Thread Count:  91
Runtime:  122s | Rate:    19996 rows/s | Total:  97900000 rows | Queue: 101 items | CPU Usage:    6.00% | Memory Usage:   4.88 GB | Thread Count:  91
Runtime:  123s | Rate:   789617 rows/s | Total:  98690000 rows | Queue:  60 items | CPU Usage:  703.77% | Memory Usage:   4.88 GB | Thread Count:  90
Runtime:  124s | Rate:   139970 rows/s | Total:  98830000 rows | Queue:  71 items | CPU Usage:   61.98% | Memory Usage:   4.88 GB | Thread Count:  90
Runtime:  125s | Rate:   899614 rows/s | Total:  99730000 rows | Queue:   0 items | CPU Usage:  756.77% | Memory Usage:   4.87 GB | Thread Count:  85
Runtime:  126s | Rate:   269939 rows/s | Total: 100000000 rows | Queue:   0 items | CPU Usage:  129.95% | Memory Usage:   4.87 GB | Thread Count:  83
```

bypass=8，没有复现
```yaml
Runtime:    1s | Rate:  1759787 rows/s | Total:   1760000 rows | Queue: 142 items | CPU Usage:  512.32% | Memory Usage:   4.80 GB | Thread Count:  99
Runtime:    2s | Rate:        0 rows/s | Total:   1760000 rows | Queue: 142 items | CPU Usage:    2.00% | Memory Usage:   4.80 GB | Thread Count:  99
Runtime:    3s | Rate:        0 rows/s | Total:   1760000 rows | Queue: 142 items | CPU Usage:    0.00% | Memory Usage:   4.80 GB | Thread Count:  99
Runtime:    4s | Rate:   629865 rows/s | Total:   2390000 rows | Queue: 129 items | CPU Usage: 1014.80% | Memory Usage:   4.84 GB | Thread Count:  99
Runtime:    5s | Rate:   499893 rows/s | Total:   2890000 rows | Queue: 130 items | CPU Usage:  397.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    6s | Rate:   459831 rows/s | Total:   3350000 rows | Queue: 140 items | CPU Usage:  294.86% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    7s | Rate:   789562 rows/s | Total:   4140000 rows | Queue: 139 items | CPU Usage:  594.66% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    8s | Rate:   789570 rows/s | Total:   4930000 rows | Queue: 139 items | CPU Usage:  590.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:    9s | Rate:   679694 rows/s | Total:   5610000 rows | Queue: 131 items | CPU Usage:  569.82% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   10s | Rate:   109976 rows/s | Total:   5720000 rows | Queue: 139 items | CPU Usage:   27.99% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   11s | Rate:        0 rows/s | Total:   5720000 rows | Queue: 139 items | CPU Usage:    2.00% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   12s | Rate:   479778 rows/s | Total:   6200000 rows | Queue: 131 items | CPU Usage:  416.86% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   13s | Rate:   449902 rows/s | Total:   6650000 rows | Queue: 130 items | CPU Usage:  329.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   14s | Rate:   649803 rows/s | Total:   7300000 rows | Queue: 139 items | CPU Usage:  441.88% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   15s | Rate:   789818 rows/s | Total:   8090000 rows | Queue: 138 items | CPU Usage:  592.79% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   16s | Rate:   789605 rows/s | Total:   8880000 rows | Queue: 139 items | CPU Usage:  599.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   17s | Rate:   689667 rows/s | Total:   9570000 rows | Queue: 131 items | CPU Usage:  562.80% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   18s | Rate:   249946 rows/s | Total:   9820000 rows | Queue: 128 items | CPU Usage:  190.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   19s | Rate:   639804 rows/s | Total:  10460000 rows | Queue: 142 items | CPU Usage:  422.80% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   20s | Rate:   789547 rows/s | Total:  11250000 rows | Queue: 140 items | CPU Usage:  589.74% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   21s | Rate:   749806 rows/s | Total:  12000000 rows | Queue: 136 items | CPU Usage:  594.89% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   22s | Rate:   619899 rows/s | Total:  12620000 rows | Queue: 129 items | CPU Usage:  523.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   23s | Rate:   569875 rows/s | Total:  13190000 rows | Queue: 130 items | CPU Usage:  404.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   24s | Rate:   549872 rows/s | Total:  13740000 rows | Queue: 129 items | CPU Usage:  420.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   25s | Rate:   669832 rows/s | Total:  14410000 rows | Queue: 139 items | CPU Usage:  444.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   26s | Rate:   789793 rows/s | Total:  15200000 rows | Queue: 140 items | CPU Usage:  586.75% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   27s | Rate:   789615 rows/s | Total:  15990000 rows | Queue: 140 items | CPU Usage:  593.72% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   28s | Rate:   589503 rows/s | Total:  16580000 rows | Queue: 128 items | CPU Usage:  505.63% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   29s | Rate:   568749 rows/s | Total:  17150000 rows | Queue: 129 items | CPU Usage:  420.07% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   30s | Rate:   419897 rows/s | Total:  17570000 rows | Queue: 129 items | CPU Usage:  318.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   31s | Rate:   789788 rows/s | Total:  18360000 rows | Queue: 140 items | CPU Usage:  553.78% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   32s | Rate:   799504 rows/s | Total:  19160000 rows | Queue: 140 items | CPU Usage:  608.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   33s | Rate:   779862 rows/s | Total:  19940000 rows | Queue: 141 items | CPU Usage:  585.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   34s | Rate:   789787 rows/s | Total:  20730000 rows | Queue: 140 items | CPU Usage:  593.74% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   35s | Rate:   789595 rows/s | Total:  21520000 rows | Queue: 140 items | CPU Usage:  599.73% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   36s | Rate:   539137 rows/s | Total:  22060000 rows | Queue: 129 items | CPU Usage:  461.29% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   37s | Rate:   349527 rows/s | Total:  22410000 rows | Queue: 130 items | CPU Usage:  240.68% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   38s | Rate:   689845 rows/s | Total:  23100000 rows | Queue: 138 items | CPU Usage:  479.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   39s | Rate:   789794 rows/s | Total:  23890000 rows | Queue: 136 items | CPU Usage:  594.80% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   40s | Rate:   789587 rows/s | Total:  24680000 rows | Queue: 138 items | CPU Usage:  584.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   41s | Rate:   759633 rows/s | Total:  25440000 rows | Queue: 135 items | CPU Usage:  613.75% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   42s | Rate:   719799 rows/s | Total:  26160000 rows | Queue: 130 items | CPU Usage:  583.86% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   43s | Rate:   639877 rows/s | Total:  26800000 rows | Queue: 133 items | CPU Usage:  468.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   44s | Rate:   369630 rows/s | Total:  27170000 rows | Queue: 128 items | CPU Usage:  291.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   45s | Rate:   669845 rows/s | Total:  27840000 rows | Queue: 137 items | CPU Usage:  515.89% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   46s | Rate:   789840 rows/s | Total:  28630000 rows | Queue: 138 items | CPU Usage:  579.79% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   47s | Rate:   769600 rows/s | Total:  29400000 rows | Queue: 138 items | CPU Usage:  587.80% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   48s | Rate:   378608 rows/s | Total:  29780000 rows | Queue: 127 items | CPU Usage:  335.76% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   49s | Rate:   519890 rows/s | Total:  30300000 rows | Queue: 130 items | CPU Usage:  371.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   50s | Rate:   779120 rows/s | Total:  31080000 rows | Queue: 131 items | CPU Usage:  581.34% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   51s | Rate:   709824 rows/s | Total:  31790000 rows | Queue: 138 items | CPU Usage:  490.89% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   52s | Rate:   789845 rows/s | Total:  32580000 rows | Queue: 138 items | CPU Usage:  585.89% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   53s | Rate:   789876 rows/s | Total:  33370000 rows | Queue: 137 items | CPU Usage:  586.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   54s | Rate:   789840 rows/s | Total:  34160000 rows | Queue: 140 items | CPU Usage:  596.88% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   55s | Rate:   439929 rows/s | Total:  34600000 rows | Queue: 128 items | CPU Usage:  373.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   56s | Rate:   349736 rows/s | Total:  34950000 rows | Queue: 129 items | CPU Usage:  255.81% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   57s | Rate:   789765 rows/s | Total:  35740000 rows | Queue: 139 items | CPU Usage:  561.77% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   58s | Rate:   789717 rows/s | Total:  36530000 rows | Queue: 138 items | CPU Usage:  592.78% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   59s | Rate:   779606 rows/s | Total:  37310000 rows | Queue: 140 items | CPU Usage:  588.75% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   60s | Rate:   719840 rows/s | Total:  38030000 rows | Queue: 134 items | CPU Usage:  580.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   61s | Rate:   489914 rows/s | Total:  38520000 rows | Queue: 129 items | CPU Usage:  388.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   62s | Rate:   479896 rows/s | Total:  39000000 rows | Queue: 129 items | CPU Usage:  344.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   63s | Rate:   689830 rows/s | Total:  39690000 rows | Queue: 137 items | CPU Usage:  469.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   64s | Rate:   789859 rows/s | Total:  40480000 rows | Queue: 138 items | CPU Usage:  588.80% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   65s | Rate:   789594 rows/s | Total:  41270000 rows | Queue: 139 items | CPU Usage:  593.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   66s | Rate:   789634 rows/s | Total:  42060000 rows | Queue: 139 items | CPU Usage:  588.73% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   67s | Rate:   429806 rows/s | Total:  42490000 rows | Queue: 128 items | CPU Usage:  384.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   68s | Rate:   689845 rows/s | Total:  43180000 rows | Queue: 129 items | CPU Usage:  517.88% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   69s | Rate:   479827 rows/s | Total:  43660000 rows | Queue: 130 items | CPU Usage:  345.88% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   70s | Rate:   769816 rows/s | Total:  44430000 rows | Queue: 142 items | CPU Usage:  529.81% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   71s | Rate:   789615 rows/s | Total:  45220000 rows | Queue: 140 items | CPU Usage:  588.73% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   72s | Rate:   539765 rows/s | Total:  45760000 rows | Queue: 128 items | CPU Usage:  451.85% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   73s | Rate:   418385 rows/s | Total:  46180000 rows | Queue: 128 items | CPU Usage:  321.76% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   74s | Rate:   619874 rows/s | Total:  46800000 rows | Queue: 137 items | CPU Usage:  410.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   75s | Rate:   789832 rows/s | Total:  47590000 rows | Queue: 138 items | CPU Usage:  587.84% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   76s | Rate:   789732 rows/s | Total:  48380000 rows | Queue: 139 items | CPU Usage:  597.73% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   77s | Rate:   789616 rows/s | Total:  49170000 rows | Queue: 141 items | CPU Usage:  595.76% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   78s | Rate:   709829 rows/s | Total:  49880000 rows | Queue: 133 items | CPU Usage:  584.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   79s | Rate:   589900 rows/s | Total:  50470000 rows | Queue: 132 items | CPU Usage:  434.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   80s | Rate:   559897 rows/s | Total:  51030000 rows | Queue: 129 items | CPU Usage:  430.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   81s | Rate:   519761 rows/s | Total:  51550000 rows | Queue: 139 items | CPU Usage:  330.85% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   82s | Rate:   779780 rows/s | Total:  52330000 rows | Queue: 140 items | CPU Usage:  588.72% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   83s | Rate:   789554 rows/s | Total:  53120000 rows | Queue: 139 items | CPU Usage:  591.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   84s | Rate:   789584 rows/s | Total:  53910000 rows | Queue: 140 items | CPU Usage:  599.68% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   85s | Rate:   519723 rows/s | Total:  54430000 rows | Queue: 127 items | CPU Usage:  454.81% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   86s | Rate:   469879 rows/s | Total:  54900000 rows | Queue: 130 items | CPU Usage:  368.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   87s | Rate:   589837 rows/s | Total:  55490000 rows | Queue: 131 items | CPU Usage:  456.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   88s | Rate:   789725 rows/s | Total:  56280000 rows | Queue: 138 items | CPU Usage:  554.81% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   89s | Rate:   789770 rows/s | Total:  57070000 rows | Queue: 141 items | CPU Usage:  595.82% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   90s | Rate:   789761 rows/s | Total:  57860000 rows | Queue: 139 items | CPU Usage:  587.76% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   91s | Rate:   789582 rows/s | Total:  58650000 rows | Queue: 141 items | CPU Usage:  584.74% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   92s | Rate:   499877 rows/s | Total:  59150000 rows | Queue: 131 items | CPU Usage:  415.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   93s | Rate:   419855 rows/s | Total:  59570000 rows | Queue: 130 items | CPU Usage:  308.89% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   94s | Rate:   659810 rows/s | Total:  60230000 rows | Queue: 138 items | CPU Usage:  444.82% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   95s | Rate:   789542 rows/s | Total:  61020000 rows | Queue: 137 items | CPU Usage:  590.65% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   96s | Rate:   789599 rows/s | Total:  61810000 rows | Queue: 137 items | CPU Usage:  586.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   97s | Rate:   789577 rows/s | Total:  62600000 rows | Queue: 136 items | CPU Usage:  592.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   98s | Rate:   529721 rows/s | Total:  63130000 rows | Queue: 128 items | CPU Usage:  468.81% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   99s | Rate:   549886 rows/s | Total:  63680000 rows | Queue: 128 items | CPU Usage:  390.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  100s | Rate:   499881 rows/s | Total:  64180000 rows | Queue: 137 items | CPU Usage:  317.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  101s | Rate:   789769 rows/s | Total:  64970000 rows | Queue: 138 items | CPU Usage:  588.74% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  102s | Rate:   779594 rows/s | Total:  65750000 rows | Queue: 137 items | CPU Usage:  578.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  103s | Rate:   609675 rows/s | Total:  66360000 rows | Queue: 127 items | CPU Usage:  527.79% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  104s | Rate:   318485 rows/s | Total:  66680000 rows | Queue: 131 items | CPU Usage:  222.94% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  105s | Rate:   659756 rows/s | Total:  67340000 rows | Queue: 138 items | CPU Usage:  444.84% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  106s | Rate:   789782 rows/s | Total:  68130000 rows | Queue: 138 items | CPU Usage:  589.75% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  107s | Rate:   789566 rows/s | Total:  68920000 rows | Queue: 139 items | CPU Usage:  593.67% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  108s | Rate:   789562 rows/s | Total:  69710000 rows | Queue: 141 items | CPU Usage:  581.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  109s | Rate:   609722 rows/s | Total:  70320000 rows | Queue: 128 items | CPU Usage:  514.82% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  110s | Rate:   429904 rows/s | Total:  70750000 rows | Queue: 130 items | CPU Usage:  308.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  111s | Rate:   549835 rows/s | Total:  71300000 rows | Queue: 136 items | CPU Usage:  367.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  112s | Rate:   779804 rows/s | Total:  72080000 rows | Queue: 141 items | CPU Usage:  581.87% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  113s | Rate:   789866 rows/s | Total:  72870000 rows | Queue: 139 items | CPU Usage:  586.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  114s | Rate:   789868 rows/s | Total:  73660000 rows | Queue: 138 items | CPU Usage:  585.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  115s | Rate:   286649 rows/s | Total:  73950000 rows | Queue: 126 items | CPU Usage:  290.59% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  116s | Rate:   768877 rows/s | Total:  74720000 rows | Queue: 126 items | CPU Usage:  575.17% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  117s | Rate:   609864 rows/s | Total:  75330000 rows | Queue: 128 items | CPU Usage:  443.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  118s | Rate:   699754 rows/s | Total:  76030000 rows | Queue: 136 items | CPU Usage:  488.77% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  119s | Rate:   789591 rows/s | Total:  76820000 rows | Queue: 137 items | CPU Usage:  600.68% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  120s | Rate:   789587 rows/s | Total:  77610000 rows | Queue: 135 items | CPU Usage:  585.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  121s | Rate:   639717 rows/s | Total:  78250000 rows | Queue: 129 items | CPU Usage:  527.84% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  122s | Rate:   398211 rows/s | Total:  78650000 rows | Queue: 128 items | CPU Usage:  313.59% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  123s | Rate:   549882 rows/s | Total:  79200000 rows | Queue: 136 items | CPU Usage:  358.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  124s | Rate:   779795 rows/s | Total:  79980000 rows | Queue: 138 items | CPU Usage:  576.82% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  125s | Rate:   789765 rows/s | Total:  80770000 rows | Queue: 139 items | CPU Usage:  586.83% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  126s | Rate:   789751 rows/s | Total:  81560000 rows | Queue: 140 items | CPU Usage:  597.77% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  127s | Rate:   249883 rows/s | Total:  81810000 rows | Queue: 130 items | CPU Usage:  238.92% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  128s | Rate:   549847 rows/s | Total:  82360000 rows | Queue: 134 items | CPU Usage:  384.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  129s | Rate:   779777 rows/s | Total:  83140000 rows | Queue: 141 items | CPU Usage:  559.84% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  130s | Rate:   789841 rows/s | Total:  83930000 rows | Queue: 140 items | CPU Usage:  595.80% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  131s | Rate:   789561 rows/s | Total:  84720000 rows | Queue: 140 items | CPU Usage:  592.66% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  132s | Rate:   699636 rows/s | Total:  85420000 rows | Queue: 131 items | CPU Usage:  570.80% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  133s | Rate:   279953 rows/s | Total:  85700000 rows | Queue: 127 items | CPU Usage:  222.96% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  134s | Rate:   639861 rows/s | Total:  86340000 rows | Queue: 128 items | CPU Usage:  481.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  135s | Rate:   749758 rows/s | Total:  87090000 rows | Queue: 140 items | CPU Usage:  499.76% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  136s | Rate:   789574 rows/s | Total:  87880000 rows | Queue: 141 items | CPU Usage:  592.70% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  137s | Rate:   789544 rows/s | Total:  88670000 rows | Queue: 139 items | CPU Usage:  584.66% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  138s | Rate:   789610 rows/s | Total:  89460000 rows | Queue: 138 items | CPU Usage:  592.73% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  139s | Rate:   468192 rows/s | Total:  89930000 rows | Queue: 129 items | CPU Usage:  414.44% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  140s | Rate:   369096 rows/s | Total:  90300000 rows | Queue: 128 items | CPU Usage:  277.32% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  141s | Rate:   739774 rows/s | Total:  91040000 rows | Queue: 137 items | CPU Usage:  509.78% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  142s | Rate:   789616 rows/s | Total:  91830000 rows | Queue: 140 items | CPU Usage:  587.71% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  143s | Rate:   789617 rows/s | Total:  92620000 rows | Queue: 139 items | CPU Usage:  595.73% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  144s | Rate:   759648 rows/s | Total:  93380000 rows | Queue: 136 items | CPU Usage:  586.81% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  145s | Rate:   449927 rows/s | Total:  93830000 rows | Queue: 128 items | CPU Usage:  361.93% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  146s | Rate:   499868 rows/s | Total:  94330000 rows | Queue: 127 items | CPU Usage:  373.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  147s | Rate:   649791 rows/s | Total:  94980000 rows | Queue: 140 items | CPU Usage:  433.84% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  148s | Rate:   799761 rows/s | Total:  95780000 rows | Queue: 141 items | CPU Usage:  601.78% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  149s | Rate:   789591 rows/s | Total:  96570000 rows | Queue: 141 items | CPU Usage:  589.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  150s | Rate:   669688 rows/s | Total:  97240000 rows | Queue: 130 items | CPU Usage:  566.82% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  151s | Rate:   379924 rows/s | Total:  97620000 rows | Queue: 128 items | CPU Usage:  284.94% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  152s | Rate:   589872 rows/s | Total:  98210000 rows | Queue: 128 items | CPU Usage:  445.91% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  153s | Rate:   739843 rows/s | Total:  98950000 rows | Queue: 139 items | CPU Usage:  496.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  154s | Rate:   779834 rows/s | Total:  99730000 rows | Queue: 137 items | CPU Usage:  579.79% | Memory Usage:   4.87 GB | Thread Count:  91
Runtime:  155s | Rate:   259857 rows/s | Total:  99990000 rows | Queue:  87 items | CPU Usage:  307.84% | Memory Usage:   4.87 GB | Thread Count:  85
Runtime:  156s | Rate:     9995 rows/s | Total: 100000000 rows | Queue:  19 items | CPU Usage:  166.92% | Memory Usage:   4.87 GB | Thread Count:  83
```


## 8. 采样率调整为10

bypass=0
```yaml
Runtime:   10s | Rate:   491998 rows/s | Total:   4920000 rows | Queue: 137 items | CPU Usage:  391.42% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   20s | Rate:   395991 rows/s | Total:   8880000 rows | Queue: 139 items | CPU Usage:  298.79% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   30s | Rate:   236996 rows/s | Total:  11250000 rows | Queue: 138 items | CPU Usage:  178.10% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   40s | Rate:   315993 rows/s | Total:  14410000 rows | Queue: 139 items | CPU Usage:  237.90% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   50s | Rate:   235995 rows/s | Total:  16770000 rows | Queue: 140 items | CPU Usage:  178.20% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   60s | Rate:   236996 rows/s | Total:  19140000 rows | Queue: 140 items | CPU Usage:  178.80% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   70s | Rate:   157996 rows/s | Total:  20720000 rows | Queue: 137 items | CPU Usage:  119.40% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   80s | Rate:   237988 rows/s | Total:  23100000 rows | Queue: 138 items | CPU Usage:  179.09% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   90s | Rate:   155996 rows/s | Total:  24660000 rows | Queue: 139 items | CPU Usage:  118.10% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  100s | Rate:   158991 rows/s | Total:  26250000 rows | Queue: 139 items | CPU Usage:  119.39% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  110s | Rate:   156992 rows/s | Total:  27820000 rows | Queue: 139 items | CPU Usage:  118.69% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:  120s | Rate:   157991 rows/s | Total:  29400000 rows | Queue: 139 items | CPU Usage:  120.59% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  130s | Rate:   157992 rows/s | Total:  30980000 rows | Queue: 138 items | CPU Usage:  119.49% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  140s | Rate:   157991 rows/s | Total:  32560000 rows | Queue: 139 items | CPU Usage:  120.19% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  150s | Rate:   157992 rows/s | Total:  34140000 rows | Queue: 140 items | CPU Usage:  119.09% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  160s | Rate:   157992 rows/s | Total:  35720000 rows | Queue: 136 items | CPU Usage:  119.70% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  170s | Rate:    78998 rows/s | Total:  36510000 rows | Queue: 137 items | CPU Usage:   59.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  180s | Rate:   157990 rows/s | Total:  38090000 rows | Queue: 136 items | CPU Usage:  117.69% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  190s | Rate:   157994 rows/s | Total:  39670000 rows | Queue: 137 items | CPU Usage:  119.10% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  200s | Rate:    78997 rows/s | Total:  40460000 rows | Queue: 137 items | CPU Usage:   60.50% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  210s | Rate:   157992 rows/s | Total:  42040000 rows | Queue: 138 items | CPU Usage:  120.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  220s | Rate:    78998 rows/s | Total:  42830000 rows | Queue: 140 items | CPU Usage:   60.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  230s | Rate:   157996 rows/s | Total:  44410000 rows | Queue: 140 items | CPU Usage:  118.70% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  240s | Rate:    78998 rows/s | Total:  45200000 rows | Queue: 140 items | CPU Usage:   60.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  250s | Rate:   157992 rows/s | Total:  46780000 rows | Queue: 139 items | CPU Usage:  120.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  260s | Rate:    78998 rows/s | Total:  47570000 rows | Queue: 138 items | CPU Usage:   59.80% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  270s | Rate:    90995 rows/s | Total:  48480000 rows | Queue: 131 items | CPU Usage:   72.40% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  280s | Rate:   145996 rows/s | Total:  49940000 rows | Queue: 139 items | CPU Usage:  106.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  290s | Rate:    78997 rows/s | Total:  50730000 rows | Queue: 138 items | CPU Usage:   59.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  300s | Rate:    78996 rows/s | Total:  51520000 rows | Queue: 135 items | CPU Usage:   61.60% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  310s | Rate:   157995 rows/s | Total:  53100000 rows | Queue: 139 items | CPU Usage:  117.70% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  320s | Rate:    78998 rows/s | Total:  53890000 rows | Queue: 139 items | CPU Usage:   60.60% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  330s | Rate:    78995 rows/s | Total:  54680000 rows | Queue: 140 items | CPU Usage:   60.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  340s | Rate:    77995 rows/s | Total:  55460000 rows | Queue: 139 items | CPU Usage:   59.30% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  350s | Rate:   157992 rows/s | Total:  57040000 rows | Queue: 139 items | CPU Usage:  119.60% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  360s | Rate:    78998 rows/s | Total:  57830000 rows | Queue: 141 items | CPU Usage:   60.30% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  370s | Rate:    78997 rows/s | Total:  58620000 rows | Queue: 141 items | CPU Usage:   59.80% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  380s | Rate:    77997 rows/s | Total:  59400000 rows | Queue: 137 items | CPU Usage:   59.40% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  390s | Rate:    78996 rows/s | Total:  60190000 rows | Queue: 138 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  400s | Rate:   112994 rows/s | Total:  61320000 rows | Queue: 129 items | CPU Usage:   91.80% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  410s | Rate:   123997 rows/s | Total:  62560000 rows | Queue: 136 items | CPU Usage:   88.40% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  420s | Rate:    78998 rows/s | Total:  63350000 rows | Queue: 138 items | CPU Usage:   60.60% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  430s | Rate:    78998 rows/s | Total:  64140000 rows | Queue: 136 items | CPU Usage:   60.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  440s | Rate:    78998 rows/s | Total:  64930000 rows | Queue: 139 items | CPU Usage:   60.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  450s | Rate:    78995 rows/s | Total:  65720000 rows | Queue: 138 items | CPU Usage:   60.30% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  460s | Rate:    77995 rows/s | Total:  66500000 rows | Queue: 138 items | CPU Usage:   59.40% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  470s | Rate:    78995 rows/s | Total:  67290000 rows | Queue: 136 items | CPU Usage:   60.50% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  480s | Rate:    78996 rows/s | Total:  68080000 rows | Queue: 137 items | CPU Usage:   60.70% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  490s | Rate:    78995 rows/s | Total:  68870000 rows | Queue: 139 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  500s | Rate:    78996 rows/s | Total:  69660000 rows | Queue: 138 items | CPU Usage:   60.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  510s | Rate:    78995 rows/s | Total:  70450000 rows | Queue: 138 items | CPU Usage:   59.70% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  520s | Rate:    78995 rows/s | Total:  71240000 rows | Queue: 138 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  530s | Rate:    78996 rows/s | Total:  72030000 rows | Queue: 136 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  540s | Rate:    78996 rows/s | Total:  72820000 rows | Queue: 139 items | CPU Usage:   60.60% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  550s | Rate:    78995 rows/s | Total:  73610000 rows | Queue: 135 items | CPU Usage:   59.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  560s | Rate:    78997 rows/s | Total:  74400000 rows | Queue: 139 items | CPU Usage:   59.80% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  570s | Rate:    78995 rows/s | Total:  75190000 rows | Queue: 140 items | CPU Usage:   59.50% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  580s | Rate:    78996 rows/s | Total:  75980000 rows | Queue: 139 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  590s | Rate:    78996 rows/s | Total:  76770000 rows | Queue: 139 items | CPU Usage:   60.80% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  600s | Rate:    77998 rows/s | Total:  77550000 rows | Queue: 138 items | CPU Usage:   59.10% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  610s | Rate:    78998 rows/s | Total:  78340000 rows | Queue: 140 items | CPU Usage:   60.60% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  620s | Rate:    78998 rows/s | Total:  79130000 rows | Queue: 139 items | CPU Usage:   60.40% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  630s | Rate:    78998 rows/s | Total:  79920000 rows | Queue: 139 items | CPU Usage:   60.40% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  640s | Rate:    36978 rows/s | Total:  80290000 rows | Queue: 130 items | CPU Usage:   33.08% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  650s | Rate:    41999 rows/s | Total:  80710000 rows | Queue: 137 items | CPU Usage:   27.70% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  660s | Rate:    78998 rows/s | Total:  81500000 rows | Queue: 140 items | CPU Usage:   60.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  670s | Rate:    78996 rows/s | Total:  82290000 rows | Queue: 139 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  680s | Rate:    78997 rows/s | Total:  83080000 rows | Queue: 138 items | CPU Usage:   59.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  690s | Rate:    78996 rows/s | Total:  83870000 rows | Queue: 140 items | CPU Usage:   59.80% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  700s | Rate:    78998 rows/s | Total:  84660000 rows | Queue: 139 items | CPU Usage:   61.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  710s | Rate:    78998 rows/s | Total:  85450000 rows | Queue: 139 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  720s | Rate:     6997 rows/s | Total:  85520000 rows | Queue: 130 items | CPU Usage:    9.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  730s | Rate:    71998 rows/s | Total:  86240000 rows | Queue: 139 items | CPU Usage:   50.80% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  740s | Rate:    78996 rows/s | Total:  87030000 rows | Queue: 139 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  750s | Rate:    78995 rows/s | Total:  87820000 rows | Queue: 138 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  760s | Rate:    78998 rows/s | Total:  88610000 rows | Queue: 137 items | CPU Usage:   60.30% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  770s | Rate:    78998 rows/s | Total:  89400000 rows | Queue: 136 items | CPU Usage:   60.30% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  780s | Rate:        0 rows/s | Total:  89400000 rows | Queue: 136 items | CPU Usage:    1.10% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  790s | Rate:    78998 rows/s | Total:  90190000 rows | Queue: 138 items | CPU Usage:   59.50% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  800s | Rate:    78995 rows/s | Total:  90980000 rows | Queue: 139 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  810s | Rate:    78996 rows/s | Total:  91770000 rows | Queue: 136 items | CPU Usage:   60.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  820s | Rate:    78998 rows/s | Total:  92560000 rows | Queue: 139 items | CPU Usage:   60.20% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  830s | Rate:        0 rows/s | Total:  92560000 rows | Queue: 139 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  840s | Rate:    78995 rows/s | Total:  93350000 rows | Queue: 137 items | CPU Usage:   59.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  850s | Rate:    78996 rows/s | Total:  94140000 rows | Queue: 139 items | CPU Usage:   59.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  860s | Rate:    78996 rows/s | Total:  94930000 rows | Queue: 137 items | CPU Usage:   60.50% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  870s | Rate:    78998 rows/s | Total:  95720000 rows | Queue: 140 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  880s | Rate:        0 rows/s | Total:  95720000 rows | Queue: 140 items | CPU Usage:    0.90% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  890s | Rate:    78998 rows/s | Total:  96510000 rows | Queue: 141 items | CPU Usage:   60.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  900s | Rate:    78996 rows/s | Total:  97300000 rows | Queue: 139 items | CPU Usage:   60.60% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  910s | Rate:    78998 rows/s | Total:  98090000 rows | Queue: 140 items | CPU Usage:   60.60% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  920s | Rate:        0 rows/s | Total:  98090000 rows | Queue: 140 items | CPU Usage:    1.00% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  930s | Rate:    78996 rows/s | Total:  98880000 rows | Queue: 137 items | CPU Usage:   59.30% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  940s | Rate:    78996 rows/s | Total:  99670000 rows | Queue: 138 items | CPU Usage:   59.80% | Memory Usage:   4.88 GB | Thread Count:  93
Runtime:  950s | Rate:    32999 rows/s | Total: 100000000 rows | Queue:  96 items | CPU Usage:   36.60% | Memory Usage:   4.88 GB | Thread Count:  83
```

bypass=8
```yaml
Warning: Failed to set real-time priority. Requires root privileges or CAP_SYS_NICE capability.
Runtime:   10s | Rate:   593996 rows/s | Total:   5940000 rows | Queue: 131 items | CPU Usage:  445.78% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   20s | Rate:   530980 rows/s | Total:  11250000 rows | Queue: 141 items | CPU Usage:  391.19% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   30s | Rate:   632981 rows/s | Total:  17580000 rows | Queue: 139 items | CPU Usage:  474.68% | Memory Usage:   4.87 GB | Thread Count:  99
Runtime:   40s | Rate:   645686 rows/s | Total:  24040000 rows | Queue: 128 items | CPU Usage:  486.17% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   50s | Rate:   695981 rows/s | Total:  31000000 rows | Queue: 136 items | CPU Usage:  511.58% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   60s | Rate:   631968 rows/s | Total:  37320000 rows | Queue: 140 items | CPU Usage:  481.37% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   70s | Rate:   631968 rows/s | Total:  43640000 rows | Queue: 136 items | CPU Usage:  478.48% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   80s | Rate:   688988 rows/s | Total:  50530000 rows | Queue: 130 items | CPU Usage:  521.19% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:   90s | Rate:   653980 rows/s | Total:  57070000 rows | Queue: 138 items | CPU Usage:  482.38% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  100s | Rate:   631970 rows/s | Total:  63390000 rows | Queue: 138 items | CPU Usage:  469.78% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  110s | Rate:   682987 rows/s | Total:  70220000 rows | Queue: 129 items | CPU Usage:  512.19% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  120s | Rate:   659980 rows/s | Total:  76820000 rows | Queue: 137 items | CPU Usage:  485.38% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  130s | Rate:   631970 rows/s | Total:  83140000 rows | Queue: 136 items | CPU Usage:  470.09% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  140s | Rate:   660989 rows/s | Total:  89750000 rows | Queue: 128 items | CPU Usage:  500.79% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  150s | Rate:   680983 rows/s | Total:  96560000 rows | Queue: 139 items | CPU Usage:  501.38% | Memory Usage:   4.88 GB | Thread Count:  99
Runtime:  160s | Rate:   343981 rows/s | Total: 100000000 rows | Queue:   0 items | CPU Usage:  289.48% | Memory Usage:   4.88 GB | Thread Count:  83
```

## 9. 按照@肖波发现问题的配置，在物理机43上进行测试： 发现问题的配置，在物理机43上进行测试：

bypass=0
```yaml
#采样率 5s
Runtime:    5s | Rate:   939985 rows/s | Total:   4700000 rows | Queue: 104 items | CPU Usage: 1169.46% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   10s | Rate:  1836427 rows/s | Total:  14070000 rows | Queue:   0 items | CPU Usage: 2514.82% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   15s | Rate:  1861862 rows/s | Total:  23380000 rows | Queue:   0 items | CPU Usage: 2398.05% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   20s | Rate:  1111933 rows/s | Total:  28940000 rows | Queue:  94 items | CPU Usage: 1309.51% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   25s | Rate:  1833786 rows/s | Total:  38110000 rows | Queue:   0 items | CPU Usage: 2480.11% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   30s | Rate:  1847868 rows/s | Total:  47350000 rows | Queue:   0 items | CPU Usage: 2396.44% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   35s | Rate:  1267910 rows/s | Total:  53690000 rows | Queue: 142 items | CPU Usage: 1456.69% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   40s | Rate:  1655570 rows/s | Total:  61970000 rows | Queue:   2 items | CPU Usage: 2327.20% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   45s | Rate:  1845870 rows/s | Total:  71200000 rows | Queue:   0 items | CPU Usage: 2399.85% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   50s | Rate:  1907881 rows/s | Total:  80740000 rows | Queue: 106 items | CPU Usage: 2328.83% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   55s | Rate:  1073907 rows/s | Total:  86110000 rows | Queue:  13 items | CPU Usage: 1516.48% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   60s | Rate:  1827897 rows/s | Total:  95250000 rows | Queue:   0 items | CPU Usage: 2413.05% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   65s | Rate:  1845872 rows/s | Total: 104480000 rows | Queue:   1 items | CPU Usage: 2396.65% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:   70s | Rate:  1187924 rows/s | Total: 110420000 rows | Queue:  93 items | CPU Usage: 1444.51% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:   75s | Rate:  1827883 rows/s | Total: 119560000 rows | Queue:   0 items | CPU Usage: 2478.04% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:   80s | Rate:  1873861 rows/s | Total: 128930000 rows | Queue:  71 items | CPU Usage: 2389.03% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:   85s | Rate:  1629893 rows/s | Total: 137080000 rows | Queue: 126 items | CPU Usage: 2032.44% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:   90s | Rate:  1229876 rows/s | Total: 143230000 rows | Queue:   4 items | CPU Usage: 1780.64% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:   95s | Rate:  1875348 rows/s | Total: 152610000 rows | Queue:  39 items | CPU Usage: 2378.18% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:  100s | Rate:  1823879 rows/s | Total: 161730000 rows | Queue:   0 items | CPU Usage: 2426.83% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:  105s | Rate:  1303908 rows/s | Total: 168250000 rows | Queue:  37 items | CPU Usage: 1632.09% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:  110s | Rate:  1873872 rows/s | Total: 177620000 rows | Queue:  78 items | CPU Usage: 2369.84% | Memory Usage:   5.00 GB | Thread Count: 159
Runtime:  115s | Rate:  1761880 rows/s | Total: 186430000 rows | Queue:  86 items | CPU Usage: 2329.44% | Memory Usage:   5.00 GB | Thread Count: 158
Runtime:  120s | Rate:  1355872 rows/s | Total: 193210000 rows | Queue: 128 items | CPU Usage: 1706.44% | Memory Usage:   5.00 GB | Thread Count: 156
Runtime:  125s | Rate:   879938 rows/s | Total: 197610000 rows | Queue:   0 items | CPU Usage: 1267.72% | Memory Usage:   5.00 GB | Thread Count: 149
Runtime:  130s | Rate:   477966 rows/s | Total: 200000000 rows | Queue:   0 items | CPU Usage:  498.96% | Memory Usage:   5.00 GB | Thread Count: 143

=============================================== Insert Summary Statistics ====================================================
Insert Threads: 16
Total Rows: 200000000
Total Duration: 122.57 seconds
Average Rate: 1631731.89 rows/second
==============================================================================================================================

=============================================== Insert Latency & Efficiency Metrics ==========================================
Total Operations: 20000
Total Duration: 122.57 seconds
Pure Insert Latency: 97.99 seconds
Effective Time Ratio: 79.94%
Framework Overhead: 20.06%
Idle Time After Finish: 4.40 seconds
Write Latency Distribution: min: 26.7143ms, avg: 78.3882ms, p90: 87.3192ms, p95: 149.5335ms, p99: 569.5127ms, max: 1997.5681ms
==============================================================================================================================

Runtime:    1s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  336.64% | Memory Usage:   2.66 GB | Thread Count: 159
Runtime:    2s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  333.84% | Memory Usage:   3.15 GB | Thread Count: 159
Runtime:    3s | Rate:  1599248 rows/s | Total:   1600000 rows | Queue: 143 items | CPU Usage:  624.75% | Memory Usage:   3.46 GB | Thread Count: 159
Runtime:    4s | Rate:   489843 rows/s | Total:   2090000 rows | Queue: 140 items | CPU Usage: 1522.54% | Memory Usage:   3.54 GB | Thread Count: 159
Runtime:    5s | Rate:   489868 rows/s | Total:   2580000 rows | Queue: 142 items | CPU Usage: 1611.53% | Memory Usage:   3.57 GB | Thread Count: 159
Runtime:    6s | Rate:   489837 rows/s | Total:   3070000 rows | Queue: 142 items | CPU Usage: 1614.45% | Memory Usage:   3.60 GB | Thread Count: 159
Runtime:    7s | Rate:   479841 rows/s | Total:   3550000 rows | Queue: 142 items | CPU Usage: 1603.48% | Memory Usage:   3.62 GB | Thread Count: 159
Runtime:    8s | Rate:   419862 rows/s | Total:   3970000 rows | Queue: 141 items | CPU Usage: 1594.48% | Memory Usage:   3.65 GB | Thread Count: 159
Runtime:    9s | Rate:   489841 rows/s | Total:   4460000 rows | Queue: 142 items | CPU Usage: 1604.47% | Memory Usage:   3.68 GB | Thread Count: 159
Runtime:   10s | Rate:   459845 rows/s | Total:   4920000 rows | Queue: 142 items | CPU Usage: 1603.45% | Memory Usage:   3.70 GB | Thread Count: 159
Runtime:   11s | Rate:   449848 rows/s | Total:   5370000 rows | Queue: 142 items | CPU Usage: 1581.48% | Memory Usage:   3.71 GB | Thread Count: 159
Runtime:   12s | Rate:   499835 rows/s | Total:   5870000 rows | Queue: 142 items | CPU Usage: 1609.47% | Memory Usage:   3.73 GB | Thread Count: 159
Runtime:   13s | Rate:   469833 rows/s | Total:   6340000 rows | Queue: 141 items | CPU Usage: 1603.40% | Memory Usage:   3.76 GB | Thread Count: 159
Runtime:   14s | Rate:   479806 rows/s | Total:   6820000 rows | Queue: 142 items | CPU Usage: 1601.38% | Memory Usage:   3.78 GB | Thread Count: 159
Runtime:   15s | Rate:   479827 rows/s | Total:   7300000 rows | Queue: 142 items | CPU Usage: 1604.41% | Memory Usage:   3.80 GB | Thread Count: 159
Runtime:   16s | Rate:   449817 rows/s | Total:   7750000 rows | Queue: 142 items | CPU Usage: 1589.34% | Memory Usage:   3.83 GB | Thread Count: 159
Runtime:   17s | Rate:   489795 rows/s | Total:   8240000 rows | Queue: 142 items | CPU Usage: 1608.33% | Memory Usage:   3.86 GB | Thread Count: 159
Runtime:   18s | Rate:   479821 rows/s | Total:   8720000 rows | Queue: 140 items | CPU Usage: 1605.41% | Memory Usage:   3.87 GB | Thread Count: 159
Runtime:   19s | Rate:   589784 rows/s | Total:   9310000 rows | Queue: 142 items | CPU Usage: 1586.44% | Memory Usage:   3.89 GB | Thread Count: 159
Runtime:   20s | Rate:   489819 rows/s | Total:   9800000 rows | Queue: 141 items | CPU Usage: 1555.36% | Memory Usage:   3.90 GB | Thread Count: 159
Runtime:   21s | Rate:   509745 rows/s | Total:  10310000 rows | Queue: 141 items | CPU Usage: 1619.25% | Memory Usage:   3.92 GB | Thread Count: 159
Runtime:   22s | Rate:   509813 rows/s | Total:  10820000 rows | Queue: 139 items | CPU Usage: 1599.44% | Memory Usage:   3.93 GB | Thread Count: 159
Runtime:   23s | Rate:   529806 rows/s | Total:  11350000 rows | Queue: 142 items | CPU Usage: 1617.40% | Memory Usage:   3.95 GB | Thread Count: 159
Runtime:   24s | Rate:   579781 rows/s | Total:  11930000 rows | Queue: 141 items | CPU Usage: 1626.38% | Memory Usage:   3.98 GB | Thread Count: 159
Runtime:   25s | Rate:   639769 rows/s | Total:  12570000 rows | Queue: 141 items | CPU Usage: 1632.47% | Memory Usage:   3.99 GB | Thread Count: 159
Runtime:   26s | Rate:   579809 rows/s | Total:  13150000 rows | Queue: 140 items | CPU Usage: 1625.39% | Memory Usage:   4.01 GB | Thread Count: 159
Runtime:   27s | Rate:   669728 rows/s | Total:  13820000 rows | Queue: 141 items | CPU Usage: 1626.37% | Memory Usage:   4.02 GB | Thread Count: 159
Runtime:   28s | Rate:   649754 rows/s | Total:  14470000 rows | Queue: 141 items | CPU Usage: 1645.28% | Memory Usage:   4.03 GB | Thread Count: 159
Runtime:   29s | Rate:   709662 rows/s | Total:  15180000 rows | Queue: 142 items | CPU Usage: 1558.18% | Memory Usage:   4.05 GB | Thread Count: 159
Runtime:   30s | Rate:   647973 rows/s | Total:  15830000 rows | Queue: 142 items | CPU Usage: 1635.20% | Memory Usage:   4.07 GB | Thread Count: 159
Runtime:   31s | Rate:   709663 rows/s | Total:  16540000 rows | Queue: 141 items | CPU Usage: 1651.27% | Memory Usage:   4.08 GB | Thread Count: 159
Runtime:   32s | Rate:   729724 rows/s | Total:  17270000 rows | Queue: 142 items | CPU Usage: 1649.37% | Memory Usage:   4.09 GB | Thread Count: 159
Runtime:   33s | Rate:   759701 rows/s | Total:  18030000 rows | Queue: 140 items | CPU Usage: 1656.38% | Memory Usage:   4.11 GB | Thread Count: 159
Runtime:   34s | Rate:   889707 rows/s | Total:  18920000 rows | Queue: 142 items | CPU Usage: 1672.45% | Memory Usage:   4.12 GB | Thread Count: 159
Runtime:   35s | Rate:   779747 rows/s | Total:  19700000 rows | Queue: 142 items | CPU Usage: 1659.42% | Memory Usage:   4.13 GB | Thread Count: 159
Runtime:   36s | Rate:   879619 rows/s | Total:  20580000 rows | Queue: 141 items | CPU Usage: 1675.30% | Memory Usage:   4.14 GB | Thread Count: 159
Runtime:   37s | Rate:   869675 rows/s | Total:  21450000 rows | Queue: 141 items | CPU Usage: 1678.39% | Memory Usage:   4.15 GB | Thread Count: 159
Runtime:   38s | Rate:  1019640 rows/s | Total:  22470000 rows | Queue: 142 items | CPU Usage: 1704.38% | Memory Usage:   4.16 GB | Thread Count: 159
Runtime:   39s | Rate:   829723 rows/s | Total:  23300000 rows | Queue: 141 items | CPU Usage: 1667.47% | Memory Usage:   4.18 GB | Thread Count: 159
Runtime:   40s | Rate:  1129634 rows/s | Total:  24430000 rows | Queue: 140 items | CPU Usage: 1718.43% | Memory Usage:   4.19 GB | Thread Count: 159
Runtime:   41s | Rate:  1049636 rows/s | Total:  25480000 rows | Queue: 137 items | CPU Usage: 1690.39% | Memory Usage:   4.22 GB | Thread Count: 159
Runtime:   42s | Rate:   249907 rows/s | Total:  25730000 rows | Queue: 139 items | CPU Usage:  430.85% | Memory Usage:   4.22 GB | Thread Count: 159
Runtime:   43s | Rate:  1249574 rows/s | Total:  26980000 rows | Queue: 139 items | CPU Usage: 1513.42% | Memory Usage:   4.24 GB | Thread Count: 159
Runtime:   44s | Rate:  1189440 rows/s | Total:  28170000 rows | Queue: 141 items | CPU Usage: 1729.27% | Memory Usage:   4.28 GB | Thread Count: 159
Runtime:   45s | Rate:  1489457 rows/s | Total:  29660000 rows | Queue: 140 items | CPU Usage: 1785.31% | Memory Usage:   4.30 GB | Thread Count: 159
Runtime:   46s | Rate:  1629385 rows/s | Total:  31290000 rows | Queue: 139 items | CPU Usage: 1815.32% | Memory Usage:   4.32 GB | Thread Count: 159
Runtime:   47s | Rate:  1689374 rows/s | Total:  32980000 rows | Queue: 139 items | CPU Usage: 1828.32% | Memory Usage:   4.34 GB | Thread Count: 159
Runtime:   48s | Rate:  1719403 rows/s | Total:  34700000 rows | Queue: 139 items | CPU Usage: 1828.41% | Memory Usage:   4.35 GB | Thread Count: 159
Runtime:   49s | Rate:  1849462 rows/s | Total:  36550000 rows | Queue: 141 items | CPU Usage: 1838.49% | Memory Usage:   4.37 GB | Thread Count: 159
Runtime:   50s | Rate:  2039232 rows/s | Total:  38590000 rows | Queue: 137 items | CPU Usage: 1893.26% | Memory Usage:   4.39 GB | Thread Count: 159
Runtime:   51s | Rate:  2019367 rows/s | Total:  40610000 rows | Queue: 138 items | CPU Usage: 1889.41% | Memory Usage:   4.40 GB | Thread Count: 159
Runtime:   52s | Rate:  2179212 rows/s | Total:  42790000 rows | Queue: 139 items | CPU Usage: 1876.30% | Memory Usage:   4.41 GB | Thread Count: 159
Runtime:   53s | Rate:  2259169 rows/s | Total:  45050000 rows | Queue: 140 items | CPU Usage: 1921.34% | Memory Usage:   4.43 GB | Thread Count: 159
Runtime:   54s | Rate:  2279383 rows/s | Total:  47330000 rows | Queue: 140 items | CPU Usage: 1919.41% | Memory Usage:   4.44 GB | Thread Count: 159
Runtime:   55s | Rate:  2369199 rows/s | Total:  49700000 rows | Queue: 139 items | CPU Usage: 1941.37% | Memory Usage:   4.46 GB | Thread Count: 159
Runtime:   56s | Rate:  2359219 rows/s | Total:  52060000 rows | Queue: 140 items | CPU Usage: 1949.31% | Memory Usage:   4.47 GB | Thread Count: 159
Runtime:   57s | Rate:  1269504 rows/s | Total:  53330000 rows | Queue: 142 items | CPU Usage: 1060.61% | Memory Usage:   4.47 GB | Thread Count: 159
Runtime:   58s | Rate:   259907 rows/s | Total:  53590000 rows | Queue: 142 items | CPU Usage:  214.92% | Memory Usage:   4.47 GB | Thread Count: 159
Runtime:   59s | Rate:   159936 rows/s | Total:  53750000 rows | Queue: 142 items | CPU Usage:  165.93% | Memory Usage:   4.47 GB | Thread Count: 159
Runtime:   60s | Rate:  1219564 rows/s | Total:  54970000 rows | Queue: 140 items | CPU Usage: 1002.64% | Memory Usage:   4.47 GB | Thread Count: 159
Runtime:   61s | Rate:  2329226 rows/s | Total:  57300000 rows | Queue: 139 items | CPU Usage: 1854.41% | Memory Usage:   4.48 GB | Thread Count: 159
Runtime:   62s | Rate:  2519279 rows/s | Total:  59820000 rows | Queue: 140 items | CPU Usage: 1970.48% | Memory Usage:   4.48 GB | Thread Count: 159
Runtime:   63s | Rate:  2399346 rows/s | Total:  62220000 rows | Queue: 135 items | CPU Usage: 1871.40% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   64s | Rate:  2619081 rows/s | Total:  64840000 rows | Queue: 138 items | CPU Usage: 1972.38% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   65s | Rate:  2589232 rows/s | Total:  67430000 rows | Queue: 140 items | CPU Usage: 1984.45% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   66s | Rate:  2659313 rows/s | Total:  70090000 rows | Queue: 137 items | CPU Usage: 1976.41% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   67s | Rate:  2728950 rows/s | Total:  72820000 rows | Queue: 136 items | CPU Usage: 1954.30% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   68s | Rate:  2709217 rows/s | Total:  75530000 rows | Queue: 134 items | CPU Usage: 1968.41% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   69s | Rate:  2799140 rows/s | Total:  78330000 rows | Queue: 137 items | CPU Usage: 2013.37% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   70s | Rate:  2399195 rows/s | Total:  80730000 rows | Queue: 134 items | CPU Usage: 1714.44% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   71s | Rate:   549821 rows/s | Total:  81280000 rows | Queue: 142 items | CPU Usage:  417.86% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   72s | Rate:   389856 rows/s | Total:  81670000 rows | Queue: 135 items | CPU Usage:  283.88% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   73s | Rate:  1429323 rows/s | Total:  83100000 rows | Queue: 138 items | CPU Usage: 1006.51% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   74s | Rate:  1819317 rows/s | Total:  84920000 rows | Queue: 140 items | CPU Usage: 1258.56% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   75s | Rate:  2748924 rows/s | Total:  87670000 rows | Queue: 136 items | CPU Usage: 1931.30% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   76s | Rate:  2788883 rows/s | Total:  90460000 rows | Queue: 128 items | CPU Usage: 1977.26% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   77s | Rate:  2939157 rows/s | Total:  93400000 rows | Queue: 140 items | CPU Usage: 2002.39% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   78s | Rate:  2859024 rows/s | Total:  96260000 rows | Queue: 138 items | CPU Usage: 2004.34% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   79s | Rate:  2889066 rows/s | Total:  99150000 rows | Queue: 138 items | CPU Usage: 2009.39% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   80s | Rate:  2889200 rows/s | Total: 102040000 rows | Queue: 139 items | CPU Usage: 2017.39% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   81s | Rate:  2849055 rows/s | Total: 104890000 rows | Queue: 137 items | CPU Usage: 1988.34% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   82s | Rate:  2879055 rows/s | Total: 107770000 rows | Queue: 137 items | CPU Usage: 1993.33% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   83s | Rate:  1119571 rows/s | Total: 108890000 rows | Queue: 142 items | CPU Usage:  795.71% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   84s | Rate:   499807 rows/s | Total: 109390000 rows | Queue: 142 items | CPU Usage:  356.86% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   85s | Rate:   339882 rows/s | Total: 109730000 rows | Queue: 142 items | CPU Usage:  206.93% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   86s | Rate:   989688 rows/s | Total: 110720000 rows | Queue: 142 items | CPU Usage:  706.78% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   87s | Rate:  2789131 rows/s | Total: 113510000 rows | Queue: 138 items | CPU Usage: 1939.41% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   88s | Rate:  2858960 rows/s | Total: 116370000 rows | Queue: 137 items | CPU Usage: 1990.26% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   89s | Rate:  2709215 rows/s | Total: 119080000 rows | Queue: 134 items | CPU Usage: 1870.44% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   90s | Rate:  2739032 rows/s | Total: 121820000 rows | Queue: 136 items | CPU Usage: 1896.22% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   91s | Rate:  2878742 rows/s | Total: 124700000 rows | Queue: 139 items | CPU Usage: 1976.24% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   92s | Rate:  2809030 rows/s | Total: 127510000 rows | Queue: 134 items | CPU Usage: 1954.32% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   93s | Rate:  2888972 rows/s | Total: 130400000 rows | Queue: 136 items | CPU Usage: 2009.29% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   94s | Rate:  2839048 rows/s | Total: 133240000 rows | Queue: 138 items | CPU Usage: 1977.33% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   95s | Rate:  2688912 rows/s | Total: 135930000 rows | Queue: 142 items | CPU Usage: 1881.30% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   96s | Rate:  1139626 rows/s | Total: 137070000 rows | Queue: 142 items | CPU Usage:  811.71% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   97s | Rate:  1639419 rows/s | Total: 138710000 rows | Queue: 137 items | CPU Usage: 1142.63% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   98s | Rate:  2839223 rows/s | Total: 141550000 rows | Queue: 138 items | CPU Usage: 1954.48% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   99s | Rate:  2799266 rows/s | Total: 144350000 rows | Queue: 135 items | CPU Usage: 1937.47% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  100s | Rate:  2789212 rows/s | Total: 147140000 rows | Queue: 135 items | CPU Usage: 1932.37% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  101s | Rate:  2898849 rows/s | Total: 150040000 rows | Queue: 138 items | CPU Usage: 1999.24% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  102s | Rate:  2889049 rows/s | Total: 152930000 rows | Queue: 138 items | CPU Usage: 1989.36% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  103s | Rate:  2889040 rows/s | Total: 155820000 rows | Queue: 140 items | CPU Usage: 1994.34% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  104s | Rate:  2549170 rows/s | Total: 158370000 rows | Queue: 142 items | CPU Usage: 1779.43% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  105s | Rate:  2619148 rows/s | Total: 160990000 rows | Queue: 139 items | CPU Usage: 1801.40% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  106s | Rate:  2579010 rows/s | Total: 163570000 rows | Queue: 142 items | CPU Usage: 1798.31% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  107s | Rate:  1059587 rows/s | Total: 164630000 rows | Queue: 142 items | CPU Usage:  753.71% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  108s | Rate:   559781 rows/s | Total: 165190000 rows | Queue: 142 items | CPU Usage:  397.84% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  109s | Rate:   399860 rows/s | Total: 165590000 rows | Queue: 133 items | CPU Usage:  292.90% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  110s | Rate:  1029673 rows/s | Total: 166620000 rows | Queue: 142 items | CPU Usage:  738.71% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  111s | Rate:  2219014 rows/s | Total: 168840000 rows | Queue: 134 items | CPU Usage: 1560.43% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  112s | Rate:  2839192 rows/s | Total: 171680000 rows | Queue: 139 items | CPU Usage: 1999.46% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  113s | Rate:  2869244 rows/s | Total: 174550000 rows | Queue: 139 items | CPU Usage: 1998.47% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  114s | Rate:  2838769 rows/s | Total: 177390000 rows | Queue: 138 items | CPU Usage: 1990.05% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  115s | Rate:  2839083 rows/s | Total: 180230000 rows | Queue: 135 items | CPU Usage: 2001.32% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  116s | Rate:  2218978 rows/s | Total: 182450000 rows | Queue: 140 items | CPU Usage: 1522.36% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  117s | Rate:   899682 rows/s | Total: 183350000 rows | Queue: 142 items | CPU Usage:  673.76% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  118s | Rate:  1749378 rows/s | Total: 185100000 rows | Queue: 142 items | CPU Usage: 1266.55% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  119s | Rate:  1069676 rows/s | Total: 186170000 rows | Queue: 135 items | CPU Usage:  758.74% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  120s | Rate:  2609080 rows/s | Total: 188780000 rows | Queue: 139 items | CPU Usage: 1848.42% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  121s | Rate:  2039362 rows/s | Total: 190820000 rows | Queue: 142 items | CPU Usage: 1444.54% | Memory Usage:   4.53 GB | Thread Count: 158
Runtime:  122s | Rate:  1369496 rows/s | Total: 192190000 rows | Queue: 142 items | CPU Usage:  980.64% | Memory Usage:   4.53 GB | Thread Count: 157
Runtime:  123s | Rate:   969633 rows/s | Total: 193160000 rows | Queue: 142 items | CPU Usage:  704.73% | Memory Usage:   4.53 GB | Thread Count: 155
Runtime:  124s | Rate:   539813 rows/s | Total: 193700000 rows | Queue: 142 items | CPU Usage:  385.86% | Memory Usage:   4.53 GB | Thread Count: 155
Runtime:  125s | Rate:  1179556 rows/s | Total: 194880000 rows | Queue: 136 items | CPU Usage:  860.70% | Memory Usage:   4.53 GB | Thread Count: 155
Runtime:  126s | Rate:  1899233 rows/s | Total: 196780000 rows | Queue: 136 items | CPU Usage: 1372.48% | Memory Usage:   4.53 GB | Thread Count: 151
Runtime:  127s | Rate:  1709485 rows/s | Total: 198490000 rows | Queue: 132 items | CPU Usage: 1331.54% | Memory Usage:   4.53 GB | Thread Count: 149
Runtime:  128s | Rate:  1509378 rows/s | Total: 200000000 rows | Queue:  34 items | CPU Usage: 1631.41% | Memory Usage:   4.53 GB | Thread Count: 143

=============================================== Insert Summary Statistics ====================================================
Insert Threads: 16
Total Rows: 200000000
Total Duration: 125.85 seconds
Average Rate: 1589193.96 rows/second
==============================================================================================================================

=============================================== Insert Latency & Efficiency Metrics ==========================================
Total Operations: 20000
Total Duration: 125.85 seconds
Pure Insert Latency: 125.76 seconds
Effective Time Ratio: 99.93%
Framework Overhead: 0.06%
Idle Time After Finish: 0.02 seconds
Write Latency Distribution: min: 28.2153ms, avg: 100.6085ms, p90: 256.3119ms, p95: 334.6710ms, p99: 536.2285ms, max: 1130.2638ms
==============================================================================================================================
```

bypass=2，没有波动的现象
```yaml
#采样率 5s
Runtime:    5s | Rate:  1003984 rows/s | Total:   5020000 rows | Queue:  14 items | CPU Usage: 1218.51% | Memory Usage:   4.97 GB | Thread Count: 159
Runtime:   10s | Rate:  1995850 rows/s | Total:  15000000 rows | Queue:   0 items | CPU Usage: 2440.24% | Memory Usage:   4.97 GB | Thread Count: 159
Runtime:   15s | Rate:  1995861 rows/s | Total:  24980000 rows | Queue:   0 items | CPU Usage: 2434.22% | Memory Usage:   4.97 GB | Thread Count: 159
Runtime:   20s | Rate:  1991850 rows/s | Total:  34940000 rows | Queue:   0 items | CPU Usage: 2431.83% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   25s | Rate:  1993868 rows/s | Total:  44910000 rows | Queue:   0 items | CPU Usage: 2433.25% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   30s | Rate:  1995892 rows/s | Total:  54890000 rows | Queue:   0 items | CPU Usage: 2432.06% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   35s | Rate:  1991869 rows/s | Total:  64850000 rows | Queue:   0 items | CPU Usage: 2433.84% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   40s | Rate:  1993865 rows/s | Total:  74820000 rows | Queue:   0 items | CPU Usage: 2434.99% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   45s | Rate:  1981813 rows/s | Total:  84730000 rows | Queue:   0 items | CPU Usage: 2431.81% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   50s | Rate:  1987858 rows/s | Total:  94670000 rows | Queue:   0 items | CPU Usage: 2431.83% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   55s | Rate:  2001884 rows/s | Total: 104680000 rows | Queue:   0 items | CPU Usage: 2434.61% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   60s | Rate:  1969803 rows/s | Total: 114530000 rows | Queue:   0 items | CPU Usage: 2431.18% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   65s | Rate:  1989823 rows/s | Total: 124480000 rows | Queue:   0 items | CPU Usage: 2448.00% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   70s | Rate:  1993859 rows/s | Total: 134450000 rows | Queue:   0 items | CPU Usage: 2431.04% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   75s | Rate:  1975875 rows/s | Total: 144330000 rows | Queue:   0 items | CPU Usage: 2431.63% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   80s | Rate:  1985851 rows/s | Total: 154260000 rows | Queue:   0 items | CPU Usage: 2433.84% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   85s | Rate:  1997868 rows/s | Total: 164250000 rows | Queue:   0 items | CPU Usage: 2434.45% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   90s | Rate:  1969894 rows/s | Total: 174100000 rows | Queue:   0 items | CPU Usage: 2406.46% | Memory Usage:   4.99 GB | Thread Count: 158
Runtime:   95s | Rate:  1645897 rows/s | Total: 182330000 rows | Queue:   0 items | CPU Usage: 2053.86% | Memory Usage:   4.99 GB | Thread Count: 155
Runtime:  100s | Rate:  1457887 rows/s | Total: 189620000 rows | Queue:   0 items | CPU Usage: 1841.45% | Memory Usage:   4.99 GB | Thread Count: 155
Runtime:  105s | Rate:  1363878 rows/s | Total: 196440000 rows | Queue:   0 items | CPU Usage: 1697.25% | Memory Usage:   4.99 GB | Thread Count: 150
Runtime:  110s | Rate:   711937 rows/s | Total: 200000000 rows | Queue:   0 items | CPU Usage:  713.74% | Memory Usage:   4.99 GB | Thread Count: 143
=============================================== Insert Summary Statistics ====================================================
Insert Threads: 16
Total Rows: 200000000
Total Duration: 101.40 seconds
Average Rate: 1972336.14 rows/second
==============================================================================================================================

=============================================== Insert Latency & Efficiency Metrics ==========================================
Total Operations: 20000
Total Duration: 101.40 seconds
Pure Insert Latency: 61.35 seconds
Effective Time Ratio: 60.50%
Framework Overhead: 39.50%
Idle Time After Finish: 6.55 seconds
Write Latency Distribution: min: 21.5974ms, avg: 49.0774ms, p90: 64.7564ms, p95: 75.9240ms, p99: 118.3166ms, max: 670.2253ms
==============================================================================================================================
#采样率 1s
Runtime:    1s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  336.42% | Memory Usage:   2.63 GB | Thread Count: 159
Runtime:    2s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  329.85% | Memory Usage:   3.11 GB | Thread Count: 159
Runtime:    3s | Rate:  1589313 rows/s | Total:   1590000 rows | Queue: 143 items | CPU Usage:  582.77% | Memory Usage:   3.42 GB | Thread Count: 159
Runtime:    4s | Rate:   479825 rows/s | Total:   2070000 rows | Queue: 142 items | CPU Usage: 1425.46% | Memory Usage:   3.54 GB | Thread Count: 159
Runtime:    5s | Rate:   489813 rows/s | Total:   2560000 rows | Queue: 142 items | CPU Usage: 1632.38% | Memory Usage:   3.57 GB | Thread Count: 159
Runtime:    6s | Rate:   479817 rows/s | Total:   3040000 rows | Queue: 142 items | CPU Usage: 1612.39% | Memory Usage:   3.60 GB | Thread Count: 159
Runtime:    7s | Rate:   489798 rows/s | Total:   3530000 rows | Queue: 142 items | CPU Usage: 1534.35% | Memory Usage:   3.62 GB | Thread Count: 159
Runtime:    8s | Rate:   479794 rows/s | Total:   4010000 rows | Queue: 142 items | CPU Usage: 1586.31% | Memory Usage:   3.65 GB | Thread Count: 159
Runtime:    9s | Rate:   539789 rows/s | Total:   4550000 rows | Queue: 142 items | CPU Usage: 1457.51% | Memory Usage:   3.68 GB | Thread Count: 159
Runtime:   10s | Rate:   529843 rows/s | Total:   5080000 rows | Queue: 142 items | CPU Usage: 1525.49% | Memory Usage:   3.70 GB | Thread Count: 159
Runtime:   11s | Rate:   469814 rows/s | Total:   5550000 rows | Queue: 142 items | CPU Usage: 1557.37% | Memory Usage:   3.73 GB | Thread Count: 159
Runtime:   12s | Rate:   489768 rows/s | Total:   6040000 rows | Queue: 142 items | CPU Usage: 1553.27% | Memory Usage:   3.75 GB | Thread Count: 159
Runtime:   13s | Rate:   499794 rows/s | Total:   6540000 rows | Queue: 142 items | CPU Usage: 1628.32% | Memory Usage:   3.77 GB | Thread Count: 159
Runtime:   14s | Rate:   409821 rows/s | Total:   6950000 rows | Queue: 142 items | CPU Usage: 1552.35% | Memory Usage:   3.79 GB | Thread Count: 159
Runtime:   15s | Rate:   549774 rows/s | Total:   7500000 rows | Queue: 142 items | CPU Usage: 1483.37% | Memory Usage:   3.80 GB | Thread Count: 159
Runtime:   16s | Rate:   399824 rows/s | Total:   7900000 rows | Queue: 141 items | CPU Usage: 1598.25% | Memory Usage:   3.83 GB | Thread Count: 159
Runtime:   17s | Rate:   569758 rows/s | Total:   8470000 rows | Queue: 141 items | CPU Usage: 1618.44% | Memory Usage:   3.85 GB | Thread Count: 159
Runtime:   18s | Rate:   449869 rows/s | Total:   8920000 rows | Queue: 139 items | CPU Usage: 1459.53% | Memory Usage:   3.88 GB | Thread Count: 159
Runtime:   19s | Rate:   559776 rows/s | Total:   9480000 rows | Queue: 142 items | CPU Usage: 1549.36% | Memory Usage:   3.89 GB | Thread Count: 159
Runtime:   20s | Rate:   449812 rows/s | Total:   9930000 rows | Queue: 142 items | CPU Usage: 1604.32% | Memory Usage:   3.91 GB | Thread Count: 159
Runtime:   21s | Rate:   509775 rows/s | Total:  10440000 rows | Queue: 142 items | CPU Usage: 1542.26% | Memory Usage:   3.92 GB | Thread Count: 159
Runtime:   22s | Rate:   469788 rows/s | Total:  10910000 rows | Queue: 141 items | CPU Usage: 1634.35% | Memory Usage:   3.94 GB | Thread Count: 159
Runtime:   23s | Rate:   529776 rows/s | Total:  11440000 rows | Queue: 141 items | CPU Usage: 1622.34% | Memory Usage:   3.96 GB | Thread Count: 159
Runtime:   24s | Rate:   499808 rows/s | Total:  11940000 rows | Queue: 140 items | CPU Usage: 1601.37% | Memory Usage:   3.98 GB | Thread Count: 159
Runtime:   25s | Rate:   539774 rows/s | Total:  12480000 rows | Queue: 142 items | CPU Usage: 1530.36% | Memory Usage:   3.99 GB | Thread Count: 159
Runtime:   26s | Rate:   529787 rows/s | Total:  13010000 rows | Queue: 139 items | CPU Usage: 1525.38% | Memory Usage:   4.00 GB | Thread Count: 159
Runtime:   27s | Rate:   629755 rows/s | Total:  13640000 rows | Queue: 140 items | CPU Usage: 1669.32% | Memory Usage:   4.01 GB | Thread Count: 159
Runtime:   28s | Rate:   499749 rows/s | Total:  14140000 rows | Queue: 142 items | CPU Usage: 1515.29% | Memory Usage:   4.03 GB | Thread Count: 159
Runtime:   29s | Rate:   659733 rows/s | Total:  14800000 rows | Queue: 142 items | CPU Usage: 1611.32% | Memory Usage:   4.04 GB | Thread Count: 159
Runtime:   30s | Rate:   709698 rows/s | Total:  15510000 rows | Queue: 141 items | CPU Usage: 1644.33% | Memory Usage:   4.07 GB | Thread Count: 159
Runtime:   31s | Rate:   729717 rows/s | Total:  16240000 rows | Queue: 142 items | CPU Usage: 1652.33% | Memory Usage:   4.07 GB | Thread Count: 159
Runtime:   32s | Rate:   709714 rows/s | Total:  16950000 rows | Queue: 142 items | CPU Usage: 1615.40% | Memory Usage:   4.09 GB | Thread Count: 159
Runtime:   33s | Rate:   709744 rows/s | Total:  17660000 rows | Queue: 142 items | CPU Usage: 1557.38% | Memory Usage:   4.10 GB | Thread Count: 159
Runtime:   34s | Rate:   969572 rows/s | Total:  18630000 rows | Queue: 142 items | CPU Usage: 1669.27% | Memory Usage:   4.12 GB | Thread Count: 159
Runtime:   35s | Rate:  1029556 rows/s | Total:  19660000 rows | Queue: 140 items | CPU Usage: 1669.29% | Memory Usage:   4.14 GB | Thread Count: 159
Runtime:   36s | Rate:  1029587 rows/s | Total:  20690000 rows | Queue: 142 items | CPU Usage: 1544.41% | Memory Usage:   4.15 GB | Thread Count: 159
Runtime:   37s | Rate:  1079639 rows/s | Total:  21770000 rows | Queue: 140 items | CPU Usage: 1586.51% | Memory Usage:   4.16 GB | Thread Count: 159
Runtime:   38s | Rate:  1339564 rows/s | Total:  23110000 rows | Queue: 141 items | CPU Usage: 1777.36% | Memory Usage:   4.21 GB | Thread Count: 159
Runtime:   39s | Rate:  1149555 rows/s | Total:  24260000 rows | Queue: 132 items | CPU Usage: 1657.27% | Memory Usage:   4.23 GB | Thread Count: 159
Runtime:   40s | Rate:  1719224 rows/s | Total:  25980000 rows | Queue: 139 items | CPU Usage: 1722.29% | Memory Usage:   4.25 GB | Thread Count: 159
Runtime:   41s | Rate:  1849308 rows/s | Total:  27830000 rows | Queue: 140 items | CPU Usage: 1806.34% | Memory Usage:   4.28 GB | Thread Count: 159
Runtime:   42s | Rate:  1839211 rows/s | Total:  29670000 rows | Queue: 142 items | CPU Usage: 1665.30% | Memory Usage:   4.30 GB | Thread Count: 159
Runtime:   43s | Rate:  1959259 rows/s | Total:  31630000 rows | Queue: 142 items | CPU Usage: 1796.34% | Memory Usage:   4.32 GB | Thread Count: 159
Runtime:   44s | Rate:  2169167 rows/s | Total:  33800000 rows | Queue: 138 items | CPU Usage: 1844.30% | Memory Usage:   4.34 GB | Thread Count: 159
Runtime:   45s | Rate:  2389112 rows/s | Total:  36190000 rows | Queue: 139 items | CPU Usage: 2020.25% | Memory Usage:   4.35 GB | Thread Count: 159
Runtime:   46s | Rate:  2319204 rows/s | Total:  38510000 rows | Queue: 135 items | CPU Usage: 1951.28% | Memory Usage:   4.37 GB | Thread Count: 159
Runtime:   47s | Rate:  2728975 rows/s | Total:  41240000 rows | Queue: 136 items | CPU Usage: 2077.22% | Memory Usage:   4.40 GB | Thread Count: 159
Runtime:   48s | Rate:  2609020 rows/s | Total:  43850000 rows | Queue: 137 items | CPU Usage: 1975.29% | Memory Usage:   4.42 GB | Thread Count: 159
Runtime:   49s | Rate:  2788985 rows/s | Total:  46640000 rows | Queue: 139 items | CPU Usage: 2068.25% | Memory Usage:   4.45 GB | Thread Count: 159
Runtime:   50s | Rate:  2749053 rows/s | Total:  49390000 rows | Queue: 140 items | CPU Usage: 2045.32% | Memory Usage:   4.46 GB | Thread Count: 159
Runtime:   51s | Rate:  2759079 rows/s | Total:  52150000 rows | Queue: 138 items | CPU Usage: 2039.29% | Memory Usage:   4.47 GB | Thread Count: 159
Runtime:   52s | Rate:  3048879 rows/s | Total:  55200000 rows | Queue: 138 items | CPU Usage: 2160.18% | Memory Usage:   4.47 GB | Thread Count: 159
Runtime:   53s | Rate:  2778978 rows/s | Total:  57980000 rows | Queue: 140 items | CPU Usage: 1959.33% | Memory Usage:   4.48 GB | Thread Count: 159
Runtime:   54s | Rate:  2999005 rows/s | Total:  60980000 rows | Queue: 134 items | CPU Usage: 2097.23% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   55s | Rate:  3008865 rows/s | Total:  63990000 rows | Queue: 139 items | CPU Usage: 2085.25% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   56s | Rate:  3188849 rows/s | Total:  67180000 rows | Queue: 137 items | CPU Usage: 2155.19% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   57s | Rate:  3178792 rows/s | Total:  70360000 rows | Queue: 136 items | CPU Usage: 2127.21% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   58s | Rate:  3168838 rows/s | Total:  73530000 rows | Queue: 141 items | CPU Usage: 2119.20% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   59s | Rate:  2928899 rows/s | Total:  76460000 rows | Queue: 140 items | CPU Usage: 2000.29% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   60s | Rate:  3128914 rows/s | Total:  79590000 rows | Queue: 138 items | CPU Usage: 2115.21% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   61s | Rate:  3208761 rows/s | Total:  82800000 rows | Queue: 137 items | CPU Usage: 2160.21% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   62s | Rate:  3138837 rows/s | Total:  85940000 rows | Queue: 134 items | CPU Usage: 2106.16% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   63s | Rate:  3148697 rows/s | Total:  89090000 rows | Queue: 138 items | CPU Usage: 2097.22% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   64s | Rate:  3268866 rows/s | Total:  92360000 rows | Queue: 138 items | CPU Usage: 2183.21% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   65s | Rate:  3278812 rows/s | Total:  95640000 rows | Queue: 137 items | CPU Usage: 2153.23% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   66s | Rate:  3298813 rows/s | Total:  98940000 rows | Queue: 137 items | CPU Usage: 2159.21% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   67s | Rate:  3278794 rows/s | Total: 102220000 rows | Queue: 136 items | CPU Usage: 2162.23% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   68s | Rate:  3008933 rows/s | Total: 105230000 rows | Queue: 129 items | CPU Usage: 2024.18% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   69s | Rate:  3388561 rows/s | Total: 108620000 rows | Queue: 137 items | CPU Usage: 2193.15% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   70s | Rate:  3128519 rows/s | Total: 111750000 rows | Queue: 139 items | CPU Usage: 2051.06% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   71s | Rate:  3408845 rows/s | Total: 115160000 rows | Queue: 137 items | CPU Usage: 2227.23% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   72s | Rate:  3158852 rows/s | Total: 118320000 rows | Queue: 134 items | CPU Usage: 2083.14% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   73s | Rate:  3378357 rows/s | Total: 121700000 rows | Queue: 142 items | CPU Usage: 2192.08% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   74s | Rate:  3099038 rows/s | Total: 124800000 rows | Queue: 134 items | CPU Usage: 2065.36% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   75s | Rate:  3318915 rows/s | Total: 128120000 rows | Queue: 135 items | CPU Usage: 2177.25% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   76s | Rate:  3258808 rows/s | Total: 131380000 rows | Queue: 137 items | CPU Usage: 2129.07% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   77s | Rate:  3248244 rows/s | Total: 134630000 rows | Queue: 142 items | CPU Usage: 2126.06% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   78s | Rate:  3278774 rows/s | Total: 137910000 rows | Queue: 140 items | CPU Usage: 2141.05% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   79s | Rate:  3098562 rows/s | Total: 141010000 rows | Queue: 140 items | CPU Usage: 2054.16% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   80s | Rate:  3308827 rows/s | Total: 144320000 rows | Queue: 136 items | CPU Usage: 2176.19% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   81s | Rate:  3378671 rows/s | Total: 147700000 rows | Queue: 138 items | CPU Usage: 2172.18% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   82s | Rate:  3388744 rows/s | Total: 151090000 rows | Queue: 138 items | CPU Usage: 2205.14% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   83s | Rate:  3288670 rows/s | Total: 154380000 rows | Queue: 135 items | CPU Usage: 2135.01% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   84s | Rate:  3408438 rows/s | Total: 157790000 rows | Queue: 135 items | CPU Usage: 2209.14% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   85s | Rate:  3348727 rows/s | Total: 161140000 rows | Queue: 137 items | CPU Usage: 2161.16% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   86s | Rate:  3238793 rows/s | Total: 164380000 rows | Queue: 138 items | CPU Usage: 2095.18% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   87s | Rate:  3398379 rows/s | Total: 167780000 rows | Queue: 138 items | CPU Usage: 2203.98% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   88s | Rate:  3358700 rows/s | Total: 171140000 rows | Queue: 142 items | CPU Usage: 2168.23% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   89s | Rate:  3078936 rows/s | Total: 174220000 rows | Queue: 134 items | CPU Usage: 2036.16% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   90s | Rate:  3298313 rows/s | Total: 177520000 rows | Queue: 137 items | CPU Usage: 2129.07% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   91s | Rate:  3308951 rows/s | Total: 180830000 rows | Queue: 139 items | CPU Usage: 2129.23% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   92s | Rate:  3358599 rows/s | Total: 184190000 rows | Queue: 137 items | CPU Usage: 2184.10% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   93s | Rate:  3278571 rows/s | Total: 187470000 rows | Queue: 135 items | CPU Usage: 2122.04% | Memory Usage:   4.53 GB | Thread Count: 158
Runtime:   94s | Rate:  3318551 rows/s | Total: 190790000 rows | Queue: 139 items | CPU Usage: 2193.98% | Memory Usage:   4.53 GB | Thread Count: 156
Runtime:   95s | Rate:  3338187 rows/s | Total: 194130000 rows | Queue: 136 items | CPU Usage: 2222.87% | Memory Usage:   4.53 GB | Thread Count: 156
Runtime:   96s | Rate:  3248514 rows/s | Total: 197380000 rows | Queue: 134 items | CPU Usage: 2192.81% | Memory Usage:   4.53 GB | Thread Count: 149
Runtime:   97s | Rate:  2478483 rows/s | Total: 199860000 rows | Queue:  37 items | CPU Usage: 2075.78% | Memory Usage:   4.53 GB | Thread Count: 145
Runtime:   98s | Rate:   139923 rows/s | Total: 200000000 rows | Queue:   0 items | CPU Usage:  278.88% | Memory Usage:   4.53 GB | Thread Count: 143
=============================================== Insert Summary Statistics ====================================================
Insert Threads: 16
Total Rows: 200000000
Total Duration: 94.89 seconds
Average Rate: 2107745.84 rows/second
==============================================================================================================================

=============================================== Insert Latency & Efficiency Metrics ==========================================
Total Operations: 20000
Total Duration: 94.89 seconds
Pure Insert Latency: 94.73 seconds
Effective Time Ratio: 99.83%
Framework Overhead: 0.10%
Idle Time After Finish: 0.06 seconds
Write Latency Distribution: min: 27.5106ms, avg: 75.7815ms, p90: 134.5495ms, p95: 268.9521ms, p99: 453.0863ms, max: 840.7743ms
==============================================================================================================================
```

bypass=4，有波动，但是没有降为0的长时间阻塞
```yaml

## 10. 采样率 5s

Runtime:    5s | Rate:  1137984 rows/s | Total:   5690000 rows | Queue:   1 items | CPU Usage: 1327.33% | Memory Usage:   4.97 GB | Thread Count: 159
Runtime:   10s | Rate:  1915894 rows/s | Total:  15270000 rows | Queue:   0 items | CPU Usage: 2432.65% | Memory Usage:   4.97 GB | Thread Count: 159
Runtime:   15s | Rate:  1909867 rows/s | Total:  24820000 rows | Queue:  66 items | CPU Usage: 2380.43% | Memory Usage:   4.97 GB | Thread Count: 159
Runtime:   20s | Rate:  1185915 rows/s | Total:  30750000 rows | Queue:   1 items | CPU Usage: 1593.88% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   25s | Rate:  1887852 rows/s | Total:  40190000 rows | Queue:   0 items | CPU Usage: 2436.63% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   30s | Rate:  1871840 rows/s | Total:  49550000 rows | Queue:   0 items | CPU Usage: 2471.58% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   35s | Rate:  1051922 rows/s | Total:  54810000 rows | Queue:  66 items | CPU Usage: 1346.90% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   40s | Rate:  1859855 rows/s | Total:  64110000 rows | Queue:   0 items | CPU Usage: 2473.00% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   45s | Rate:  1887846 rows/s | Total:  73550000 rows | Queue:   0 items | CPU Usage: 2428.43% | Memory Usage:   4.98 GB | Thread Count: 159
Runtime:   50s | Rate:  1595871 rows/s | Total:  81530000 rows | Queue: 140 items | CPU Usage: 1883.64% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   55s | Rate:  1411886 rows/s | Total:  88590000 rows | Queue:   0 items | CPU Usage: 2009.25% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   60s | Rate:  1899878 rows/s | Total:  98090000 rows | Queue:   0 items | CPU Usage: 2434.46% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   65s | Rate:  1889883 rows/s | Total: 107540000 rows | Queue:  10 items | CPU Usage: 2419.64% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   70s | Rate:  1117921 rows/s | Total: 113130000 rows | Queue:   0 items | CPU Usage: 1479.30% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   75s | Rate:  1883849 rows/s | Total: 122550000 rows | Queue:   0 items | CPU Usage: 2432.40% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   80s | Rate:  1891440 rows/s | Total: 132010000 rows | Queue:   0 items | CPU Usage: 2429.69% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   85s | Rate:  1263912 rows/s | Total: 138330000 rows | Queue: 126 items | CPU Usage: 1488.48% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   90s | Rate:  1807841 rows/s | Total: 147370000 rows | Queue:   1 items | CPU Usage: 2484.81% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:   95s | Rate:  1893871 rows/s | Total: 156840000 rows | Queue:   0 items | CPU Usage: 2436.63% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:  100s | Rate:  1619876 rows/s | Total: 164940000 rows | Queue: 141 items | CPU Usage: 1902.25% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:  105s | Rate:  1427896 rows/s | Total: 172080000 rows | Queue:   0 items | CPU Usage: 2036.46% | Memory Usage:   4.99 GB | Thread Count: 159
Runtime:  110s | Rate:  1855875 rows/s | Total: 181360000 rows | Queue:   0 items | CPU Usage: 2373.03% | Memory Usage:   4.99 GB | Thread Count: 158
Runtime:  115s | Rate:  1641864 rows/s | Total: 189570000 rows | Queue:   0 items | CPU Usage: 2125.43% | Memory Usage:   4.99 GB | Thread Count: 156
Runtime:  120s | Rate:  1043922 rows/s | Total: 194790000 rows | Queue: 114 items | CPU Usage: 1236.30% | Memory Usage:   4.99 GB | Thread Count: 155
Runtime:  125s | Rate:   933929 rows/s | Total: 199460000 rows | Queue:   0 items | CPU Usage: 1179.51% | Memory Usage:   4.99 GB | Thread Count: 149
Runtime:  130s | Rate:   107990 rows/s | Total: 200000000 rows | Queue:   0 items | CPU Usage:  106.79% | Memory Usage:   4.99 GB | Thread Count: 143
=============================================== Insert Summary Statistics ====================================================
Insert Threads: 16
Total Rows: 200000000
Total Duration: 119.56 seconds
Average Rate: 1672779.27 rows/second
==============================================================================================================================

=============================================== Insert Latency & Efficiency Metrics ==========================================
Total Operations: 20000
Total Duration: 119.56 seconds
Pure Insert Latency: 89.39 seconds
Effective Time Ratio: 74.77%
Framework Overhead: 25.23%
Idle Time After Finish: 5.45 seconds
Write Latency Distribution: min: 25.4664ms, avg: 71.5132ms, p90: 79.4064ms, p95: 116.3428ms, p99: 562.4548ms, max: 1764.8348ms
==============================================================================================================================
#采样率 1s
Runtime:    1s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  339.74% | Memory Usage:   2.75 GB | Thread Count: 159
Runtime:    2s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  329.86% | Memory Usage:   3.23 GB | Thread Count: 159
Runtime:    3s | Rate:  1739232 rows/s | Total:   1740000 rows | Queue: 142 items | CPU Usage:  735.69% | Memory Usage:   3.49 GB | Thread Count: 159
Runtime:    4s | Rate:   429846 rows/s | Total:   2170000 rows | Queue: 141 items | CPU Usage: 1660.35% | Memory Usage:   3.55 GB | Thread Count: 159
Runtime:    5s | Rate:   469805 rows/s | Total:   2640000 rows | Queue: 142 items | CPU Usage: 1617.35% | Memory Usage:   3.58 GB | Thread Count: 159
Runtime:    6s | Rate:   469781 rows/s | Total:   3110000 rows | Queue: 141 items | CPU Usage: 1606.25% | Memory Usage:   3.61 GB | Thread Count: 159
Runtime:    7s | Rate:   419820 rows/s | Total:   3530000 rows | Queue: 142 items | CPU Usage: 1583.34% | Memory Usage:   3.64 GB | Thread Count: 159
Runtime:    8s | Rate:   509789 rows/s | Total:   4040000 rows | Queue: 142 items | CPU Usage: 1613.32% | Memory Usage:   3.66 GB | Thread Count: 159
Runtime:    9s | Rate:   509784 rows/s | Total:   4550000 rows | Queue: 142 items | CPU Usage: 1566.34% | Memory Usage:   3.68 GB | Thread Count: 159
Runtime:   10s | Rate:   419821 rows/s | Total:   4970000 rows | Queue: 142 items | CPU Usage: 1620.32% | Memory Usage:   3.71 GB | Thread Count: 159
Runtime:   11s | Rate:   459805 rows/s | Total:   5430000 rows | Queue: 141 items | CPU Usage: 1602.32% | Memory Usage:   3.74 GB | Thread Count: 159
Runtime:   12s | Rate:   469798 rows/s | Total:   5900000 rows | Queue: 141 items | CPU Usage: 1602.31% | Memory Usage:   3.76 GB | Thread Count: 159
Runtime:   13s | Rate:   429796 rows/s | Total:   6330000 rows | Queue: 142 items | CPU Usage: 1595.25% | Memory Usage:   3.78 GB | Thread Count: 159
Runtime:   14s | Rate:   489801 rows/s | Total:   6820000 rows | Queue: 142 items | CPU Usage: 1604.35% | Memory Usage:   3.79 GB | Thread Count: 159
Runtime:   15s | Rate:   489793 rows/s | Total:   7310000 rows | Queue: 142 items | CPU Usage: 1603.33% | Memory Usage:   3.81 GB | Thread Count: 159
Runtime:   16s | Rate:   409828 rows/s | Total:   7720000 rows | Queue: 141 items | CPU Usage: 1548.35% | Memory Usage:   3.83 GB | Thread Count: 159
Runtime:   17s | Rate:   499787 rows/s | Total:   8220000 rows | Queue: 141 items | CPU Usage: 1627.29% | Memory Usage:   3.85 GB | Thread Count: 159
Runtime:   18s | Rate:   439809 rows/s | Total:   8660000 rows | Queue: 142 items | CPU Usage: 1574.31% | Memory Usage:   3.88 GB | Thread Count: 159
Runtime:   19s | Rate:   499775 rows/s | Total:   9160000 rows | Queue: 137 items | CPU Usage: 1534.21% | Memory Usage:   3.90 GB | Thread Count: 159
Runtime:   20s | Rate:   479751 rows/s | Total:   9640000 rows | Queue: 142 items | CPU Usage: 1653.27% | Memory Usage:   3.92 GB | Thread Count: 159
Runtime:   21s | Rate:   439813 rows/s | Total:  10080000 rows | Queue: 142 items | CPU Usage: 1570.34% | Memory Usage:   3.95 GB | Thread Count: 159
Runtime:   22s | Rate:   499785 rows/s | Total:  10580000 rows | Queue: 140 items | CPU Usage: 1617.29% | Memory Usage:   3.97 GB | Thread Count: 159
Runtime:   23s | Rate:   439823 rows/s | Total:  11020000 rows | Queue: 142 items | CPU Usage: 1596.39% | Memory Usage:   3.98 GB | Thread Count: 159
Runtime:   24s | Rate:   459836 rows/s | Total:  11480000 rows | Queue: 141 items | CPU Usage: 1601.44% | Memory Usage:   4.00 GB | Thread Count: 159
Runtime:   25s | Rate:   479830 rows/s | Total:  11960000 rows | Queue: 142 items | CPU Usage: 1604.43% | Memory Usage:   4.01 GB | Thread Count: 159
Runtime:   26s | Rate:   439846 rows/s | Total:  12400000 rows | Queue: 140 items | CPU Usage: 1595.43% | Memory Usage:   4.02 GB | Thread Count: 159
Runtime:   27s | Rate:   459826 rows/s | Total:  12860000 rows | Queue: 142 items | CPU Usage: 1599.41% | Memory Usage:   4.03 GB | Thread Count: 159
Runtime:   28s | Rate:   469833 rows/s | Total:  13330000 rows | Queue: 141 items | CPU Usage: 1573.45% | Memory Usage:   4.04 GB | Thread Count: 159
Runtime:   29s | Rate:   459838 rows/s | Total:  13790000 rows | Queue: 142 items | CPU Usage: 1617.41% | Memory Usage:   4.05 GB | Thread Count: 159
Runtime:   30s | Rate:   459829 rows/s | Total:  14250000 rows | Queue: 142 items | CPU Usage: 1593.42% | Memory Usage:   4.06 GB | Thread Count: 159
Runtime:   31s | Rate:   539806 rows/s | Total:  14790000 rows | Queue: 141 items | CPU Usage: 1626.43% | Memory Usage:   4.07 GB | Thread Count: 159
Runtime:   32s | Rate:   519823 rows/s | Total:  15310000 rows | Queue: 142 items | CPU Usage: 1594.45% | Memory Usage:   4.08 GB | Thread Count: 159
Runtime:   33s | Rate:   469818 rows/s | Total:  15780000 rows | Queue: 141 items | CPU Usage: 1609.34% | Memory Usage:   4.09 GB | Thread Count: 159
Runtime:   34s | Rate:   629728 rows/s | Total:  16410000 rows | Queue: 142 items | CPU Usage: 1574.33% | Memory Usage:   4.10 GB | Thread Count: 159
Runtime:   35s | Rate:   739697 rows/s | Total:  17150000 rows | Queue: 141 items | CPU Usage: 1654.29% | Memory Usage:   4.11 GB | Thread Count: 159
Runtime:   36s | Rate:   639712 rows/s | Total:  17790000 rows | Queue: 141 items | CPU Usage: 1637.29% | Memory Usage:   4.12 GB | Thread Count: 159
Runtime:   37s | Rate:   709691 rows/s | Total:  18500000 rows | Queue: 142 items | CPU Usage: 1632.29% | Memory Usage:   4.14 GB | Thread Count: 159
Runtime:   38s | Rate:   769691 rows/s | Total:  19270000 rows | Queue: 141 items | CPU Usage: 1641.30% | Memory Usage:   4.15 GB | Thread Count: 159
Runtime:   39s | Rate:   949523 rows/s | Total:  20220000 rows | Queue: 142 items | CPU Usage: 1697.20% | Memory Usage:   4.18 GB | Thread Count: 159
Runtime:   40s | Rate:   829652 rows/s | Total:  21050000 rows | Queue: 142 items | CPU Usage: 1660.32% | Memory Usage:   4.19 GB | Thread Count: 159
Runtime:   41s | Rate:   819673 rows/s | Total:  21870000 rows | Queue: 140 items | CPU Usage: 1646.34% | Memory Usage:   4.21 GB | Thread Count: 159
Runtime:   42s | Rate:  1059587 rows/s | Total:  22930000 rows | Queue: 138 items | CPU Usage: 1716.32% | Memory Usage:   4.22 GB | Thread Count: 159
Runtime:   43s | Rate:  1229520 rows/s | Total:  24160000 rows | Queue: 140 items | CPU Usage: 1733.32% | Memory Usage:   4.23 GB | Thread Count: 159
Runtime:   44s | Rate:  1049558 rows/s | Total:  25210000 rows | Queue: 141 items | CPU Usage: 1697.34% | Memory Usage:   4.25 GB | Thread Count: 159
Runtime:   45s | Rate:   449845 rows/s | Total:  25660000 rows | Queue: 142 items | CPU Usage:  812.70% | Memory Usage:   4.25 GB | Thread Count: 159
Runtime:   46s | Rate:   929623 rows/s | Total:  26590000 rows | Queue: 138 items | CPU Usage: 1351.46% | Memory Usage:   4.28 GB | Thread Count: 159
Runtime:   47s | Rate:  1159571 rows/s | Total:  27750000 rows | Queue: 140 items | CPU Usage: 1727.36% | Memory Usage:   4.29 GB | Thread Count: 159
Runtime:   48s | Rate:  1479463 rows/s | Total:  29230000 rows | Queue: 139 items | CPU Usage: 1787.33% | Memory Usage:   4.30 GB | Thread Count: 159
Runtime:   49s | Rate:  1489493 rows/s | Total:  30720000 rows | Queue: 139 items | CPU Usage: 1787.42% | Memory Usage:   4.33 GB | Thread Count: 159
Runtime:   50s | Rate:  1559422 rows/s | Total:  32280000 rows | Queue: 137 items | CPU Usage: 1797.25% | Memory Usage:   4.34 GB | Thread Count: 159
Runtime:   51s | Rate:  1899116 rows/s | Total:  34180000 rows | Queue: 139 items | CPU Usage: 1876.18% | Memory Usage:   4.38 GB | Thread Count: 159
Runtime:   52s | Rate:  2069136 rows/s | Total:  36250000 rows | Queue: 138 items | CPU Usage: 1914.17% | Memory Usage:   4.41 GB | Thread Count: 159
Runtime:   53s | Rate:  2289072 rows/s | Total:  38540000 rows | Queue: 141 items | CPU Usage: 1941.19% | Memory Usage:   4.43 GB | Thread Count: 159
Runtime:   54s | Rate:  2388780 rows/s | Total:  40930000 rows | Queue: 138 items | CPU Usage: 1970.07% | Memory Usage:   4.45 GB | Thread Count: 159
Runtime:   55s | Rate:  2629075 rows/s | Total:  43560000 rows | Queue: 137 items | CPU Usage: 2020.28% | Memory Usage:   4.46 GB | Thread Count: 159
Runtime:   56s | Rate:  2728928 rows/s | Total:  46290000 rows | Queue: 138 items | CPU Usage: 2036.13% | Memory Usage:   4.47 GB | Thread Count: 159
Runtime:   57s | Rate:  2718805 rows/s | Total:  49010000 rows | Queue: 137 items | CPU Usage: 2034.14% | Memory Usage:   4.48 GB | Thread Count: 159
Runtime:   58s | Rate:  2948781 rows/s | Total:  51960000 rows | Queue: 140 items | CPU Usage: 2077.24% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   59s | Rate:  1379490 rows/s | Total:  53340000 rows | Queue: 142 items | CPU Usage:  985.65% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   60s | Rate:   159939 rows/s | Total:  53500000 rows | Queue: 142 items | CPU Usage:  114.96% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   61s | Rate:   249911 rows/s | Total:  53750000 rows | Queue: 142 items | CPU Usage:  170.94% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   62s | Rate:   959614 rows/s | Total:  54710000 rows | Queue: 138 items | CPU Usage:  675.72% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   63s | Rate:  3038841 rows/s | Total:  57750000 rows | Queue: 135 items | CPU Usage: 2091.29% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   64s | Rate:  3039052 rows/s | Total:  60790000 rows | Queue: 138 items | CPU Usage: 2082.20% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   65s | Rate:  3038781 rows/s | Total:  63830000 rows | Queue: 136 items | CPU Usage: 2100.15% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   66s | Rate:  3078796 rows/s | Total:  66910000 rows | Queue: 138 items | CPU Usage: 2094.24% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   67s | Rate:  3028624 rows/s | Total:  69940000 rows | Queue: 136 items | CPU Usage: 2076.10% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   68s | Rate:  3068973 rows/s | Total:  73010000 rows | Queue: 135 items | CPU Usage: 2098.29% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   69s | Rate:  3118982 rows/s | Total:  76130000 rows | Queue: 138 items | CPU Usage: 2094.35% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   70s | Rate:  3079057 rows/s | Total:  79210000 rows | Queue: 135 items | CPU Usage: 2092.34% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   71s | Rate:  2149242 rows/s | Total:  81360000 rows | Queue: 142 items | CPU Usage: 1478.51% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   72s | Rate:   759757 rows/s | Total:  82120000 rows | Queue: 142 items | CPU Usage:  531.83% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   73s | Rate:  2509281 rows/s | Total:  84630000 rows | Queue: 137 items | CPU Usage: 1678.50% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   74s | Rate:  3009048 rows/s | Total:  87640000 rows | Queue: 139 items | CPU Usage: 2041.33% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   75s | Rate:  3089030 rows/s | Total:  90730000 rows | Queue: 136 items | CPU Usage: 2115.30% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   76s | Rate:  3148905 rows/s | Total:  93880000 rows | Queue: 138 items | CPU Usage: 2113.29% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   77s | Rate:  3138952 rows/s | Total:  97020000 rows | Queue: 138 items | CPU Usage: 2118.27% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   78s | Rate:  3148934 rows/s | Total: 100170000 rows | Queue: 140 items | CPU Usage: 2081.33% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   79s | Rate:  3099011 rows/s | Total: 103270000 rows | Queue: 136 items | CPU Usage: 2076.37% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   80s | Rate:  3149122 rows/s | Total: 106420000 rows | Queue: 139 items | CPU Usage: 2105.39% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   81s | Rate:  2469213 rows/s | Total: 108890000 rows | Queue: 142 items | CPU Usage: 1668.50% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   82s | Rate:   209933 rows/s | Total: 109100000 rows | Queue: 133 items | CPU Usage:  150.95% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   83s | Rate:   359875 rows/s | Total: 109460000 rows | Queue: 142 items | CPU Usage:  254.91% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   84s | Rate:   749742 rows/s | Total: 110210000 rows | Queue: 140 items | CPU Usage:  515.80% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   85s | Rate:  1849233 rows/s | Total: 112060000 rows | Queue: 137 items | CPU Usage: 1252.46% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   86s | Rate:  3158662 rows/s | Total: 115220000 rows | Queue: 139 items | CPU Usage: 2120.18% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   87s | Rate:  3159016 rows/s | Total: 118380000 rows | Queue: 137 items | CPU Usage: 2108.30% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   88s | Rate:  3118843 rows/s | Total: 121500000 rows | Queue: 137 items | CPU Usage: 2109.14% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   89s | Rate:  2418903 rows/s | Total: 123920000 rows | Queue: 137 items | CPU Usage: 1629.31% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   90s | Rate:  3088314 rows/s | Total: 127010000 rows | Queue: 135 items | CPU Usage: 2074.86% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   91s | Rate:  3148808 rows/s | Total: 130160000 rows | Queue: 138 items | CPU Usage: 2097.24% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   92s | Rate:  3158853 rows/s | Total: 133320000 rows | Queue: 138 items | CPU Usage: 2102.22% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   93s | Rate:  2638940 rows/s | Total: 135960000 rows | Queue: 142 items | CPU Usage: 1771.36% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   94s | Rate:   869737 rows/s | Total: 136830000 rows | Queue: 142 items | CPU Usage:  597.80% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   95s | Rate:   429821 rows/s | Total: 137260000 rows | Queue: 142 items | CPU Usage:  305.88% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   96s | Rate:   459794 rows/s | Total: 137720000 rows | Queue: 142 items | CPU Usage:  325.86% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   97s | Rate:  1569490 rows/s | Total: 139290000 rows | Queue: 134 items | CPU Usage: 1074.59% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   98s | Rate:  3178673 rows/s | Total: 142470000 rows | Queue: 139 items | CPU Usage: 2111.13% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   99s | Rate:  3148786 rows/s | Total: 145620000 rows | Queue: 138 items | CPU Usage: 2108.24% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  100s | Rate:  3158642 rows/s | Total: 148780000 rows | Queue: 138 items | CPU Usage: 2105.13% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  101s | Rate:  3139020 rows/s | Total: 151920000 rows | Queue: 134 items | CPU Usage: 2119.28% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  102s | Rate:  3118909 rows/s | Total: 155040000 rows | Queue: 137 items | CPU Usage: 2100.24% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  103s | Rate:  3148607 rows/s | Total: 158190000 rows | Queue: 137 items | CPU Usage: 2100.06% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  104s | Rate:  2458965 rows/s | Total: 160650000 rows | Queue: 139 items | CPU Usage: 1652.28% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  105s | Rate:  2948708 rows/s | Total: 163600000 rows | Queue: 142 items | CPU Usage: 1993.28% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  106s | Rate:  1459515 rows/s | Total: 165060000 rows | Queue: 140 items | CPU Usage:  999.60% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  107s | Rate:  1029549 rows/s | Total: 166090000 rows | Queue: 140 items | CPU Usage:  690.73% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  108s | Rate:  2089333 rows/s | Total: 168180000 rows | Queue: 134 items | CPU Usage: 1435.50% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  109s | Rate:  3138752 rows/s | Total: 171320000 rows | Queue: 137 items | CPU Usage: 2092.22% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  110s | Rate:  3105795 rows/s | Total: 174430000 rows | Queue: 138 items | CPU Usage: 2074.13% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  111s | Rate:  3138744 rows/s | Total: 177570000 rows | Queue: 139 items | CPU Usage: 2110.30% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  112s | Rate:  3139047 rows/s | Total: 180710000 rows | Queue: 136 items | CPU Usage: 2108.31% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  113s | Rate:  3127082 rows/s | Total: 183840000 rows | Queue: 136 items | CPU Usage: 2130.02% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  114s | Rate:  3148971 rows/s | Total: 186990000 rows | Queue: 137 items | CPU Usage: 2108.33% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:  115s | Rate:  3129051 rows/s | Total: 190120000 rows | Queue: 134 items | CPU Usage: 2095.32% | Memory Usage:   4.54 GB | Thread Count: 158
Runtime:  116s | Rate:  2109160 rows/s | Total: 192230000 rows | Queue: 142 items | CPU Usage: 1414.46% | Memory Usage:   4.53 GB | Thread Count: 157
Runtime:  117s | Rate:   569782 rows/s | Total: 192800000 rows | Queue: 142 items | CPU Usage:  415.84% | Memory Usage:   4.53 GB | Thread Count: 157
Runtime:  118s | Rate:   429836 rows/s | Total: 193230000 rows | Queue: 142 items | CPU Usage:  304.88% | Memory Usage:   4.53 GB | Thread Count: 157
Runtime:  119s | Rate:   749708 rows/s | Total: 193980000 rows | Queue: 142 items | CPU Usage:  518.80% | Memory Usage:   4.53 GB | Thread Count: 157
Runtime:  120s | Rate:  1599407 rows/s | Total: 195580000 rows | Queue: 141 items | CPU Usage: 1062.60% | Memory Usage:   4.53 GB | Thread Count: 154
Runtime:  121s | Rate:  2909080 rows/s | Total: 198490000 rows | Queue: 120 items | CPU Usage: 2103.38% | Memory Usage:   4.53 GB | Thread Count: 148
Runtime:  122s | Rate:  1509515 rows/s | Total: 200000000 rows | Queue:   0 items | CPU Usage: 1642.44% | Memory Usage:   4.53 GB | Thread Count: 143
=============================================== Insert Summary Statistics ====================================================
Insert Threads: 16
Total Rows: 200000000
Total Duration: 115.69 seconds
Average Rate: 1728708.67 rows/second
==============================================================================================================================

=============================================== Insert Latency & Efficiency Metrics ==========================================
Total Operations: 20000
Total Duration: 115.69 seconds
Pure Insert Latency: 115.60 seconds
Effective Time Ratio: 99.92%
Framework Overhead: 0.07%
Idle Time After Finish: 0.02 seconds
Write Latency Distribution: min: 28.5932ms, avg: 92.4788ms, p90: 258.7585ms, p95: 328.9716ms, p99: 493.1834ms, max: 1292.8813ms
==============================================================================================================================
```

Bypass = 8 
```yaml
Runtime:    1s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  350.84% | Memory Usage:   2.77 GB | Thread Count: 159
Runtime:    2s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  335.85% | Memory Usage:   3.25 GB | Thread Count: 159
Runtime:    3s | Rate:  1759217 rows/s | Total:   1760000 rows | Queue: 142 items | CPU Usage:  833.64% | Memory Usage:   3.49 GB | Thread Count: 159
Runtime:    4s | Rate:   439838 rows/s | Total:   2200000 rows | Queue: 141 items | CPU Usage: 1620.41% | Memory Usage:   3.55 GB | Thread Count: 159
Runtime:    5s | Rate:   529815 rows/s | Total:   2730000 rows | Queue: 142 items | CPU Usage: 1622.44% | Memory Usage:   3.58 GB | Thread Count: 159
Runtime:    6s | Rate:   439850 rows/s | Total:   3170000 rows | Queue: 142 items | CPU Usage: 1600.46% | Memory Usage:   3.61 GB | Thread Count: 159
Runtime:    7s | Rate:   459843 rows/s | Total:   3630000 rows | Queue: 142 items | CPU Usage: 1602.45% | Memory Usage:   3.64 GB | Thread Count: 159
Runtime:    8s | Rate:   489835 rows/s | Total:   4120000 rows | Queue: 141 items | CPU Usage: 1607.46% | Memory Usage:   3.66 GB | Thread Count: 159
Runtime:    9s | Rate:   439840 rows/s | Total:   4560000 rows | Queue: 141 items | CPU Usage: 1597.34% | Memory Usage:   3.69 GB | Thread Count: 159
Runtime:   10s | Rate:   509784 rows/s | Total:   5070000 rows | Queue: 142 items | CPU Usage: 1570.38% | Memory Usage:   3.71 GB | Thread Count: 159
Runtime:   11s | Rate:   449817 rows/s | Total:   5520000 rows | Queue: 142 items | CPU Usage: 1609.39% | Memory Usage:   3.74 GB | Thread Count: 159
Runtime:   12s | Rate:   499830 rows/s | Total:   6020000 rows | Queue: 139 items | CPU Usage: 1607.44% | Memory Usage:   3.76 GB | Thread Count: 159
Runtime:   13s | Rate:   469831 rows/s | Total:   6490000 rows | Queue: 142 items | CPU Usage: 1594.44% | Memory Usage:   3.77 GB | Thread Count: 159
Runtime:   14s | Rate:   509820 rows/s | Total:   7000000 rows | Queue: 142 items | CPU Usage: 1596.44% | Memory Usage:   3.79 GB | Thread Count: 159
Runtime:   15s | Rate:   449844 rows/s | Total:   7450000 rows | Queue: 142 items | CPU Usage: 1600.44% | Memory Usage:   3.81 GB | Thread Count: 159
Runtime:   16s | Rate:   529813 rows/s | Total:   7980000 rows | Queue: 141 items | CPU Usage: 1617.45% | Memory Usage:   3.83 GB | Thread Count: 159
Runtime:   17s | Rate:   469842 rows/s | Total:   8450000 rows | Queue: 142 items | CPU Usage: 1600.47% | Memory Usage:   3.86 GB | Thread Count: 159
Runtime:   18s | Rate:   439856 rows/s | Total:   8890000 rows | Queue: 140 items | CPU Usage: 1601.43% | Memory Usage:   3.88 GB | Thread Count: 159
Runtime:   19s | Rate:   519811 rows/s | Total:   9410000 rows | Queue: 141 items | CPU Usage: 1590.46% | Memory Usage:   3.90 GB | Thread Count: 159
Runtime:   20s | Rate:   439855 rows/s | Total:   9850000 rows | Queue: 142 items | CPU Usage: 1607.47% | Memory Usage:   3.91 GB | Thread Count: 159
Runtime:   21s | Rate:   569804 rows/s | Total:  10420000 rows | Queue: 142 items | CPU Usage: 1615.37% | Memory Usage:   3.94 GB | Thread Count: 159
Runtime:   22s | Rate:   539792 rows/s | Total:  10960000 rows | Queue: 142 items | CPU Usage: 1593.45% | Memory Usage:   3.96 GB | Thread Count: 159
Runtime:   23s | Rate:   539808 rows/s | Total:  11500000 rows | Queue: 142 items | CPU Usage: 1600.43% | Memory Usage:   3.97 GB | Thread Count: 159
Runtime:   24s | Rate:   589787 rows/s | Total:  12090000 rows | Queue: 139 items | CPU Usage: 1644.40% | Memory Usage:   4.00 GB | Thread Count: 159
Runtime:   25s | Rate:   579791 rows/s | Total:  12670000 rows | Queue: 142 items | CPU Usage: 1621.44% | Memory Usage:   4.01 GB | Thread Count: 159
Runtime:   26s | Rate:   599779 rows/s | Total:  13270000 rows | Queue: 140 items | CPU Usage: 1627.38% | Memory Usage:   4.03 GB | Thread Count: 159
Runtime:   27s | Rate:   699739 rows/s | Total:  13970000 rows | Queue: 141 items | CPU Usage: 1642.37% | Memory Usage:   4.05 GB | Thread Count: 159
Runtime:   28s | Rate:   719698 rows/s | Total:  14690000 rows | Queue: 141 items | CPU Usage: 1569.41% | Memory Usage:   4.06 GB | Thread Count: 159
Runtime:   29s | Rate:   759747 rows/s | Total:  15450000 rows | Queue: 141 items | CPU Usage: 1652.38% | Memory Usage:   4.07 GB | Thread Count: 159
Runtime:   30s | Rate:   769675 rows/s | Total:  16220000 rows | Queue: 142 items | CPU Usage: 1658.28% | Memory Usage:   4.09 GB | Thread Count: 159
Runtime:   31s | Rate:   749668 rows/s | Total:  16970000 rows | Queue: 141 items | CPU Usage: 1652.27% | Memory Usage:   4.11 GB | Thread Count: 159
Runtime:   32s | Rate:   759701 rows/s | Total:  17730000 rows | Queue: 139 items | CPU Usage: 1657.40% | Memory Usage:   4.13 GB | Thread Count: 159
Runtime:   33s | Rate:   909715 rows/s | Total:  18640000 rows | Queue: 141 items | CPU Usage: 1663.45% | Memory Usage:   4.16 GB | Thread Count: 159
Runtime:   34s | Rate:   999641 rows/s | Total:  19640000 rows | Queue: 141 items | CPU Usage: 1705.39% | Memory Usage:   4.18 GB | Thread Count: 159
Runtime:   35s | Rate:  1089612 rows/s | Total:  20730000 rows | Queue: 140 items | CPU Usage: 1711.37% | Memory Usage:   4.19 GB | Thread Count: 159
Runtime:   36s | Rate:  1079611 rows/s | Total:  21810000 rows | Queue: 138 items | CPU Usage: 1709.39% | Memory Usage:   4.21 GB | Thread Count: 159
Runtime:   37s | Rate:  1299532 rows/s | Total:  23110000 rows | Queue: 138 items | CPU Usage: 1752.38% | Memory Usage:   4.23 GB | Thread Count: 159
Runtime:   38s | Rate:  1419500 rows/s | Total:  24530000 rows | Queue: 139 items | CPU Usage: 1776.37% | Memory Usage:   4.24 GB | Thread Count: 159
Runtime:   39s | Rate:  1019597 rows/s | Total:  25550000 rows | Queue: 142 items | CPU Usage: 1203.53% | Memory Usage:   4.26 GB | Thread Count: 159
Runtime:   40s | Rate:   439832 rows/s | Total:  25990000 rows | Queue: 136 items | CPU Usage:  420.84% | Memory Usage:   4.26 GB | Thread Count: 159
Runtime:   41s | Rate:  1689390 rows/s | Total:  27680000 rows | Queue: 142 items | CPU Usage: 1850.29% | Memory Usage:   4.27 GB | Thread Count: 159
Runtime:   42s | Rate:  1999169 rows/s | Total:  29680000 rows | Queue: 141 items | CPU Usage: 1881.30% | Memory Usage:   4.30 GB | Thread Count: 159
Runtime:   43s | Rate:  2049394 rows/s | Total:  31730000 rows | Queue: 141 items | CPU Usage: 1840.39% | Memory Usage:   4.34 GB | Thread Count: 159
Runtime:   44s | Rate:  1989280 rows/s | Total:  33720000 rows | Queue: 138 items | CPU Usage: 1886.37% | Memory Usage:   4.36 GB | Thread Count: 159
Runtime:   45s | Rate:  2459252 rows/s | Total:  36180000 rows | Queue: 137 items | CPU Usage: 1965.33% | Memory Usage:   4.40 GB | Thread Count: 159
Runtime:   46s | Rate:  2469092 rows/s | Total:  38650000 rows | Queue: 139 items | CPU Usage: 1956.27% | Memory Usage:   4.44 GB | Thread Count: 159
Runtime:   47s | Rate:  2628952 rows/s | Total:  41280000 rows | Queue: 139 items | CPU Usage: 1958.21% | Memory Usage:   4.45 GB | Thread Count: 159
Runtime:   48s | Rate:  2698906 rows/s | Total:  43980000 rows | Queue: 137 items | CPU Usage: 1975.21% | Memory Usage:   4.46 GB | Thread Count: 159
Runtime:   49s | Rate:  2728897 rows/s | Total:  46710000 rows | Queue: 138 items | CPU Usage: 1942.28% | Memory Usage:   4.47 GB | Thread Count: 159
Runtime:   50s | Rate:  2808941 rows/s | Total:  49520000 rows | Queue: 137 items | CPU Usage: 1999.14% | Memory Usage:   4.48 GB | Thread Count: 159
Runtime:   51s | Rate:  2828806 rows/s | Total:  52350000 rows | Queue: 138 items | CPU Usage: 1972.29% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   52s | Rate:   979557 rows/s | Total:  53330000 rows | Queue: 142 items | CPU Usage:  702.66% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   53s | Rate:   199913 rows/s | Total:  53530000 rows | Queue: 142 items | CPU Usage:  141.94% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   54s | Rate:   219908 rows/s | Total:  53750000 rows | Queue: 142 items | CPU Usage:  155.93% | Memory Usage:   4.49 GB | Thread Count: 159
Runtime:   55s | Rate:  1129586 rows/s | Total:  54880000 rows | Queue: 139 items | CPU Usage:  803.66% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   56s | Rate:  2798261 rows/s | Total:  57680000 rows | Queue: 138 items | CPU Usage: 1953.88% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   57s | Rate:  2908890 rows/s | Total:  60590000 rows | Queue: 140 items | CPU Usage: 1999.24% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   58s | Rate:  1819324 rows/s | Total:  62410000 rows | Queue: 141 items | CPU Usage: 1266.49% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   59s | Rate:  2758889 rows/s | Total:  65170000 rows | Queue: 138 items | CPU Usage: 1929.33% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   60s | Rate:  2879098 rows/s | Total:  68050000 rows | Queue: 136 items | CPU Usage: 1977.29% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   61s | Rate:  2828807 rows/s | Total:  70880000 rows | Queue: 139 items | CPU Usage: 1950.23% | Memory Usage:   4.51 GB | Thread Count: 159
Runtime:   62s | Rate:  2878796 rows/s | Total:  73760000 rows | Queue: 136 items | CPU Usage: 2008.13% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   63s | Rate:  2788892 rows/s | Total:  76550000 rows | Queue: 138 items | CPU Usage: 1916.25% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   64s | Rate:  2818914 rows/s | Total:  79370000 rows | Queue: 139 items | CPU Usage: 1932.28% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   65s | Rate:  1729311 rows/s | Total:  81100000 rows | Queue: 142 items | CPU Usage: 1221.52% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   66s | Rate:   539775 rows/s | Total:  81640000 rows | Queue: 142 items | CPU Usage:  382.84% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   67s | Rate:  1609400 rows/s | Total:  83250000 rows | Queue: 134 items | CPU Usage: 1119.55% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   68s | Rate:  2868811 rows/s | Total:  86120000 rows | Queue: 137 items | CPU Usage: 1960.18% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   69s | Rate:  2848767 rows/s | Total:  88970000 rows | Queue: 136 items | CPU Usage: 1964.17% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   70s | Rate:  2868762 rows/s | Total:  91840000 rows | Queue: 139 items | CPU Usage: 1971.06% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   71s | Rate:  2895564 rows/s | Total:  94740000 rows | Queue: 137 items | CPU Usage: 2002.06% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   72s | Rate:  2888888 rows/s | Total:  97630000 rows | Queue: 137 items | CPU Usage: 1975.23% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   73s | Rate:  2628983 rows/s | Total: 100260000 rows | Queue: 142 items | CPU Usage: 1835.27% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   74s | Rate:  2678834 rows/s | Total: 102940000 rows | Queue: 139 items | CPU Usage: 1850.18% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   75s | Rate:  2808848 rows/s | Total: 105750000 rows | Queue: 139 items | CPU Usage: 1942.29% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   76s | Rate:  2569087 rows/s | Total: 108320000 rows | Queue: 142 items | CPU Usage: 1795.34% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   77s | Rate:   689718 rows/s | Total: 109010000 rows | Queue: 138 items | CPU Usage:  485.81% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   78s | Rate:   449835 rows/s | Total: 109460000 rows | Queue: 142 items | CPU Usage:  318.88% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   79s | Rate:   429825 rows/s | Total: 109890000 rows | Queue: 142 items | CPU Usage:  314.86% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   80s | Rate:  1246208 rows/s | Total: 111140000 rows | Queue: 135 items | CPU Usage:  866.35% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   81s | Rate:  2788773 rows/s | Total: 113930000 rows | Queue: 138 items | CPU Usage: 1889.28% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   82s | Rate:  2869087 rows/s | Total: 116800000 rows | Queue: 141 items | CPU Usage: 1972.35% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   83s | Rate:  2799034 rows/s | Total: 119600000 rows | Queue: 136 items | CPU Usage: 1970.28% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   84s | Rate:  2808901 rows/s | Total: 122410000 rows | Queue: 138 items | CPU Usage: 1928.21% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   85s | Rate:  2868774 rows/s | Total: 125280000 rows | Queue: 138 items | CPU Usage: 1961.21% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   86s | Rate:  2838942 rows/s | Total: 128120000 rows | Queue: 135 items | CPU Usage: 1988.29% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   87s | Rate:  2879016 rows/s | Total: 131000000 rows | Queue: 137 items | CPU Usage: 1970.32% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   88s | Rate:  1929307 rows/s | Total: 132930000 rows | Queue: 139 items | CPU Usage: 1326.52% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   89s | Rate:  2828989 rows/s | Total: 135760000 rows | Queue: 140 items | CPU Usage: 1939.25% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   90s | Rate:  1049572 rows/s | Total: 136810000 rows | Queue: 140 items | CPU Usage:  757.74% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   91s | Rate:   459836 rows/s | Total: 137270000 rows | Queue: 142 items | CPU Usage:  324.88% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   92s | Rate:   439836 rows/s | Total: 137710000 rows | Queue: 142 items | CPU Usage:  307.88% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   93s | Rate:  2029261 rows/s | Total: 139740000 rows | Queue: 137 items | CPU Usage: 1446.43% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   94s | Rate:  2868821 rows/s | Total: 142610000 rows | Queue: 137 items | CPU Usage: 2011.28% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   95s | Rate:  2879093 rows/s | Total: 145490000 rows | Queue: 141 items | CPU Usage: 1978.39% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   96s | Rate:  2849149 rows/s | Total: 148340000 rows | Queue: 138 items | CPU Usage: 1987.30% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   97s | Rate:  2858842 rows/s | Total: 151200000 rows | Queue: 142 items | CPU Usage: 1960.27% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   98s | Rate:  2819067 rows/s | Total: 154020000 rows | Queue: 139 items | CPU Usage: 1948.26% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:   99s | Rate:  2868637 rows/s | Total: 156890000 rows | Queue: 138 items | CPU Usage: 1985.13% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  100s | Rate:  2899014 rows/s | Total: 159790000 rows | Queue: 139 items | CPU Usage: 2007.37% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  101s | Rate:  2879145 rows/s | Total: 162670000 rows | Queue: 138 items | CPU Usage: 2012.59% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  102s | Rate:  1767745 rows/s | Total: 164440000 rows | Queue: 142 items | CPU Usage: 1224.49% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  103s | Rate:   499803 rows/s | Total: 164940000 rows | Queue: 142 items | CPU Usage:  364.88% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  104s | Rate:   399845 rows/s | Total: 165340000 rows | Queue: 142 items | CPU Usage:  288.88% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  105s | Rate:   999612 rows/s | Total: 166340000 rows | Queue: 136 items | CPU Usage:  701.73% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  106s | Rate:  1649417 rows/s | Total: 167990000 rows | Queue: 140 items | CPU Usage: 1128.58% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  107s | Rate:  2838864 rows/s | Total: 170830000 rows | Queue: 136 items | CPU Usage: 1983.17% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  108s | Rate:  2838864 rows/s | Total: 173670000 rows | Queue: 136 items | CPU Usage: 1976.24% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  109s | Rate:  2848989 rows/s | Total: 176520000 rows | Queue: 137 items | CPU Usage: 1977.25% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  110s | Rate:  2868838 rows/s | Total: 179390000 rows | Queue: 138 items | CPU Usage: 1981.26% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  111s | Rate:  2429030 rows/s | Total: 181820000 rows | Queue: 142 items | CPU Usage: 1691.34% | Memory Usage:   4.53 GB | Thread Count: 159
Runtime:  112s | Rate:  1189503 rows/s | Total: 183010000 rows | Queue: 142 items | CPU Usage:  831.64% | Memory Usage:   4.53 GB | Thread Count: 158
Runtime:  113s | Rate:   609762 rows/s | Total: 183620000 rows | Queue: 141 items | CPU Usage:  438.84% | Memory Usage:   4.53 GB | Thread Count: 158
Runtime:  114s | Rate:   619771 rows/s | Total: 184240000 rows | Queue: 142 items | CPU Usage:  456.85% | Memory Usage:   4.53 GB | Thread Count: 158
Runtime:  115s | Rate:  1229662 rows/s | Total: 185470000 rows | Queue: 135 items | CPU Usage:  878.74% | Memory Usage:   4.53 GB | Thread Count: 158
Runtime:  116s | Rate:  2359147 rows/s | Total: 187830000 rows | Queue: 138 items | CPU Usage: 1605.35% | Memory Usage:   4.53 GB | Thread Count: 158
Runtime:  117s | Rate:  2948797 rows/s | Total: 190780000 rows | Queue: 137 items | CPU Usage: 2031.23% | Memory Usage:   4.53 GB | Thread Count: 156
Runtime:  118s | Rate:  2729112 rows/s | Total: 193510000 rows | Queue: 141 items | CPU Usage: 1875.42% | Memory Usage:   4.53 GB | Thread Count: 156
Runtime:  119s | Rate:  2769151 rows/s | Total: 196280000 rows | Queue: 138 items | CPU Usage: 1956.22% | Memory Usage:   4.53 GB | Thread Count: 150
Runtime:  120s | Rate:  2868711 rows/s | Total: 199150000 rows | Queue: 138 items | CPU Usage: 2103.19% | Memory Usage:   4.53 GB | Thread Count: 149
Runtime:  121s | Rate:   849647 rows/s | Total: 200000000 rows | Queue:   0 items | CPU Usage: 1296.49% | Memory Usage:   4.53 GB | Thread Count: 143
=============================================== Insert Summary Statistics ====================================================
Insert Threads: 16
Total Rows: 200000000
Total Duration: 118.77 seconds
Average Rate: 1683935.76 rows/second
==============================================================================================================================

=============================================== Insert Latency & Efficiency Metrics ==========================================
Total Operations: 20000
Total Duration: 118.77 seconds
Pure Insert Latency: 118.66 seconds
Effective Time Ratio: 99.91%
Framework Overhead: 0.07%
Idle Time After Finish: 0.02 seconds
Write Latency Distribution: min: 32.7702ms, avg: 94.9291ms, p90: 260.5279ms, p95: 332.2777ms, p99: 458.4920ms, max: 1203.8262ms
==============================================================================================================================

```

## 11. 注释掉walwrite

bypassFlag=0，已经没有cpu和写入性能下降的情况
```yaml
Runtime:    1s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  333.79% | Memory Usage:   2.75 GB | Thread Count: 159
Runtime:    2s | Rate:        0 rows/s | Total:         0 rows | Queue:   0 items | CPU Usage:  313.84% | Memory Usage:   3.20 GB | Thread Count: 159
Runtime:    3s | Rate:  1599234 rows/s | Total:   1600000 rows | Queue: 143 items | CPU Usage:  591.76% | Memory Usage:   3.46 GB | Thread Count: 159
Runtime:    4s | Rate:   429861 rows/s | Total:   2030000 rows | Queue: 142 items | CPU Usage: 1357.57% | Memory Usage:   3.57 GB | Thread Count: 159
Runtime:    5s | Rate:   489857 rows/s | Total:   2520000 rows | Queue: 142 items | CPU Usage: 1616.53% | Memory Usage:   3.61 GB | Thread Count: 159
Runtime:    6s | Rate:   529829 rows/s | Total:   3050000 rows | Queue: 141 items | CPU Usage: 1589.47% | Memory Usage:   3.64 GB | Thread Count: 159
Runtime:    7s | Rate:   449849 rows/s | Total:   3500000 rows | Queue: 140 items | CPU Usage: 1583.47% | Memory Usage:   3.68 GB | Thread Count: 159
Runtime:    8s | Rate:   449851 rows/s | Total:   3950000 rows | Queue: 142 items | CPU Usage: 1600.45% | Memory Usage:   3.70 GB | Thread Count: 159
Runtime:    9s | Rate:   479833 rows/s | Total:   4430000 rows | Queue: 140 items | CPU Usage: 1584.46% | Memory Usage:   3.74 GB | Thread Count: 159
Runtime:   10s | Rate:   469842 rows/s | Total:   4900000 rows | Queue: 142 items | CPU Usage: 1538.48% | Memory Usage:   3.77 GB | Thread Count: 159
Runtime:   11s | Rate:   459844 rows/s | Total:   5360000 rows | Queue: 142 items | CPU Usage: 1616.46% | Memory Usage:   3.80 GB | Thread Count: 159
Runtime:   12s | Rate:   519822 rows/s | Total:   5880000 rows | Queue: 142 items | CPU Usage: 1580.48% | Memory Usage:   3.83 GB | Thread Count: 159
Runtime:   13s | Rate:   479865 rows/s | Total:   6360000 rows | Queue: 142 items | CPU Usage: 1551.52% | Memory Usage:   3.85 GB | Thread Count: 159
Runtime:   14s | Rate:   439838 rows/s | Total:   6800000 rows | Queue: 142 items | CPU Usage: 1613.42% | Memory Usage:   3.87 GB | Thread Count: 159
Runtime:   15s | Rate:   569806 rows/s | Total:   7370000 rows | Queue: 142 items | CPU Usage: 1575.46% | Memory Usage:   3.89 GB | Thread Count: 159
Runtime:   16s | Rate:   449847 rows/s | Total:   7820000 rows | Queue: 141 items | CPU Usage: 1596.44% | Memory Usage:   3.92 GB | Thread Count: 159
Runtime:   17s | Rate:   479833 rows/s | Total:   8300000 rows | Queue: 142 items | CPU Usage: 1486.51% | Memory Usage:   3.95 GB | Thread Count: 159
Runtime:   18s | Rate:   509849 rows/s | Total:   8810000 rows | Queue: 142 items | CPU Usage: 1572.49% | Memory Usage:   3.98 GB | Thread Count: 159
Runtime:   19s | Rate:   559798 rows/s | Total:   9370000 rows | Queue: 141 items | CPU Usage: 1599.47% | Memory Usage:   4.01 GB | Thread Count: 159
Runtime:   20s | Rate:   489843 rows/s | Total:   9860000 rows | Queue: 141 items | CPU Usage: 1551.49% | Memory Usage:   4.02 GB | Thread Count: 159
Runtime:   21s | Rate:   609791 rows/s | Total:  10470000 rows | Queue: 141 items | CPU Usage: 1579.47% | Memory Usage:   4.05 GB | Thread Count: 159
Runtime:   22s | Rate:   509829 rows/s | Total:  10980000 rows | Queue: 141 items | CPU Usage: 1606.47% | Memory Usage:   4.07 GB | Thread Count: 159
Runtime:   23s | Rate:   629787 rows/s | Total:  11610000 rows | Queue: 142 items | CPU Usage: 1606.44% | Memory Usage:   4.11 GB | Thread Count: 159
Runtime:   24s | Rate:   739739 rows/s | Total:  12350000 rows | Queue: 142 items | CPU Usage: 1593.45% | Memory Usage:   4.12 GB | Thread Count: 159
Runtime:   25s | Rate:   569810 rows/s | Total:  12920000 rows | Queue: 142 items | CPU Usage: 1639.46% | Memory Usage:   4.14 GB | Thread Count: 159
Runtime:   26s | Rate:   659784 rows/s | Total:  13580000 rows | Queue: 142 items | CPU Usage: 1557.47% | Memory Usage:   4.16 GB | Thread Count: 159
Runtime:   27s | Rate:   639762 rows/s | Total:  14220000 rows | Queue: 142 items | CPU Usage: 1636.36% | Memory Usage:   4.17 GB | Thread Count: 159
Runtime:   28s | Rate:   669755 rows/s | Total:  14890000 rows | Queue: 142 items | CPU Usage: 1625.46% | Memory Usage:   4.19 GB | Thread Count: 159
Runtime:   29s | Rate:   729761 rows/s | Total:  15620000 rows | Queue: 140 items | CPU Usage: 1615.43% | Memory Usage:   4.21 GB | Thread Count: 159
Runtime:   30s | Rate:   629763 rows/s | Total:  16250000 rows | Queue: 140 items | CPU Usage: 1632.43% | Memory Usage:   4.22 GB | Thread Count: 159
Runtime:   31s | Rate:   809738 rows/s | Total:  17060000 rows | Queue: 141 items | CPU Usage: 1663.44% | Memory Usage:   4.25 GB | Thread Count: 159
Runtime:   32s | Rate:   769733 rows/s | Total:  17830000 rows | Queue: 140 items | CPU Usage: 1579.47% | Memory Usage:   4.27 GB | Thread Count: 159
Runtime:   33s | Rate:   939689 rows/s | Total:  18770000 rows | Queue: 141 items | CPU Usage: 1609.46% | Memory Usage:   4.28 GB | Thread Count: 159
Runtime:   34s | Rate:   899692 rows/s | Total:  19670000 rows | Queue: 139 items | CPU Usage: 1606.44% | Memory Usage:   4.30 GB | Thread Count: 159
Runtime:   35s | Rate:   979668 rows/s | Total:  20650000 rows | Queue: 141 items | CPU Usage: 1650.46% | Memory Usage:   4.32 GB | Thread Count: 159
Runtime:   36s | Rate:  1069637 rows/s | Total:  21720000 rows | Queue: 142 items | CPU Usage: 1701.42% | Memory Usage:   4.34 GB | Thread Count: 159
Runtime:   37s | Rate:  1169605 rows/s | Total:  22890000 rows | Queue: 142 items | CPU Usage: 1685.40% | Memory Usage:   4.35 GB | Thread Count: 159
Runtime:   38s | Rate:  1109604 rows/s | Total:  24000000 rows | Queue: 141 items | CPU Usage: 1726.41% | Memory Usage:   4.38 GB | Thread Count: 159
Runtime:   39s | Rate:  1079627 rows/s | Total:  25080000 rows | Queue: 139 items | CPU Usage: 1618.49% | Memory Usage:   4.40 GB | Thread Count: 159
Runtime:   40s | Rate:  1159654 rows/s | Total:  26240000 rows | Queue: 140 items | CPU Usage: 1712.45% | Memory Usage:   4.42 GB | Thread Count: 159
Runtime:   41s | Rate:  1349503 rows/s | Total:  27590000 rows | Queue: 140 items | CPU Usage: 1705.39% | Memory Usage:   4.43 GB | Thread Count: 159
Runtime:   42s | Rate:  1229580 rows/s | Total:  28820000 rows | Queue: 137 items | CPU Usage: 1642.40% | Memory Usage:   4.45 GB | Thread Count: 159
Runtime:   43s | Rate:  1219569 rows/s | Total:  30040000 rows | Queue: 139 items | CPU Usage: 1656.42% | Memory Usage:   4.48 GB | Thread Count: 159
Runtime:   44s | Rate:  1409514 rows/s | Total:  31450000 rows | Queue: 141 items | CPU Usage: 1686.43% | Memory Usage:   4.50 GB | Thread Count: 159
Runtime:   45s | Rate:  1489499 rows/s | Total:  32940000 rows | Queue: 141 items | CPU Usage: 1770.42% | Memory Usage:   4.52 GB | Thread Count: 159
Runtime:   46s | Rate:  1489404 rows/s | Total:  34430000 rows | Queue: 139 items | CPU Usage: 1665.30% | Memory Usage:   4.54 GB | Thread Count: 159
Runtime:   47s | Rate:  1499473 rows/s | Total:  35930000 rows | Queue: 133 items | CPU Usage: 1731.37% | Memory Usage:   4.55 GB | Thread Count: 159
Runtime:   48s | Rate:  1749284 rows/s | Total:  37680000 rows | Queue: 137 items | CPU Usage: 1793.36% | Memory Usage:   4.58 GB | Thread Count: 159
Runtime:   49s | Rate:  1619533 rows/s | Total:  39300000 rows | Queue: 138 items | CPU Usage: 1684.47% | Memory Usage:   4.60 GB | Thread Count: 159
Runtime:   50s | Rate:  1899044 rows/s | Total:  41200000 rows | Queue: 141 items | CPU Usage: 1790.11% | Memory Usage:   4.61 GB | Thread Count: 159
Runtime:   51s | Rate:  1779383 rows/s | Total:  42980000 rows | Queue: 134 items | CPU Usage: 1787.39% | Memory Usage:   4.62 GB | Thread Count: 159
Runtime:   52s | Rate:  1769486 rows/s | Total:  44750000 rows | Queue: 139 items | CPU Usage: 1710.49% | Memory Usage:   4.63 GB | Thread Count: 159
Runtime:   53s | Rate:  1799385 rows/s | Total:  46550000 rows | Queue: 139 items | CPU Usage: 1747.41% | Memory Usage:   4.65 GB | Thread Count: 159
Runtime:   54s | Rate:  1709462 rows/s | Total:  48260000 rows | Queue: 138 items | CPU Usage: 1637.41% | Memory Usage:   4.66 GB | Thread Count: 159
Runtime:   55s | Rate:  1859307 rows/s | Total:  50120000 rows | Queue: 141 items | CPU Usage: 1792.45% | Memory Usage:   4.67 GB | Thread Count: 159
Runtime:   56s | Rate:  1919360 rows/s | Total:  52040000 rows | Queue: 140 items | CPU Usage: 1766.35% | Memory Usage:   4.68 GB | Thread Count: 159
Runtime:   57s | Rate:  2059299 rows/s | Total:  54100000 rows | Queue: 142 items | CPU Usage: 1812.39% | Memory Usage:   4.68 GB | Thread Count: 159
Runtime:   58s | Rate:  1909201 rows/s | Total:  56010000 rows | Queue: 137 items | CPU Usage: 1756.24% | Memory Usage:   4.69 GB | Thread Count: 159
Runtime:   59s | Rate:  2199151 rows/s | Total:  58210000 rows | Queue: 140 items | CPU Usage: 1872.28% | Memory Usage:   4.70 GB | Thread Count: 159
Runtime:   60s | Rate:  2009279 rows/s | Total:  60220000 rows | Queue: 140 items | CPU Usage: 1701.44% | Memory Usage:   4.70 GB | Thread Count: 159
Runtime:   61s | Rate:  2179370 rows/s | Total:  62400000 rows | Queue: 140 items | CPU Usage: 1810.42% | Memory Usage:   4.71 GB | Thread Count: 159
Runtime:   62s | Rate:  2169250 rows/s | Total:  64570000 rows | Queue: 137 items | CPU Usage: 1817.43% | Memory Usage:   4.71 GB | Thread Count: 159
Runtime:   63s | Rate:  2259278 rows/s | Total:  66830000 rows | Queue: 141 items | CPU Usage: 1836.37% | Memory Usage:   4.72 GB | Thread Count: 159
Runtime:   64s | Rate:  2089277 rows/s | Total:  68920000 rows | Queue: 141 items | CPU Usage: 1732.40% | Memory Usage:   4.72 GB | Thread Count: 159
Runtime:   65s | Rate:  2239278 rows/s | Total:  71160000 rows | Queue: 139 items | CPU Usage: 1742.46% | Memory Usage:   4.72 GB | Thread Count: 159
Runtime:   66s | Rate:  2119369 rows/s | Total:  73280000 rows | Queue: 139 items | CPU Usage: 1667.51% | Memory Usage:   4.73 GB | Thread Count: 159
Runtime:   67s | Rate:  2289233 rows/s | Total:  75570000 rows | Queue: 140 items | CPU Usage: 1803.40% | Memory Usage:   4.73 GB | Thread Count: 159
Runtime:   68s | Rate:  2299282 rows/s | Total:  77870000 rows | Queue: 142 items | CPU Usage: 1805.41% | Memory Usage:   4.73 GB | Thread Count: 159
Runtime:   69s | Rate:  2269263 rows/s | Total:  80140000 rows | Queue: 132 items | CPU Usage: 1821.39% | Memory Usage:   4.74 GB | Thread Count: 159
Runtime:   70s | Rate:  2338806 rows/s | Total:  82480000 rows | Queue: 139 items | CPU Usage: 1813.13% | Memory Usage:   4.74 GB | Thread Count: 159
Runtime:   71s | Rate:  2179345 rows/s | Total:  84660000 rows | Queue: 140 items | CPU Usage: 1708.49% | Memory Usage:   4.74 GB | Thread Count: 159
Runtime:   72s | Rate:  2279344 rows/s | Total:  86940000 rows | Queue: 136 items | CPU Usage: 1811.39% | Memory Usage:   4.74 GB | Thread Count: 159
Runtime:   73s | Rate:  2269129 rows/s | Total:  89210000 rows | Queue: 141 items | CPU Usage: 1747.37% | Memory Usage:   4.74 GB | Thread Count: 159
Runtime:   74s | Rate:  2389102 rows/s | Total:  91600000 rows | Queue: 138 items | CPU Usage: 1805.33% | Memory Usage:   4.75 GB | Thread Count: 159
Runtime:   75s | Rate:  2369235 rows/s | Total:  93970000 rows | Queue: 139 items | CPU Usage: 1820.40% | Memory Usage:   4.75 GB | Thread Count: 159
Runtime:   76s | Rate:  2399252 rows/s | Total:  96370000 rows | Queue: 136 items | CPU Usage: 1855.43% | Memory Usage:   4.75 GB | Thread Count: 159
Runtime:   77s | Rate:  2279208 rows/s | Total:  98650000 rows | Queue: 139 items | CPU Usage: 1726.39% | Memory Usage:   4.75 GB | Thread Count: 159
Runtime:   78s | Rate:  2339180 rows/s | Total: 100990000 rows | Queue: 136 items | CPU Usage: 1819.45% | Memory Usage:   4.75 GB | Thread Count: 159
Runtime:   79s | Rate:  2389351 rows/s | Total: 103380000 rows | Queue: 138 items | CPU Usage: 1811.41% | Memory Usage:   4.75 GB | Thread Count: 159
Runtime:   80s | Rate:  2389131 rows/s | Total: 105770000 rows | Queue: 141 items | CPU Usage: 1830.39% | Memory Usage:   4.75 GB | Thread Count: 159
Runtime:   81s | Rate:  2409216 rows/s | Total: 108180000 rows | Queue: 139 items | CPU Usage: 1843.35% | Memory Usage:   4.75 GB | Thread Count: 159
Runtime:   82s | Rate:  2329148 rows/s | Total: 110510000 rows | Queue: 141 items | CPU Usage: 1780.38% | Memory Usage:   4.75 GB | Thread Count: 159
Runtime:   83s | Rate:  2369190 rows/s | Total: 112880000 rows | Queue: 136 items | CPU Usage: 1828.35% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   84s | Rate:  2379163 rows/s | Total: 115260000 rows | Queue: 140 items | CPU Usage: 1762.39% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   85s | Rate:  2379182 rows/s | Total: 117640000 rows | Queue: 141 items | CPU Usage: 1789.44% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   86s | Rate:  2439318 rows/s | Total: 120080000 rows | Queue: 139 items | CPU Usage: 1832.49% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   87s | Rate:  2469323 rows/s | Total: 122550000 rows | Queue: 139 items | CPU Usage: 1802.50% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   88s | Rate:  2359300 rows/s | Total: 124910000 rows | Queue: 135 items | CPU Usage: 1770.39% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   89s | Rate:  2479061 rows/s | Total: 127390000 rows | Queue: 137 items | CPU Usage: 1797.34% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   90s | Rate:  2479126 rows/s | Total: 129870000 rows | Queue: 142 items | CPU Usage: 1816.38% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   91s | Rate:  2399114 rows/s | Total: 132270000 rows | Queue: 141 items | CPU Usage: 1767.31% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   92s | Rate:  2398932 rows/s | Total: 134670000 rows | Queue: 139 items | CPU Usage: 1754.27% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   93s | Rate:  2319205 rows/s | Total: 136990000 rows | Queue: 133 items | CPU Usage: 1755.39% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   94s | Rate:  2479157 rows/s | Total: 139470000 rows | Queue: 141 items | CPU Usage: 1793.33% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   95s | Rate:  2409059 rows/s | Total: 141880000 rows | Queue: 138 items | CPU Usage: 1787.33% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   96s | Rate:  2449041 rows/s | Total: 144330000 rows | Queue: 139 items | CPU Usage: 1822.32% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   97s | Rate:  2429191 rows/s | Total: 146760000 rows | Queue: 137 items | CPU Usage: 1807.39% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   98s | Rate:  2489140 rows/s | Total: 149250000 rows | Queue: 139 items | CPU Usage: 1856.36% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:   99s | Rate:  2449135 rows/s | Total: 151700000 rows | Queue: 138 items | CPU Usage: 1811.38% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  100s | Rate:  2419238 rows/s | Total: 154120000 rows | Queue: 142 items | CPU Usage: 1810.41% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  101s | Rate:  2459169 rows/s | Total: 156580000 rows | Queue: 138 items | CPU Usage: 1836.38% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  102s | Rate:  2399137 rows/s | Total: 158980000 rows | Queue: 135 items | CPU Usage: 1759.34% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  103s | Rate:  2499031 rows/s | Total: 161480000 rows | Queue: 141 items | CPU Usage: 1836.38% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  104s | Rate:  2411633 rows/s | Total: 163900000 rows | Queue: 134 items | CPU Usage: 1776.78% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  105s | Rate:  2399142 rows/s | Total: 166300000 rows | Queue: 136 items | CPU Usage: 1759.39% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  106s | Rate:  2459180 rows/s | Total: 168760000 rows | Queue: 139 items | CPU Usage: 1770.42% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  107s | Rate:  2399170 rows/s | Total: 171160000 rows | Queue: 140 items | CPU Usage: 1795.36% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  108s | Rate:  2438736 rows/s | Total: 173600000 rows | Queue: 140 items | CPU Usage: 1790.07% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  109s | Rate:  2399130 rows/s | Total: 176000000 rows | Queue: 139 items | CPU Usage: 1763.41% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  110s | Rate:  2389351 rows/s | Total: 178390000 rows | Queue: 139 items | CPU Usage: 1776.52% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  111s | Rate:  2459341 rows/s | Total: 180850000 rows | Queue: 137 items | CPU Usage: 1831.45% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  112s | Rate:  2149205 rows/s | Total: 183000000 rows | Queue: 141 items | CPU Usage: 1626.44% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  113s | Rate:  1369576 rows/s | Total: 184370000 rows | Queue: 138 items | CPU Usage: 1066.60% | Memory Usage:   4.76 GB | Thread Count: 159
Runtime:  114s | Rate:  1939141 rows/s | Total: 186310000 rows | Queue: 138 items | CPU Usage: 1474.41% | Memory Usage:   4.76 GB | Thread Count: 158
Runtime:  115s | Rate:  2348921 rows/s | Total: 188660000 rows | Queue: 140 items | CPU Usage: 1715.24% | Memory Usage:   4.76 GB | Thread Count: 158
Runtime:  116s | Rate:  2139277 rows/s | Total: 190800000 rows | Queue: 141 items | CPU Usage: 1627.41% | Memory Usage:   4.76 GB | Thread Count: 158
Runtime:  117s | Rate:  2289103 rows/s | Total: 193090000 rows | Queue: 140 items | CPU Usage: 1688.38% | Memory Usage:   4.76 GB | Thread Count: 157
Runtime:  118s | Rate:  2199186 rows/s | Total: 195290000 rows | Queue: 142 items | CPU Usage: 1650.35% | Memory Usage:   4.76 GB | Thread Count: 157
Runtime:  119s | Rate:  2229179 rows/s | Total: 197520000 rows | Queue: 142 items | CPU Usage: 1667.41% | Memory Usage:   4.76 GB | Thread Count: 154
Runtime:  120s | Rate:  2119253 rows/s | Total: 199640000 rows | Queue: 135 items | CPU Usage: 1668.43% | Memory Usage:   4.76 GB | Thread Count: 146
Runtime:  121s | Rate:   359877 rows/s | Total: 200000000 rows | Queue:   0 items | CPU Usage: 1049.64% | Memory Usage:   4.76 GB | Thread Count: 143
=============================================== Insert Summary Statistics ====================================================
Insert Threads: 16
Total Rows: 200000000
Total Duration: 118.51 seconds
Average Rate: 1687617.88 rows/second
==============================================================================================================================

=============================================== Insert Latency & Efficiency Metrics ==========================================
Total Operations: 20000
Total Duration: 118.51 seconds
Pure Insert Latency: 118.41 seconds
Effective Time Ratio: 99.91%
Framework Overhead: 0.06%
Idle Time After Finish: 0.03 seconds
Write Latency Distribution: min: 33.9291ms, avg: 94.7256ms, p90: 186.1221ms, p95: 286.9818ms, p99: 406.6854ms, max: 972.6083ms
==============================================================================================================================
```
