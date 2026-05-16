# [Test Report] TD-25620 partition by column + slimit/limit优化

## 一、需求

   partition by 进行分组聚合查询，增加限制组数量的 slimit 语句后，查询仍然很慢，需要改进性能，如：
       select count(*), c0 from meters partition by c0 slimit 10;

## 二、需求预期

    优化后性能略有提升

## 三、测试方案

    选择一个参照版本，选用最近一个发布的版本 3.1.1.5 与当前改进后的版本进行对比测试

## 四、测试步骤

     1、  使用服务器 192.168.0.214
     2、 下载 3.1.1.5 版本代码并编译安装
     3、  使用 taosBenchmark 写入测试数据
              taosBenchmark -t 50000 -n 10000 -v 4 -y
     4、  taos-CLI 中运行各测试 sql ，记录每个 sql 耗时
     5、 在切换到改进后的 3.0 分支最新代码，编译出相应的 taosd
     6、 步骤 3 中生成的数据文件复制到 3.0 最新代码相应的目录下，做为测试数据集
     7、 在 taos-CLI 中运行各测试 sql, 记录每个 sql 耗时
    记录结果如下：
   数据集：  5W子表， 每子表 1 W数据，共 5亿数据量
    partition by voltage 共 99 个值，即 99 组

| 序号 | SQL | 改进前耗时 | 改进后耗时 |
| --- | --- | --- | --- |
|  | select count(*) from meters; | 0.20s | 0.13s |
| 1 | select count(*),voltage from meters partition by voltage; | 46.5s | 46.49s |
| 2 | select count(*),voltage from meters partition by voltage slimit 1; | 45.8s | 44.2s |
| 3 | select count(*),voltage from meters partition by voltage slimit 10; | 48.7s | 42.5s |
| 4 | select count(*),voltage from meters partition by voltage interval(1s); | 48.5s | 57.6s |
| 5 | select count(*),voltage from meters partition by voltage interval(1s) slimit 5; | 44.8s | 42.8s |

  结论：
       1） 带 SLIMIT 优化验证的 SQL 略有提升，提升幅度不大，这个和开发测试的预期一致。
       2）第四项耗时升高，反复测试几次确实是升高了，因为没带 SLIMIT ，所以和本次无关，单独建立 JIRA 来跟踪。

## 五、测试结论

      测试结果与预期一致，测试通过。
