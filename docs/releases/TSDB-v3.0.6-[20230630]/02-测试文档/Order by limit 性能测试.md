# Order by limit 性能测试

## 1. 数据准备

taosBenchmark 生成的1亿数据，分别是1w子表，每个表1万数据。
```sql
describe meters;
             field              |         type         |   length    |   note   |
=================================================================================
 ts                             | TIMESTAMP            |           8 |          |
 current                        | FLOAT                |           4 |          |
 voltage                        | INT                  |           4 |          |
 phase                          | FLOAT                |           4 |          |
 groupid                        | INT                  |           4 | TAG      |
 location                       | VARCHAR              |          24 | TAG      |
Query OK, 6 row(s) in set (0.002670s)

```


## 2. 对比版本

场景1:
Main 和 3.0最新，主要对比两个版本的sql时间

场景2:
3.0同版本，因为牵涉到硬盘和内存查询的优化，主要对比limit 后面的n值变化的时间变化

## 3. 场景1:对比语句

选择了ts、float、int、int（tag）、varchar（tag）进行对比，记录一下时间
```plaintext
  select count(*) from db_10000w.meters ;         
  sql0: select * from db_10000w.meters;
  sql1: select * from db_10000w.meters order by ts limit 10;
  sql2: select * from db_10000w.meters order by current limit 10;
  sql3: select * from db_10000w.meters order by voltage limit 10;
  sql4: select * from db_10000w.meters order by groupid limit 10;
  sql5: select * from db_10000w.meters order by location limit 10;

  sql6: select * from db_10000w.meters order by ts desc limit 10;
  sql7: select * from db_10000w.meters order by current desc limit 10;
  sql8: select * from db_10000w.meters order by voltage desc limit 10;
  sql9: select * from db_10000w.meters order by groupid desc limit 10;
  sql10: select * from db_10000w.meters order by location desc limit 10;
      
```

## 4. 场景1:对比结果及结论

Select * 和select ts 无提升，符合此次修改。
非ts提升明显，差不多都是130-150s提升到22-25s之间，比ts都快了1倍了。
疑问：ts列是否有提升空间？按理说ts应该更快些更合理，或者能和非ts都一样？

| sql： | main（s） | 3.0（s） | 备注 |
| --- | --- | --- | --- |
| sql0: | (48.566080s) | (46.945153s) |  |
| sql1: | (49.151729s) | (48.453028s) | ts查询变化不大 |
| sql2: | (144.638438s) | (23.417157s) |  |
| sql3: | (136.707714s) | (22.387711s) |  |
| sql4： | (133.917745s) | (22.499966s) |  |
| sql5: | (131.336954s) | (23.318787s) |  |
| sql6: | (47.950595s) | (49.287557s) |  |
| sql7: | (147.388587s) | (23.295326s) |  |
| sql8: | (143.307324s) | (22.702467s) |  |
| sql9: | (138.614661s) | (23.395721s) |  |
| sql10: | (135.157278s) | (24.813495s) |  |


## 5. 场景2:对比语句

选择了ts、float、int、int（tag）、varchar（tag）进行对比，记录一下时间
```plaintext
  limit n =1\10\100\1000\10000\100000\1000000\10000000\100000000
  
  sql1: select * from db_10000w.meters order by ts limit 10;
  sql2: select * from db_10000w.meters order by current limit 10;
  sql3: select * from db_10000w.meters order by voltage limit 10;
  sql4: select * from db_10000w.meters order by groupid limit 10;
  sql5: select * from db_10000w.meters order by location limit 10;

  sql6: select * from db_10000w.meters order by ts desc limit 10;
  sql7: select * from db_10000w.meters order by current desc limit 10;
  sql8: select * from db_10000w.meters order by voltage desc limit 10;
  sql9: select * from db_10000w.meters order by groupid desc limit 10;
  sql10: select * from db_10000w.meters order by location desc limit 10;
      
```

## 6. 场景2:对比结果及结论

当limit n<=10w时，各sql查询耗时基本不大，超过10w时，开始上涨，和开发给出的自测报告也吻合。
n=1000w时，出现了一个2410s的结果，后来我复测时还出现了2646s的结果，这个时候公司的网络特别差，不稳定，看来也会影响测试结果。

| sql： | n=1 | n=10 | n=100 | n=1000 | n=10000 | n=10w | n=100w | n=1000w | n=1b | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sql1: | (47.528498s) | (48.453028s) | (47.897273s) | (47.438143s) | (47.779174s) | (52.339324s) | (182.906212s) | (1512.991801s) |  |
| sql2: | (20.172879s) | (23.417157s) | (23.273586s) | (23.198219s) | (23.363317s) | (31.524188s) | (166.107537s) | (1505.950941s) |  |
| sql3: | (19.860466s) | (22.387711s) | (22.739438s) | (22.742801s) | (22.864136s) | (32.724622s) | (141.518916s) | (1489.871592s) |  |
| sql4： | (19.846957s) | (22.499966s) | (22.625267s) | (22.614068s) | (22.930636s) | (30.544588s) | (126.647824s) | (1470.124708s) |  |
| sql5: | (20.286657s) | (23.318787s) | (23.218093s) | (24.851841s) | (25.129410s) | (42.162446s) | (172.268951s) | (1479.845024s) |  |
| sql6: | (49.586169s) | (49.287557s) | (49.032861s) | (49.397910s) | (49.997805s) | (58.351764s) | (196.979606s) | (1460.608690s) |  |
| sql7: | (20.225443s) | (23.295326s) | (23.511564s) | (23.451962s) | (23.757277s) | (33.412262s) | (180.669565s) | (1560.328211s) |  |
| sql8: | (20.050371s) | (22.702467s) | (23.284340s) | (23.171489s) | (23.082669s) | (31.647704s) | (172.137580s) | (1503.731797s) |  |
| sql9: | (20.125378s) | (23.395721s) | (23.303287s) | (23.048362s) | (23.373311s) | (38.424263s) | (172.948121s) | (2410.234111s) | (2646.864106s) |
| sql10: | (20.368180s) | (24.813495s) | (23.866879s) | (24.154821s) | (24.005880s) | (39.228669s) | (175.133125s) | (1571.445112s) |  |
