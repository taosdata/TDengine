# INTERP 支持BOOL类型需求设计文档

用户在3.0版本跟2.6版本分别提出了对INTERP支持BOOL类型的需求：
 
TS-2815


TS-2754

根据用户需求，设计INTERP对BOOL类型支持的插值行为如下：
1. 使用FILL(NULL)，FILL(PREV)， FILL(NEXT)插值时，行为与其他类型的列插值行为保持一致。
2. 使用FILL(VALUE, xxx)插值时，具体value的值为0/false/NULL，以及可以转换为0(比如"0")和不能转换为数值类型的字符串(比如"abc")时，插值结果为false，其他情况插值结果为true。
3. 使用FILL(LINEAR)插值时，因为布尔类型只有0/1二进制，因此规定
   - 当插值点的前后数据点有NULL值的时候，插值结果为NULL值(与其他类型一致)。
   - 当插值点的前后缺少有效数据点时，不产生插值结果(与其他类型一致)。
   - 当插值点的前后数据点均为false时，插值结果为false。
   - ~~当插值点的前后数据点有true时，比如true/true, true/false, false/true，插值结果为true。~~
   - 当插值点的前后数据点均为true时，插值结果为true。
   - 比如true/false, false/true，插值结果为false，与int类型的行为保持一致，不再添加配置项：
  ```sql
  taos> select * from tb;
   ts | c0 |
  ========================================
   2023-04-03 00:00:01.000 | 1 |
   2023-04-03 00:00:02.000 | 2 |
   2023-04-03 00:00:03.000 | NULL |
   2023-04-03 00:00:04.000 | 1 |
   2023-04-03 00:00:05.000 | 0 |
   2023-04-03 00:00:06.000 | 1 |
  Query OK, 6 row(s) in set (0.003977s)
  
  taos> select _irowts,interp(c0) from tb where c0 is not NULL range('2023-04-03 00:00:04.000', '2023-04-03 00:00:05.000') every(500a) fill(linear);
   _irowts | interp(c0) |
  ========================================
   2023-04-03 00:00:04.000 | 1 |
   2023-04-03 00:00:04.500 | 0 |
   2023-04-03 00:00:05.000 | 0 |
  Query OK, 3 row(s) in set (0.005482s)
  
  taos> select _irowts,interp(c0) from tb where c0 is not NULL range('2023-04-03 00:00:05.000', '2023-04-03 00:00:06.000') every(500a) fill(linear);
   _irowts | interp(c0) |
  ========================================
   2023-04-03 00:00:05.000 | 0 |
   2023-04-03 00:00:05.500 | 0 |
   2023-04-03 00:00:06.000 | 1 |
  Query OK, 3 row(s) in set (0.003130s)
  
  taos>
  ```
