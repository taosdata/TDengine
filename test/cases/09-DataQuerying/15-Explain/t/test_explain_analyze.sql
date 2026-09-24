use test;
explain analyze verbose true select _wstart, last(ts), avg(c2) from meters interval(10s) order by _wstart desc\G;
explain analyze verbose true select _wstart, last(ts), avg(c2) from meters interval(10s) order by _wstart asc\G;
explain analyze verbose true select _wstart, first(ts), avg(c2) from meters interval(10s) order by _wstart asc\G;
explain analyze verbose true select _wstart, first(ts), avg(c2) from meters interval(10s) order by _wstart desc\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s)) order by d\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s)) order by d desc\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a) order by d\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) order by d\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a) order by d desc\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) order by d desc\G;

explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) order by d\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) order by d\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) order by d desc\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) order by d desc\G;

explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) group by c order by d\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) group by c order by d\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) group by c order by d desc\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) group by c order by d desc\G;

explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) where a > 10000 and a < 20000 interval(10s) fill(NULL) order by d\G;
explain analyze verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) where a > 10000 and a < 20000 interval(10s) fill(NULL) order by d\G;
explain analyze verbose true select last(b) as d from (select last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) where b > 10000 and b < 20000 interval(10s) fill(NULL) order by d\G;
explain analyze verbose true select last(b) as d from (select last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) where b > 10000 and b < 20000 interval(10s) fill(NULL) order by d desc\G;

explain analyze verbose true select _wstart, first(a) as d, avg(c) from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) where a > '2022-05-15 00:01:00.000' and a < '2022-05-21 00:01:08.000' interval(5h) fill(linear) order by d desc\G;
explain analyze verbose true select _wstart, first(a) as d, avg(c) from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a asc) where a > '2022-05-15 00:01:00.000' and a < '2022-05-21 00:01:08.000' interval(5h) fill(linear) order by d desc\G;

explain analyze verbose true select * from (select ts as a, c2 as b from meters order by c2 desc)\G;
explain analyze verbose true select * from (select ts as a, c2 as b from meters order by c2 desc) order by a desc\G;

explain analyze verbose true select a.ts, a.c2, b.c2 from meters as a join meters as b on a.ts = b.ts\G;
explain analyze verbose true select a.ts, a.c2, b.c2 from meters as a join meters as b on a.ts = b.ts order by a.ts\G;
explain analyze verbose true select a.ts, a.c2, b.c2 from meters as a join (select ts, c2 from meters order by ts desc) b on a.ts = b.ts order by a.ts desc\G;
explain analyze verbose true select a.ts, a.c2, b.c2 from meters as a join (select ts, c2 from meters order by ts desc) b on a.ts = b.ts order by a.ts asc\G;

explain analyze verbose true select _wstart, _wend, count(*) from meters event_window start with c2 > 0 end with c2 < 100\G;
explain analyze verbose true select _wstart, _wend, count(*) from meters event_window start with c2 > 0 end with c2 < 100 order by _wstart desc\G;
explain analyze verbose true select _wstart, _wend, count(*) from meters event_window start with c2 > 0 end with c2 < 100 order by _wstart asc\G;

explain analyze verbose true select _wstart, _wend, count(*) from meters event_window start with c2 > 0 end with c2 < 100 order by _wend desc\G;
explain analyze verbose true select _wstart, _wend, count(*) from meters event_window start with c2 > 0 end with c2 < 100 order by _wend asc\G;

explain analyze verbose true select _wstart, _wend, count(*) from meters session(ts, 1h)\G;
explain analyze verbose true select _wstart, _wend, count(*) from meters session(ts, 1h) order by _wstart desc\G;
explain analyze verbose true select _wstart, _wend, count(*) from meters session(ts, 1h) order by _wstart asc\G;

explain analyze verbose true select _wstart, _wend, count(*) from meters session(ts, 1h) order by _wend desc\G;
explain analyze verbose true select _wstart, _wend, count(*) from meters session(ts, 1h) order by _wend asc\G;

explain analyze verbose true select _wstart, _wend, count(*), last(ts) from meters state_window(c2)\G;
explain analyze verbose true select _wstart, _wend, count(*), last(ts) from meters state_window(c2) order by _wstart desc\G;
explain analyze verbose true select _wstart, _wend, count(*), last(ts) from meters state_window(c2) order by _wstart asc\G;

explain analyze verbose true select _wstart, _wend, count(*), last(ts) from meters state_window(c2) order by _wend desc\G;
explain analyze verbose true select _wstart, _wend, count(*), last(ts) from meters state_window(c2) order by _wend asc\G;

explain analyze verbose true select _wstart, _wend, count(*), last(ts) from meters state_window(c2) order by _wend asc, count(*) desc\G;

explain analyze verbose true select _wstart, _wend, last(ts) from (select _wstart as ts, _wend, count(*), last(ts) from meters state_window(c2) order by _wend desc) interval(1h) order by _wstart desc\G;
explain analyze verbose true select _wstart, _wend, last(ts) from (select _wstart as ts, _wend, count(*), last(ts) from meters state_window(c2) order by _wend asc) interval(1h) order by _wstart desc\G;

explain analyze verbose true select * from meters where cc = 'a'\G;
explain analyze verbose true select * from meters where cc != 'a'\G;
explain analyze verbose true select * from meters where cc >= 'a'\G;
explain analyze verbose true select * from meters where cc <= 'a'\G;
explain analyze verbose true select * from meters where cc > 'a'\G;
explain analyze verbose true select * from meters where cc < 'a'\G;
explain analyze verbose true select * from meters where cc like 'a'\G;
explain analyze verbose true select * from meters where cc not like 'a'\G;
explain analyze verbose true select * from meters where cc in ('a')\G;
explain analyze verbose true select * from meters where cc not in ('a')\G;
explain analyze verbose true select * from meters where cc = abs(-1)\G;
explain analyze verbose true select * from meters where cc = concat(cc, 'a')\G;
explain analyze verbose true select * from meters where case when cc > 'a' then 'a' else 'b' end > 'a'\G;
explain analyze verbose true select * from meters where cc + 'a' = 'a'\G;


explain analyze verbose true select * from meters where cc2 = 'a'\G;
explain analyze verbose true select * from meters where cc2 != 'a'\G;
explain analyze verbose true select * from meters where cc2 >= 'a'\G;
explain analyze verbose true select * from meters where cc2 <= 'a'\G;
explain analyze verbose true select * from meters where cc2 > 'a'\G;
explain analyze verbose true select * from meters where cc2 < 'a'\G;
explain analyze verbose true select * from meters where cc2 like 'a'\G;
explain analyze verbose true select * from meters where cc2 not like 'a'\G;
explain analyze verbose true select * from meters where cc2 in ('a')\G;
explain analyze verbose true select * from meters where cc2 not in ('a')\G;
explain analyze verbose true select * from meters where cc2 = abs(-1)\G;
explain analyze verbose true select * from meters where cc2 = concat(cc, 'a')\G;
explain analyze verbose true select * from meters where case when cc2 > 'a' then 'a' else 'b' end > 'a'\G;
explain analyze verbose true select * from meters where cc2 + 'a' = 'a'\G;

explain analyze verbose true select * from meters where cc = 'a' or cc = 'b'\G;
explain analyze verbose true select * from meters where cc = 'a' and cc = 'b'\G;
explain analyze verbose true select * from meters where cc = 'a' and cc != 'b'\G;
explain analyze verbose true select * from meters where cc = 'a' and cc2 = 'b'\G;
explain analyze verbose true select * from meters where cc = 'a' and cc2 != 'b'\G;
explain analyze verbose true select ts, c2 from (select _rowts ts, c2 from d1 where _rowts >= '2022-05-16 00:01:08.000' and _rowts <= '2022-05-22 00:01:08.000' union all select _rowts ts, c2 from d2 where _rowts >= '2022-05-17 00:01:08.000' and _rowts <= '2022-05-23 00:01:08.000') where ts >= '2022-05-18 00:01:08.000' and ts <= '2022-05-21 00:01:08.000'\G;
explain analyze verbose true select ts, c2 from (select _rowts ts, c2 from d1 where _rowts >= '2022-05-16 00:01:08.000' and _rowts <= '2022-05-22 00:01:08.000' union all select _rowts ts, c2 from d2 where _rowts >= '2022-05-17 00:01:08.000' and _rowts <= '2022-05-23 00:01:08.000') where ts >= '2022-05-18 00:01:08.000' and ts <= '2022-05-21 00:01:08.000' order by ts desc limit 3\G;
