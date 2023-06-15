use test;
explain verbose true select _wstart, last(ts), avg(c2) from meters interval(10s) order by _wstart desc\G;
explain verbose true select _wstart, last(ts), avg(c2) from meters interval(10s) order by _wstart asc\G;
explain verbose true select _wstart, first(ts), avg(c2) from meters interval(10s) order by _wstart asc\G;
explain verbose true select _wstart, first(ts), avg(c2) from meters interval(10s) order by _wstart desc\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s)) order by d\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s)) order by d desc\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a) order by d\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) order by d\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a) order by d desc\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) order by d desc\G;


explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) order by d\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) order by d\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) order by d desc\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) order by d desc\G;

explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) group by c order by d\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) group by c order by d\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) group by c order by d desc\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) group by c order by d desc\G;


explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) where a > 10000 and a < 20000 interval(10s) fill(NULL) order by d\G;
explain verbose true select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) where a > 10000 and a < 20000 interval(10s) fill(NULL) order by d\G;
explain verbose true select last(b) as d from (select last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) where b > 10000 and b < 20000 interval(10s) fill(NULL) order by d\G;
explain verbose true select last(b) as d from (select last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) where b > 10000 and b < 20000 interval(10s) fill(NULL) order by d desc\G;


select _wstart, last(ts), avg(c2) from meters interval(10s) order by _wstart desc;
select _wstart, last(ts), avg(c2) from meters interval(10s) order by _wstart asc;
select _wstart, first(ts), avg(c2) from meters interval(10s) order by _wstart asc;
select _wstart, first(ts), avg(c2) from meters interval(10s) order by _wstart desc;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s)) order by d;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s)) order by d desc;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a) order by d;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) order by d;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a) order by d desc;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) order by d desc;


select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) order by d;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) order by d;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) order by d desc;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) order by d desc;

select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) group by c order by d
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) group by c order by d;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b) group by c order by d desc;
select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) group by c order by d desc;

select last(a) as d from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) where a > '2022-05-15 00:01:00.000' and a < '2022-05-19 00:01:08.000' interval(10s) order by d;
select last(b) as d from (select last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) where b > '2022-05-15 00:01:00.000' and b < '2022-05-19 00:01:08.000' interval(10s) order by d;
select last(b) as d from (select last(ts) as b, avg(c2) as c from meters interval(10s) order by b desc) where b > '2022-05-15 00:01:00.000' and b < '2022-05-19 00:01:08.000' interval(10s) order by d desc;
select _wstart, first(a) as d, avg(c) from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) where a > '2022-05-15 00:01:00.000' and a < '2022-05-21 00:01:08.000' interval(5h) fill(linear) order by d desc;

explain verbose true select _wstart, first(a) as d, avg(c) from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a desc) where a > '2022-05-15 00:01:00.000' and a < '2022-05-21 00:01:08.000' interval(5h) fill(linear) order by d desc\G;
explain verbose true select _wstart, first(a) as d, avg(c) from (select _wstart as a, last(ts) as b, avg(c2) as c from meters interval(10s) order by a asc) where a > '2022-05-15 00:01:00.000' and a < '2022-05-21 00:01:08.000' interval(5h) fill(linear) order by d desc\G;

explain verbose true select * from (select ts as a, c2 as b from meters order by c2 desc)\G;
select * from (select ts as a, c2 as b from meters order by c2 desc);

explain verbose true select * from (select ts as a, c2 as b from meters order by c2 desc) order by a desc\G;
select * from (select ts as a, c2 as b from meters order by c2 desc) order by a desc;

explain verbose true select a.ts, a.c2, b.c2 from meters as a, meters as b where a.ts = b.ts\G;
explain verbose true select a.ts, a.c2, b.c2 from meters as a, meters as b where a.ts = b.ts order by a.ts\G;
select a.ts, a.c2, b.c2 from meters as a, meters as b where a.ts = b.ts;
select a.ts, a.c2, b.c2 from meters as a, meters as b where a.ts = b.ts order by a.ts desc;
explain verbose true select a.ts, a.c2, b.c2 from meters as a, (select ts, c2 from meters order by ts desc) b where a.ts = b.ts order by a.ts desc\G;
explain verbose true select a.ts, a.c2, b.c2 from meters as a, (select ts, c2 from meters order by ts desc) b where a.ts = b.ts order by a.ts asc\G;
select a.ts, a.c2, b.c2 from meters as a, (select * from meters order by ts desc) b where a.ts = b.ts order by a.ts asc;
