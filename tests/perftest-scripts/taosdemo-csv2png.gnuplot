#!/user/bin/gnuplot
reset
set terminal png

set title "TaosDemo Performance Report" font ",20"

set ylabel "Time in Seconds"

set xdata time
set timefmt "%Y%m%d"
set format x "%Y-%m-%d"
set xlabel "Date"

set style data linespoints

set terminal pngcairo size 1024,768 enhanced font 'Segoe UI, 10'
set output 'taosdemo-report.png'
set datafile separator ','

set key reverse Left outside
set grid


plot 'taosdemo-report.csv' 	using 1:2  title "Create 10,000 Table", \
	""			using 1:3 title "Create 10,000 Table and Insert 100,000 data", \
	""			using 1:4 title "Request Per Second of Insert 100,000 data"
