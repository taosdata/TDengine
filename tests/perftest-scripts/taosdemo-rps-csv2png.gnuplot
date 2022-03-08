#!/user/bin/gnuplot
reset
set terminal png

set title filename font ",20"

set ylabel "Time in Seconds"

set xdata time
set timefmt "%Y%m%d"
set format x "%Y-%m-%d"
set xlabel "Date"

set style data linespoints

set terminal pngcairo size 1024,768 enhanced font 'Segoe UI, 10'
set output filename . '.png'
set datafile separator ','

set key reverse Left outside
set grid


plot filename . '.csv' 	using 1:2  title "Request Per Second"
