#!/usr/bin/env python3

import sys

if len(sys.argv) < 3:
    print("Format: ./findBreak.py fname col_number")
    sys.exit()

fname = sys.argv[1]
col_num = int(sys.argv[2])

f = open(fname, 'r')

last = int(f.readline().split()[col_num-1])

ln1 = 1
ln2 = 2

line = f.readline()
while line != "":
    current = int(line.split()[col_num-1])
    if abs(current - last) != 1:
        print("Target: line {} and line {}".format(ln1, ln2))
        break

    last = current
    ln1 += 1
    ln2 += 1
    line = f.readline()

