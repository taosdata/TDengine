#!/bin/bash

taosBenchmark -f prepare.json
taosBenchmark -f prepare1.json
taosBenchmark -f prepare2.json
taosBenchmark -f prepare3.json


taosBenchmark -f normal1.json
taosBenchmark -f normal2.json
taosBenchmark -f normal3.json
taosBenchmark -f normalNull.json
