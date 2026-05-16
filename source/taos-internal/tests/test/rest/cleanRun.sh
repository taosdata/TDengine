#!/bin/bash
caseNum=$1
find casedir/$caseNum/runlog -name "*.log" |xargs rm
find casedir -name "$caseNum.rpt" |xargs rm
