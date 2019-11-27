sourceDir=/home/ubuntu/fpan/workspace/TDinternal
cppcheck --enable=all --output-file=static_check_report -rp=$sourceDir $sourceDir 
echo "static check result is in the report file"
