@echo off 
set LoopNumner=%1
set cmd=%2

echo "Loop Number: %LoopNumner%"
echo "command : %cmd:"=%"

For /l %%i in (1,1,%LoopNumner% ) do (
    echo "execute times:"%%i
    %cmd:"=%
)

% case loop.bat 100 "python3 .\test.py -f 2-query\query_cols_tags_and_or.py" %