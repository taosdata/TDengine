rm -rf %WIN_TDENGINE_ROOT_DIR%\debug
mkdir %WIN_TDENGINE_ROOT_DIR%\debug
mkdir %WIN_TDENGINE_ROOT_DIR%\debug\build
mkdir %WIN_TDENGINE_ROOT_DIR%\debug\build\bin
xcopy C:\TDengine\taos*.exe %WIN_TDENGINE_ROOT_DIR%\debug\build\bin

set case_out_file=%cd%\case.out

cd %WIN_TDENGINE_ROOT_DIR%\tests\system-test
python3 .\test.py -f 0-others\taosShell.py
python3 .\test.py -f 6-cluster\5dnode3mnodeSep1VnodeStopDnodeModifyMeta.py  -N 6 -M 3