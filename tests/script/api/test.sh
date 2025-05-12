#!/bin/bash

make clean

make

pgrep taosd || taosd >> /dev/null 2>&1 &

sleep 10

./dbTableRoute localhost
./batchprepare localhost
./stmt-crash localhost
./insertSameTs localhost
./passwdTest localhost
./whiteListTest localhost
./tmqViewTest

