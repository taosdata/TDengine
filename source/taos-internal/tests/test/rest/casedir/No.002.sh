#!/bin/bash
ignoreTime=1
rm ./No.002.rpt
bash ../httpRun.sh No.002 notGet
bash ../httpRun.sh No.002 notPut
bash ../httpRun.sh No.002 notDelete
bash ../httpRun.sh No.002 urlNotSql
bash ../httpRun.sh No.002 urlDbtooLong
bash ../httpRun.sh No.002 pathNotStartwithSql
bash ../httpRun.sh No.002 urltooLong
bash ../httpRun.sh No.002 pathMultiDelimiter
bash ../httpRun.sh No.002 cookietooLong ${ignoreTime}
bash ../httpRun.sh No.002 withcookie ${ignoreTime}
bash ../httpRun.sh No.002 withcookieWithoutToken
bash ../httpRun.sh No.002 withtokenWithoutCookie ${ignoreTime}
bash ../httpRun.sh No.002 withoutcookieandtoken ${ignoreTime}
bash ../httpRun.sh No.002 cookieinvalid
bash ../httpRun.sh No.002 hugebody ${ignoreTime}
bash ../httpRun.sh No.002 tokeninvalid
bash ../httpRun.sh No.002 sqliszero

