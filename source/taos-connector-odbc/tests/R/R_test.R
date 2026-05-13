###############################################################################
# MIT License
#
# Copyright (c) 2022-2023 freemine <freemine@yeah.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

#install.packages("odbc")
#install.packages("assert")

library(DBI)
library(odbc)
library(assert)

getNextTs <- function(t0) {
  repeat {
    t1 <- format.POSIXct(Sys.time(), "%Y-%m-%d %H:%M:%OS3")
    if (t1 != t0) {
      break
    }
  }
  format.POSIXct(Sys.time(), "%Y-%m-%d %H:%M:%OS3")
}

conn <- dbConnect(odbc::odbc(), dsn="TAOS_ODBC_DSN")
assert(0L == dbExecute(conn, "create database if not exists foo"))
dbDisconnect(conn)

conn <- dbConnect(odbc::odbc(), dsn="TAOS_ODBC_DSN", database="foo")
assert(!is.null(conn))

assert(0L == dbExecute(conn, "drop table if exists r_table"))
assert(0L == dbExecute(conn, "create table if not exists r_table (ts timestamp, name varchar(20))"))

t1 <- getNextTs("")
t2 <- getNextTs(t1)

rs <- dbSendStatement(conn, "insert into r_table (ts, name) values (?, ?)")
dbBind(rs, list(c(t1, t2), c("你好hello中国", "你好hello中国")))
dbClearResult(rs)

t = data.frame(
    ts = c(t1, t2),
    name = c("你好hello中国", "你好hello中国")
)

rs <- dbSendQuery(conn, "select * from r_table")
df <- dbFetch(rs)
assert(all.equal(t, df))
dbClearResult(rs)

dbDisconnect(conn)

