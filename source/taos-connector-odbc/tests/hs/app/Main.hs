-- MIT License
-- 
-- Copyright (c) 2022-2023 freemine <freemine@yeah.net>
-- 
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
-- 
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
-- 
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.


module Main where

import Database.HDBC
import Database.HDBC.ODBC
import Data.Time
import Text.Printf

myAssert :: Monad m => Bool -> String -> m ()
myAssert True _ = return ()
myAssert _    s = error s

getNextTick :: Maybe String -> IO String
getNextTick Nothing = formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S.%3q" <$> getZonedTime
getNextTick (Just prev) = do
  t <- formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S.%3q" <$> getZonedTime
  if t > prev then
    return t
  else
    getNextTick (Just prev)

runAndCheck :: IConnection conn => conn -> String -> Integer -> IO ()
runAndCheck conn sql affected_rows = do
  v <- run conn sql []
  myAssert (v==affected_rows) $ printf "\n  sql:%s\n  affected rows expected:%d,\n  but got ==============:%d:" sql affected_rows v

quickQueryAndCheck :: IConnection conn => conn -> String -> [[SqlValue]] -> IO ()
quickQueryAndCheck conn sql rs_expected = do
  rs <- quickQuery conn sql []
  myAssert (rs == rs_expected) $ printf "\n  sql:%s:\n  rs expected:%s,\n  but got ===:%s:" sql (show rs_expected) (show rs)

execAndCheck :: Statement -> [SqlValue] -> Integer -> IO ()
execAndCheck stmt params affected_rows = do
  v <- execute stmt params
  myAssert (v==affected_rows) $ printf "\n  affected rows expected:%d,\n  but got ==============:%d:" affected_rows v


keepDatabase :: String -> IO ()
keepDatabase db = do
  conn <- connectODBC "DSN=TAOS_ODBC_DSN"
  runAndCheck conn (printf "create database if not exists %s" db) 0
  disconnect conn

main :: IO ()
main = do
  putStrLn "Hello, Haskell!"
  keepDatabase "foo"

  conn <- connectODBC "DSN=TAOS_ODBC_DSN;Database=foo"

  runAndCheck conn "drop table if exists haskell" 0
  runAndCheck conn  "create table if not exists haskell (ts timestamp, name varchar(20))" 0

  stmt <- prepare conn "insert into haskell (ts, name) values (?, ?)"
  t3 <- getNextTick Nothing
  execAndCheck stmt [toSql t3, toSql "hello"] 1
  t4 <- getNextTick (Just t3)
  execAndCheck stmt [toSql t4, toSql "中国"] 1
  quickQueryAndCheck conn "select * from haskell" [[toSql t3, toSql "hello"],[toSql t4, toSql "中国"]]

  runAndCheck conn "drop table if exists haskell" 0
  runAndCheck conn "create table if not exists haskell (ts timestamp, name varchar(20))" 0
  runAndCheck conn (printf "insert into haskell (ts, name) values ('%s', '中国')" t4) 1
  quickQueryAndCheck conn "select * from haskell" [[toSql t4, toSql "中国"]]

