%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% MIT License
%
% Copyright (c) 2022-2023 freemine <freemine@yeah.net>
%
% Permission is hereby granted, free of charge, to any person obtaining a copy
% of this software and associated documentation files (the "Software"), to deal
% in the Software without restriction, including without limitation the rights
% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
% copies of the Software, and to permit persons to whom the Software is
% furnished to do so, subject to the following conditions:
%
% The above copyright notice and this permission notice shall be included in
% all copies or substantial portions of the Software.
%
% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
% SOFTWARE.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(erl_test).
-export([start/0]).

start() ->
  odbc:start(),

  {ok,C}=odbc:connect("DSN=TAOS_ODBC_DSN",[]),

  Stmts = "show databases;"
          "drop database if exists foo;"
          "create database if not exists foo;"
          "create table foo.t (ts timestamp, name varchar(20), mark nchar(20));"
          "insert into foo.t (ts, name, mark) values (now(),'name','mark');"
          "select name, mark from foo.t;",
  case odbc:sql_query(C,Stmts) of
    [{selected,_,_},
     {updated,0},
     {updated,0},
     {updated,0},
     {updated,1},
     {selected,["name","mark"],[{"name",<<109,0,97,0,114,0,107,0>>}]}] -> ok
  end,

  case odbc:param_query(C, "insert into foo.t (ts, name, mark) values (?,?,?)",
                [{{sql_varchar,40}, ["2023-05-16 03:04:05.012", "2023-05-16 03:04:05.121"]},
                 {{sql_varchar,40}, ["name1", "name2"]},
                 {{sql_varchar,40}, ["mark1", "mark2"]}]) of
    {updated,2} -> ok
  end,

  case odbc:sql_query(C,"select name, mark from foo.t") of
    {selected,["name","mark"],[{"name1", <<109,0,97,0,114,0,107,0,49,0>>},{"name2", <<109,0,97,0,114,0,107,0,50,0>>},{"name", <<109,0,97,0,114,0,107,0>>}]} -> ok
  end,

  case odbc:sql_query(C, "create stable foo.st (ts timestamp, age int) tags (name varchar(20));") of
    {updated,0} -> ok
  end,

  case odbc:param_query(C, "insert into foo.t1 using foo.st tags (?) values (?,?)",
                [{{sql_varchar,40}, ["shanghai"]},
                 {{sql_varchar,40}, ["2023-05-17 13:14:15.012"]},
                 {sql_integer, [34]}]) of
    {updated,1} -> ok
  end,

  ok.

