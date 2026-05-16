;; MIT License
;;
;; Copyright (c) 2022-2023 freemine <freemine@yeah.net>
;;
;; Permission is hereby granted, free of charge, to any person obtaining a copy
;; of this software and associated documentation files (the "Software"), to deal
;; in the Software without restriction, including without limitation the rights
;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;; copies of the Software, and to permit persons to whom the Software is
;; furnished to do so, subject to the following conditions:
;;
;; The above copyright notice and this permission notice shall be included in
;; all copies or substantial portions of the Software.
;;
;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
;; SOFTWARE.

(write-line "Hello, Common Lisp!")

;; (asdf:oos 'asdf:load-op :plain-odbc)
(ql:quickload '(:plain-odbc :local-time))
(use-package '(:plain-odbc :local-time))

(defun getNextTick (t0)
  (loop 
    (when (timestamp> (now) t0) (return (now)))
  )
)

(defun tsFormat (t0)
  (format NIL "~4,'0d-~2,'0d-~2,'0d ~2,'0d:~2,'0d:~2,'0d.~3,'0d"
    (timestamp-year t0) (timestamp-month t0) (timestamp-day t0)
    (timestamp-hour t0) (timestamp-minute t0) (timestamp-second t0) (floor (nsec-of t0) 1000000))
)

(defun prepareExecAndCheck (con insert params param-values select rows_expected)
  (let ((stm))
    (setf stm (apply #'prepare-statement (cons con (cons insert params))))
    (assert (eq 1 (apply #'exec-prepared-update (cons stm param-values))))
    (assert (equal rows_expected (apply #'exec-query (cons con (cons select nil)))))
    (free-statement stm)
  )
)

(defun main ()
  (let ((con) (t0))
    (setf con (connect-generic :dsn "TAOS_ODBC_DSN" :uid "root" :pwd "taosdata"))
    (assert (eq 0 (exec-update con "create database if not exists foo")))
    (close-connection con)

    (setf con (connect-generic :dsn "TAOS_ODBC_DSN" :uid "root" :pwd "taosdata" :database "foo"))

    (assert (eq 0 (exec-update con "drop table if exists common_lisp")))
    (assert (eq 0 (exec-update con "create table if not exists common_lisp(ts timestamp, name varchar(20))")))

    (setf t0 (tsFormat (getNextTick (now))))

    (prepareExecAndCheck
      con
      "insert into common_lisp (ts, name) values (?, ?)"
      '((:string :in) (:unicode-string :in))
      (list t0 "你好hello中国")
      "select * from common_lisp"
      (list (list t0 "你好hello中国")))

    (close-connection con)
  )
)

(main)

(write-line "==success==")

