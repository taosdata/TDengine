/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package main
 
import (
	"database/sql"
    "time"
    "log"
    "fmt"
	_ "taosSql"
)

func main() {
    taosDriverName := "taosSql"
    demodb := "demodb"
    demot := "demot"

    fmt.Printf("\n======== start demo test ========\n")
    // open connect to taos server
    db, err := sql.Open(taosDriverName, "root:taosdata@/tcp(127.0.0.1:0)/demodb")
    if err != nil {
        log.Fatalf("Open database error: %s\n", err)
    }
    defer db.Close()

    drop_database(db, demodb)
    create_database(db, demodb)
    use_database(db, demodb)
    create_table(db, demot)
    insert_data(db, demot)
    select_data(db, demot)

    fmt.Printf("\n======== start stmt mode test ========\n")

    demodbStmt := "demodbStmt"
    demotStmt := "demotStmt"
    drop_database_stmt(db, demodbStmt)
    create_database_stmt(db, demodbStmt)
    use_database_stmt(db, demodbStmt)
    create_table_stmt(db, demotStmt)
    insert_data_stmt(db, demotStmt)
    select_data_stmt(db, demotStmt)

    fmt.Printf("\n======== end demo test ========\n")
}

func drop_database(db *sql.DB, demodb string) {
    st := time.Now().Nanosecond()
    res, err := db.Exec("drop database " + demodb)
    checkErr(err)

    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()

    fmt.Printf("drop database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func create_database(db *sql.DB, demodb string) {
    st := time.Now().Nanosecond()
    // create database
    res, err := db.Exec("create database " + demodb)
    checkErr(err)

    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()

    fmt.Printf("create database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)

    return
}

func use_database(db *sql.DB, demodb string) {
    st := time.Now().Nanosecond()
    // use database
    res, err := db.Exec("use " +  demodb)  // notes: must no quote to db name
    checkErr(err)

    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()

    fmt.Printf("use database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func create_table(db *sql.DB, demot string) {
    st := time.Now().Nanosecond()
    // create table
    res, err := db.Exec("create table " + demot + " (ts timestamp, id int, name binary(8), len tinyint, flag bool, notes binary(8), fv float, dv double)")
    checkErr(err)

    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()
    fmt.Printf("create table result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func insert_data(db *sql.DB, demot string) {
    st := time.Now().Nanosecond()
    // insert data
    res, err := db.Exec("insert into " + demot +
                               " values (now, 100, 'beijing', 10,  true, 'one', 123.456, 123.456)" +
                                      " (now+1s, 101, 'shanghai', 11, true, 'two', 789.123, 789.123)" +
                                      " (now+2s, 102, 'shenzhen', 12,  false, 'three', 456.789, 456.789)")

    checkErr(err)

    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()
    fmt.Printf("insert data result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func select_data(db *sql.DB, demot string) {
    st := time.Now().Nanosecond()

    rows, err := db.Query("select * from ? " , demot)  // go text mode
    checkErr(err)

    fmt.Printf("%10s%s%8s %5s %9s%s %s %8s%s %7s%s %8s%s %4s%s %5s%s\n", " ","ts", " ", "id"," ", "name"," ","len", " ","flag"," ", "notes", " ", "fv", " ", " ", "dv")
    var affectd int
    for rows.Next() {
        var ts string
        var name string
        var id int
        var len int8
        var flag bool
        var notes string
        var fv float32
        var dv float64

        err = rows.Scan(&ts, &id, &name, &len, &flag, &notes, &fv, &dv)
        checkErr(err)

        fmt.Printf("%s\t", ts)
        fmt.Printf("%d\t",id)
        fmt.Printf("%10s\t",name)
        fmt.Printf("%d\t",len)
        fmt.Printf("%t\t",flag)
        fmt.Printf("%s\t",notes)
        fmt.Printf("%06.3f\t",fv)
        fmt.Printf("%09.6f\n",dv)

        affectd++
    }

    et := time.Now().Nanosecond()
    fmt.Printf("insert data result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
    fmt.Printf("insert data result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func drop_database_stmt(db *sql.DB,demodb string) {
    st := time.Now().Nanosecond()
    // drop test db
    stmt, err := db.Prepare("drop database ?")
    checkErr(err)
    defer stmt.Close()

    res, err := stmt.Exec(demodb)
    checkErr(err)

    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()
    fmt.Printf("drop database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func create_database_stmt(db *sql.DB,demodb string)  {
    st := time.Now().Nanosecond()
    // create database
    //var stmt interface{}
    stmt, err := db.Prepare("create database ?")
    checkErr(err)

    //var res driver.Result
    res, err := stmt.Exec(demodb)
    checkErr(err)

    //fmt.Printf("Query OK, %d row(s) affected()", res.RowsAffected())
    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()
    fmt.Printf("create database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func use_database_stmt (db *sql.DB,demodb string) {
    st := time.Now().Nanosecond()
    // create database
    //var stmt interface{}
    stmt, err := db.Prepare("use " + demodb)
    checkErr(err)

    res, err := stmt.Exec()
    checkErr(err)

    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()
    fmt.Printf("use database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func create_table_stmt (db *sql.DB,demot string)  {
    st := time.Now().Nanosecond()
    // create table
    // (ts timestamp, id int, name binary(8), len tinyint, flag bool, notes binary(8), fv float, dv double)
    stmt, err := db.Prepare("create table ? (? timestamp, ? int, ? binary(8), ? tinyint, ? bool, ? binary(8), ? float, ? double)")
    checkErr(err)

    res, err := stmt.Exec(demot, "ts", "id", "name", "len", "flag", "notes", "fv", "dv")
    checkErr(err)

    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()
    fmt.Printf("create table result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func insert_data_stmt(db *sql.DB,demot string)  {
    st := time.Now().Nanosecond()
    // insert data into table
    stmt, err := db.Prepare("insert into ? values(?, ?, ?, ?, ?, ?, ?, ?) (?, ?, ?, ?, ?, ?, ?, ?) (?, ?, ?, ?, ?, ?, ?, ?)")
    checkErr(err)

    res, err := stmt.Exec(demot, "now" ,    1000, "'haidian'" ,    6, true, "'AI world'",  6987.654, 321.987,
                                 "now+1s", 1001, "'changyang'" ,  7, false,  "'DeepMode'", 12356.456, 128634.456,
                                 "now+2s", 1002, "'chuangping'" , 8, true, "'database'",  3879.456, 65433478.456,)
    checkErr(err)

    affectd, err := res.RowsAffected()
    checkErr(err)

    et := time.Now().Nanosecond()
    fmt.Printf("insert data result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func select_data_stmt(db *sql.DB, demot string) {
    st := time.Now().Nanosecond()

    stmt, err := db.Prepare("select ?, ?, ?, ?, ?, ?, ?, ? from ?" )  // go binary mode
    checkErr(err)

    rows, err := stmt.Query("ts", "id","name","len", "flag","notes", "fv", "dv", demot)
    checkErr(err)

    fmt.Printf("%10s%s%8s %5s %9s%s %s %8s%s %7s%s %8s%s %11s%s %14s%s\n", " ","ts", " ", "id"," ", "name"," ","len", " ","flag"," ", "notes", " ", "fv", " ", " ", "dv")
    var affectd int
    for rows.Next() {
        var ts string
        var name string
        var id int
        var len int8
        var flag bool
        var notes string
        var fv float32
        var dv float64

        err = rows.Scan(&ts, &id, &name, &len, &flag, &notes, &fv, &dv)
        //fmt.Println("start scan fields from row.rs, &fv:", &fv)
        //err = rows.Scan(&fv)
        checkErr(err)

        fmt.Printf("%s\t", ts)
        fmt.Printf("%d\t",id)
        fmt.Printf("%10s\t",name)
        fmt.Printf("%d\t",len)
        fmt.Printf("%t\t",flag)
        fmt.Printf("%s\t",notes)
        fmt.Printf("%06.3f\t",fv)
        fmt.Printf("%09.6f\n",dv)

        affectd++

    }

    et := time.Now().Nanosecond()
    fmt.Printf("insert data result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func checkErr(err error) {
    if err != nil {
        panic(err)
    }
}
