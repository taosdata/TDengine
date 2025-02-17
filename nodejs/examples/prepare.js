class PrepareData {

    constructor(db, table, numOfRows, ifStable, ts, numOfSub,) {
        this.db = db;
        this.table = table;
        this.numOfRows = numOfRows;
        this.ifStable = ifStable;
        this.ts = ts;
        this.sub = table + "_sub";
        this.numOfSub = numOfSub;
    }

    createDB() {
        return `create database if not exists ${this.db} keep 3650 ;`;
    }
    useDB() {
        return `use ${this.db} ;`;
    }
    createTable() {
        let sql = ""
        if (this.ifStable) {
            sql += `create table if not exists ${this.table} (` +
                ' ts timestamp' +
                ',v1 tinyint' +
                ',v2 smallint' +
                ',v4 int' +
                ',v8 bigint' +
                ',u1 tinyint unsigned' +
                ',u2 smallint unsigned' +
                ',u4 int unsigned' +
                ',u8 bigint unsigned' +
                ',f4 float,f8 double' +
                ',bin binary(200)' +
                ',nchr nchar(200)' +
                ',b bool' +
                ',nilcol int' +
                ')tags(' +
                ' bo bool' +
                ',tt tinyint' +
                ',si smallint' +
                ',ii int' +
                ',bi bigint' +
                ',tu tinyint unsigned' +
                ',su smallint unsigned' +
                ',iu int unsigned' +
                ',bu bigint unsigned' +
                ',ff float' +
                ',dd double' +
                ',bb binary(200)' +
                ',nc nchar(200)' +
                ') ;'

        } else {
            sql += `create table if not exists ${this.table} (` +
                ' ts timestamp' +
                ',v1 tinyint' +
                ',v2 smallint' +
                ',v4 int' +
                ',v8 bigint' +
                ',u1 tinyint unsigned' +
                ',u2 smallint unsigned' +
                ',u4 int unsigned' +
                ',u8 bigint unsigned' +
                ',f4 float,f8 double' +
                ',bin binary(200)' +
                ',nchr nchar(200)' +
                ',b bool' +
                ',nilcol int' +
                ');'
        }
        return sql;
    }

    dropTable() {
        return `drop table if exists ${this.db}.${this.table};`;
    }
    dropDB() {
        return `drop database if exists ${this.db};`;
    }

    insert() {
        if (this.ifStable == false) {
            let str = `insert into ${this.table} values `
            for (let i = 0; i < this.numOfRows; i++) {
                str += '('
                str += `${this.ts}`
                str += `, ${-1 * i + 1}`
                str += `, ${-1 * i + 2}`
                str += `, ${-1 * i + 3}`
                str += `, ${-1 * i + 4}`
                str += `, ${1 * i + 1}`
                str += `, ${1 * i + 2}`
                str += `, ${1 * i + 3}`
                str += `, ${1 * i + 4}`
                str += `, ${3.1415 * i}`
                str += `, ${3.1415926536 * i}`
                str += `, 'binary_col_${i}'`
                str += `, 'nchar_col_${i}'`
                str += `, ${i & 1 == 1 ? true : false}`
                str += ', null'
                str += ')'
                this.ts += 1;
            }
            return str += ";";
        }else{
            throw new Error("using insertStable() for stable");
        }
        
    }

    insertStable() {
        let sql = [];
        for (let i = 0; i < this.numOfSub; i++) {
            let str = `insert into ${this.sub}_${i} using ${this.table} tags`
            str += '('
            str += `  ${i & 1 == 1 ? true : false}`
            str += `, ${-1 * i + 1}`
            str += `, ${-1 * i + 2}`
            str += `, ${-1 * i + 3}`
            str += `, ${-1 * i + 4}`
            str += `, ${1 * i + 1}`
            str += `, ${1 * i + 2}`
            str += `, ${1 * i + 3}`
            str += `, ${1 * i + 4}`
            str += `, ${3.1415 * i}`
            str += `, ${3.1415926536 * i}`
            str += `, 'binary_tag_${i}'`
            str += `, 'nchar_tag_${i}'`
            str += ')values'
            for (let j = 0; j < this.numOfRows; j++) {
                str += '('
                str += `${this.ts}`
                str += `, ${-1 * j + 1}`
                str += `, ${-1 * j + 2}`
                str += `, ${-1 * j + 3}`
                str += `, ${-1 * j + 4}`
                str += `, ${1 * j + 1}`
                str += `, ${1 * j + 2}`
                str += `, ${1 * j + 3}`
                str += `, ${1 * j + 4}`
                str += `, ${3.1415 * j}`
                str += `, ${3.1415926536 * j}`
                str += `, 'binary_col_${j}'`
                str += `, 'nchar_col_${j}'`
                str += `, ${j & 1 == 1 ? true : false}`
                str += ', null'
                str += ')'
                this.ts += 1;
            }
            str += ";"
            sql.push(str);
        }
        return sql;
    }

}

module.exports = PrepareData;