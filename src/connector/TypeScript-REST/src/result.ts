interface IResult {
    status: string;
    head?: Array<string>;
    column_meta?: Array<Array<any>>;
    data?: Array<Array<any>>;
    rows?: number;
    command?: string;
    //for error 
    code?: number;
    desc?: string;
}

interface meta {
    columnName: string;
    code: number;
    size: number;
    typeName?: string;
}

export class Result {
    private _status: string;
    private _head?: string[];
    private _column_meta?: Array<meta>;
    private _data?: Array<Array<any>>;
    private _rows?: number;
    private _command?: string;
    //for error 
    private _code?: number;
    private _desc?: string;

    constructor(res: IResult, commands?: string) {
        let meta_list_length = res.column_meta == undefined ? 0 : res.column_meta.length
        if (res.status === 'succ') {
            this._status = res.status;
            this._head = res.head;
            this._column_meta = new Array(meta_list_length);
            this._data = res.data;
            this._rows = res.rows;
            this._command = commands;
            this._initMeta(res);
            this._code = undefined;
            this._desc = undefined;
        } else {
            this._status = res.status;
            this._head = undefined;
            this._column_meta = undefined;
            this._data = undefined;
            this._rows = undefined;
            this._command = commands;
            this._code = res.code;
            this._desc = res.desc;
        }

    }
    private _initMeta(res: IResult): void {
        if (res.column_meta != undefined) {
            res.column_meta.forEach((item, index) => {
                if (this._column_meta != undefined)
                    this._column_meta[index] = {
                        columnName: item[0],
                        code: item[1],
                        size: item[2],
                        typeName: typeNameMap[item[1]]
                    }
            })
        }
    }

    getResult(): Result {
        return this;
    }

    getStatus(): string {
        return this._status;
    }

    getHead(): Array<any> | undefined {
        return this._head;
    }

    getMeta(): Array<meta> | undefined {
        return this._column_meta;
    }

    getData(): Array<Array<any>> | undefined {
        return this._data;
    }

    getAffectRows(): number | undefined {
        return this._rows;
    }

    getCommand(): string | undefined {
        return this._command;
    }

    getErrCode(): number | undefined {
        return this._code;
    }

    getErrStr(): string | undefined {
        return this._desc;
    }

    toString(): void {
        let str = '';
        if(this._command != undefined){
            console.log(this._command);
        }
        if (this._status === 'succ' && this._column_meta != undefined && this._data != undefined) {
             str = this._prettyStr(this._column_meta, this._data)
        } else {
            str = `Execute ${this._status},reason:${this._desc}. error_no:${this._code} `;
            console.log(str)
        }
    }

    private _prettyStr(fields: Array<meta>, data: Array<Array<any>>): string {
        let colName = new Array<string>();
        let colType = new Array<string | undefined>();
        let colSize = new Array<number>();
        let colStr = '';

        for (let i = 0; i < fields.length; i++) {
            colName.push(fields[i].columnName)
            colType.push(fields[i].typeName);

            if ((fields[i].code) == 8 || (fields[i].code) == 10) {
                colSize.push(Math.max(fields[i].columnName.length, fields[i].size));  //max(column_name.length,column_type_precision)
            } else {
                colSize.push(Math.max(fields[i].columnName.length, suggestedMinWidths[fields[i].size]));// max(column_name.length,suggest_column_with_suggestion)
            }
            // console.log(colSize)
        }
        colName.forEach((name, i) => {
            colStr += this._fillEmpty(Math.floor(colSize[i] / 2 - name.length / 2)) + name.toString() + this._fillEmpty(Math.ceil(colSize[i] / 2 - name.length / 2)) + " | "
        })

        let strSperator = ""
        let sizeSum = colSize.reduce((a, b) => a += b, (0)) + colSize.length * 3
        strSperator = this._printN("=", sizeSum)

        console.log("\n" + colStr)
        console.log(strSperator)

        data.forEach((row) => {
            let rowStr = ""
            row.forEach((cell, index) => {
                rowStr += cell == null ? 'null' : cell.toString();
                rowStr += this._fillEmpty(colSize[index] - cell.toString().length) + " | "
            })
            console.log(rowStr)
        })

        return colStr
    }

    private _fillEmpty(n:number) {
        let str = "";
        for (let i = 0; i < n; i++) {
            str += " ";
        }
        return str;
    }

    private _printN(s:string, n:number) {
        let f = "";
        for (let i = 0; i < n; i++) {
            f += s;
        }
        return f;
    }
}



interface indexableString {
    [index: number]: string
}
/**
 * this file record TDengine's data type and code.
 */
const typeNameMap: indexableString = {
    0: 'null',
    1: 'bool',
    2: 'tinyint',
    3: 'smallint',
    4: 'int',
    5: 'bigint',
    6: 'float',
    7: 'double',
    8: 'binary',
    9: 'timestamp',
    10: 'nchar',
    11: 'unsigned tinyint',
    12: 'unsigned smallint',
    13: 'unsigned int',
    14: 'unsigned bigint',
    15: 'json'
}

interface indexableNumber {
    [index: number]: number
}
const suggestedMinWidths: indexableNumber = {
    0: 4,
    1: 4,
    2: 4,
    3: 6,
    4: 11,
    5: 12,
    6: 24,
    7: 24,
    8: 10,
    9: 25,
    10: 10,
}


