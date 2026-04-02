interface IResult {
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
    typeName: string;
    size: number;
}

export class Result {
    private _column_meta?: Array<meta>;
    private _data?: Array<Array<any>>;
    private _rows?: number;
    private _command?: string;
    //for error 
    private _code?: number;
    private _desc?: string;

    constructor(res: IResult, commands?: string) {
        let meta_list_length = res.column_meta == undefined ? 0 : res.column_meta.length
        if(res.code == 0){
            this._code = res.code;
            if(res.data&&res.column_meta&&res.column_meta[0][0] === "affected_rows"){
                this._rows = res.data[0][0]
            }else if(res.data){
                this._rows = res.data.length;
            }
            this._column_meta = new Array(meta_list_length);
            this._initMeta(res);
            this._data = res.data;
            this._command = commands;
            this._desc = undefined;
        }else {
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
                        typeName: item[1],
                        size: item[2],              
                    }
            })
        }
    }
    private _initData(resData:Array<Array<any>>,meta:Array<Array<any>>,rows:number):void{
        if (resData.length = 0){
            this._data=[[]];
        } else {
            this._data= new Array<Array<any>>(resData.length);
            for (let i =0 ;i<rows;i++){
                for (let j=0;j<meta.length;j++){
                    if(meta[j][1] ==9 ){
                        this._data[i][j] = new Date(resData[i][j])
                    }else{
                        this._data[i][j] = resData[i][j]
                    }
                }
            }
        }
    }

    getResult(): Result {
        return this;
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

    private _prettyStr(fields: Array<meta>, data: Array<Array<any>>): string {
        let colName = new Array<string>();
        let colType = new Array<string | undefined>();
        let colSize = new Array<number>();
        let colStr = '';

        for (let i = 0; i < fields.length; i++) {
            colName.push(fields[i].columnName)
            colType.push(fields[i].typeName);

            if ((fields[i].typeName) === "VARCHAR" || (fields[i].typeName) === "NCHAR") {
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


