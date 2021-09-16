import {getTaoType} from '../src/restConstant'


export class TDengineRestResultSet {
  constructor(result) {
    this.status = ''    //succ
    this.column_name = {} //head
    this.column_type = {} //column_meta
    this.data = {}
    this.affectRows = null //rows
    this.code = null
    this.desc = null
    this._init(result)
  }

  //initial the resultSet with a jason parameter
  /**
   *
   * @param jason
   */
  _init(result) {
    if (result.status) {
      this.status = result.status
    }
    if (result.head) {
      this.column_name = result.head
    }
    if (result.column_meta) {
      this.column_type = result.column_meta
    }
    if (result.data) {
      this.data = result.data
    }
    if (result.rows) {
      this.affectRows = result.rows
    }
    if (result.code) {
      this.code = result.code
    }
    if (result.desc) {
      this.desc = result.desc
    }
  }

  getStatus() {
    return this.status
  }

  getColumn_name() {
    return this.column_name
  }

  getColumn_type() {
    let column_data = []
    this.column_type.forEach(function (column) {
      column[1] = getTaoType(column[1])
      column_data.push(column)
    })
    return column_data
  }

  getData() {
    return this.data
  }

  getAffectRow() {
    return this.affectRows
  }

  getCode() {
    return this.code
  }

  getDesc() {
    return this.desc
  }


  toString() {
    let fields = this.column_type
    let rows = this.data
    this._prettyStr(fields, rows)
  }

  _prettyStr(fields, data) {
    let colName = []
    let colType = []
    let colSize = []
    let colStr = ""


    for (let i = 0; i < fields.length; i++) {
      colName.push(fields[i][0])
      colType.push(fields[i][1])

      if ((fields[i][1]) == 8 || (fields[i][1]) == 10) {
        colSize.push(Math.max(fields[i][0].length, fields[i][2]));  //max(column_name.length,column_type_precision)
      } else {
        colSize.push(Math.max(fields[i][0].length, suggestedMinWidths[fields[i][1]]));// max(column_name.length,suggest_column_with_suggestion)
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

  _fillEmpty(n) {
    let str = "";
    for (let i = 0; i < n; i++) {
      str += " ";
    }
    return str;
  }

  _printN(s, n) {
    let f = "";
    for (let i = 0; i < n; i++) {
      f += s;
    }
    return f;
  }
}

const suggestedMinWidths = {
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
