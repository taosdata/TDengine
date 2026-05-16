export class ColumnInfo {
    data: ArrayBuffer;
    length: number;
    type: number;
    typeLen: number;
    isNull?: number[];
    _rows: number;
    _haveLength: number = 0;
    _dataLengths?: number[];

    constructor(
        [length, data]: [number, ArrayBuffer],
        type: number,
        typeLen: number,
        rows: number,
        isNull?: number[],
        dataLengths?: number[],
        haveLength: number = 0
    ) {
        this.data = data;
        this.type = type;
        this.length = length;
        this.typeLen = typeLen;
        this._rows = rows;
        this.isNull = isNull;
        this._dataLengths = dataLengths;
        this._haveLength = haveLength;
    }
}
