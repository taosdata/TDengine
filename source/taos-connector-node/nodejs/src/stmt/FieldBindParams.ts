export class FieldBindParams {
    params: any[];
    dataType: string;
    typeLen: number;
    columnType: number;
    bindType: number; // 0: normal, 1: table name, 2: tag

    constructor(
        params: any[],
        dataType: string,
        typeLen: number,
        columnType: number,
        bindType: number
    ) {
        this.params = [...params];
        this.dataType = dataType;
        this.typeLen = typeLen;
        this.columnType = columnType;
        this.bindType = bindType;
    }
}
