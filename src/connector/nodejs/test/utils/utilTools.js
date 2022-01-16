/**
 * This is an util function will return the column info based on the create sql.
 * @param {*} sql 
 * @returns Return an Array about the column names and column type.
 * 
 */
function getFeildsFromDll(sql) {
    let fields = [];

    let firstBracket = sql.indexOf('(');
    let lastBracket = sql.lastIndexOf(')')

    let metaStr = sql.slice(firstBracket, lastBracket + 1);
    let splitTags = metaStr.split("tags");

    splitTags.forEach((item, index, arr) => {
        arr[index] = item.slice(1, item.length - 1)
    })
    splitTags.forEach((item) => {
        let tmp = item.split(",");
        tmp.forEach((item) => {
            let newItem = item.trim();
            let spaceInd = newItem.indexOf(' ', 1)
            fields.push(newItem.slice(0, spaceInd));
            fields.push(newItem.slice(spaceInd + 1, newItem.length))
        })
    })
    return fields;
}

/**
 * Based on the input array, it will generate sql that could be used to insert the data of array into the db.
 * @param {*} tableName It could be the table name that you want to insert data.
 * @param {*} stable If you want to using stable as template to create table automatically, 
 *                   set this to your stable name. Deault if '';
 * @param {*} dataArr An Array of data that you want insert (it could be mutilple lines)
 * @param {*} tagArr  An Array used to store one sub table's tag info  
 * @param {*} numOfColumn The number of columns that the target table has.
 * @returns Return an insert sql string.
 */
function buildInsertSql(tableName, stable = '', dataArr, tagArr = [], numOfColumn) {
    let insertSql = "";
    let dataPartial = "(";
    let tagPart = "(";

    dataArr.forEach((item, index) => {
        // let  item = dataArr[index];
        if (typeof item == "string") {
            dataPartial += '\'' + item + '\'';
        } else {
            dataPartial += item;
        }
        if ((index + 1) % numOfColumn == 0 && (index + 1) != dataArr.length) {
            dataPartial += ")("
        } else if ((index + 1) % numOfColumn == 0 && (index + 1) == dataArr.length) {
            dataPartial += ")"
        } else {
            dataPartial += ","
        }

    })
    if (stable != '') {
        tagArr.forEach((item, index) => {
            if (typeof item == "string") {
                tagPart += '\'' + item + '\'';
            } else {
                tagPart += item;
            }

            if (index != tagArr.length - 1) {
                tagPart += ",";
            } else {
                tagPart += ")";
            }
        })
    }

    if (stable == '') {
        insertSql += `insert into ${tableName} values ${dataPartial};`
    } else {
        insertSql += `insert into ${tableName} using ${stable} tags ${tagPart} values ${dataPartial};`
    }

    return insertSql;
}
/**
 * used to mapping the data type of an create clause into TDengine's datatype code
 */
const TDengineTypeCode = {
    'null': 0,
    'bool': 1,
    'tinyint': 2,
    'smallint': 3,
    'int': 4,
    'bigint': 5,
    'float': 6,
    'double': 7,
    'binary': 8,
    'timestamp': 9,
    'nchar': 10,
    'tinyint unsigned': 11,
    'smallint unsigned': 12,
    'int unsigned': 13,
    'bigint unsigned': 14,
    'json': 15,
}

/**
 * Mapping the data type with corresponing size that has defined in tdengine
 */
const TDengineTypeBytes = {
    'null': 0,
    'bool': 1,
    'tinyint': 1,
    'smallint': 2,
    'int': 4,
    'bigint': 8,
    'float': 4,
    'double': 8,
    'timestamp': 8,
    'tinyint unsigned': 1,
    'smallint unsigned': 2,
    'int unsigned': 4,
    'bigint unsigned': 8,
    'json': 4096,
}

/**
 * Used to create an array of taos feilds object. 
 * @param {*} arr This should be the return array from the method getFeildsFromDll()
 * @returns Return an array of taosFeild Object
 */
function getFieldArr(arr) {
    let feild = [];
    for (let i = 0; i < arr.length;) {
        let bracetPosi = arr[i + 1].indexOf('(');
        let type = '';
        let size = -1;
       
        if (bracetPosi == -1) {
            type = TDengineTypeCode[arr[i + 1]];
            size = TDengineTypeBytes[arr[i + 1]];
        }else{
            type = TDengineTypeCode[arr[i + 1].slice(0, bracetPosi)];
            size = Number(arr[i + 1].slice(bracetPosi + 1, arr[i + 1].indexOf(')')));
        }
        let fieldObj = {
            name: arr[i].toLowerCase(),
            type: type,
            bytes: size
        }
        feild.push(fieldObj);
        i = i + 2;
    }
    return feild;
}
/**
 * Conbine arrays of data info and tag info together, and return a new array. This array construction is simmilar with query result
 * from the tdengine by taos shell.This method only can be used by a subtable.
 * @param {*} dataArr An array holds  columns' data  that will be insert into the db.
 * @param {*} tagArr An array holds tags' data that is belong to a sub table.
 * @param {*} numOfcolumn 
 * @returns return the an array of column data and tag data.
 */
function getResData(dataArr, tagArr, numOfcolumn) {
    let resData = [];    
    dataArr.forEach((item, index) => {
        resData.push(item);
        if ((index + 1) % numOfcolumn == 0) {
            tagArr.forEach((element) => {
                resData.push(element);
            }) ;
        } 
    });
    return resData;
}
module.exports = { getFeildsFromDll, buildInsertSql, getFieldArr, getResData };