const ref = require('ref-napi');
const StructType = require('ref-struct-di')(ref);
const taosConst = require('./constants');

var TAOS_MULTI_BIND = StructType({
    'buffer_type': ref.types.int,
    'buffer': ref.refType(ref.types.void),
    'buffer_length': ref.types.ulong,
    'length': ref.refType(ref.types.int),
    'is_null': ref.refType(ref.types.char),
    'num': ref.types.int,
})

class TaosMultiBind {
    constructor() {
    }

    /**
     * To bind bool through an array.
     * @param {*} boolArray is an boolean array that stores one column's value.
     * @returns A instance of struct TAOS_MULTI_BIND  that contains one column's data with bool type.
     */
    multiBindBool(boolArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.bool.size * boolArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * boolArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * boolArray.length);

        boolArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.bool.size, ref.types.int)
            if (element == null || element == undefined) {
                // ref.set(mbindBufferBuf,index * ref.types.int64.size,taosConst.C_BIGINT_NULL,ref.types.int64);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.set(mbindBufferBuf, index * ref.types.bool.size, element, ref.types.bool);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_BOOL,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.bool.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: boolArray.length,
        })
        return mbind;
    }

    /**
     * to bind tiny int through an array.
     * @param {*} tinyIntArray is an array that stores tiny int.
     * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with tiny int.
     */
    multiBindTinyInt(tinyIntArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.int8.size * tinyIntArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * tinyIntArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * tinyIntArray.length);

        tinyIntArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.int8.size, ref.types.int)
            if (element == null || element == undefined) {
                // ref.set(mbindBufferBuf,index * ref.types.int64.size,taosConst.C_BIGINT_NULL,ref.types.int64);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.set(mbindBufferBuf, index * ref.types.int8.size, element, ref.types.int8);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_TINYINT,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.int8.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: tinyIntArray.length,
        })
        return mbind;
    }

    /**
     * To bind small int through an array.
     * @param {*} smallIntArray is an array that stores small int.
     * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with small int.
     */
    multiBindSmallInt(smallIntArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.int16.size * smallIntArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * smallIntArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * smallIntArray.length);

        smallIntArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.int16.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.set(mbindBufferBuf, index * ref.types.int16.size, element, ref.types.int16);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_SMALLINT,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.int16.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: smallIntArray.length,
        })
        return mbind;
    }

    /**
    * To bind int through an array.
    * @param {*} intArray is an array that stores int.
    * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with int.
    */
    multiBindInt(intArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.int.size * intArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * intArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * intArray.length);

        intArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.int.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.set(mbindBufferBuf, index * ref.types.int.size, element, ref.types.int);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_INT,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.int.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: intArray.length,
        })
        return mbind;
    }

    /**
     * To bind big int through an array.
     * @param {*} bigIntArray is an array that stores big int.
     * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with big int.
     */
    multiBindBigInt(bigIntArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.int64.size * bigIntArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * bigIntArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * bigIntArray.length);

        bigIntArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.int64.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.writeInt64LE(mbindBufferBuf, index * ref.types.int64.size, element.toString())
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_BIGINT,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.int64.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: bigIntArray.length,
        })
        return mbind;
    }

    /**
     * To bind float through an array.
     * @param {*} floatArray is an array that stores float.
     * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with float.
     */
    multiBindFloat(floatArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.float.size * floatArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * floatArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * floatArray.length);

        floatArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.float.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.set(mbindBufferBuf, index * ref.types.float.size, element, ref.types.float);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_FLOAT,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.float.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: floatArray.length,
        })
        return mbind;
    }

    /**
     * To bind double through an array.
     * @param {*} doubleArray is an array that stores double.
     * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with double.
     */
    multiBindDouble(doubleArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.double.size * doubleArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * doubleArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * doubleArray.length);

        doubleArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.double.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.set(mbindBufferBuf, index * ref.types.double.size, element, ref.types.double);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_DOUBLE,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.double.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: doubleArray.length,
        })
        return mbind;
    }

    /**
     * To bind tdengine's binary through an array.
     * @param {*} strArr is an array that stores string.
     * (Null string can be defined as undefined or null,notice '' is not null.)
     * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with binary.
     */
    multiBindBinary(strArr) {
        let maxStrUFT8Length = this._maxUTF8StrArrLength(strArr);
        console.log(`maxStrUFT8Length * strArr.length=${maxStrUFT8Length * strArr.length}`);
        let mbindBufferBuf = Buffer.alloc(maxStrUFT8Length * strArr.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * strArr.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * strArr.length);

        strArr.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, this._stringUTF8Length(element), ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.writeCString(mbindBufferBuf, index * maxStrUFT8Length, element, 'utf8');
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }
        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_BINARY,
            buffer: mbindBufferBuf,
            buffer_length: maxStrUFT8Length,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: strArr.length,
        })
        return mbind;
    }

    /**
     * To bind timestamp through an array.
     * @param {*} timestampArray is an array that stores timestamp.
     * @returns  A instance of struct TAOS_MULTI_BIND that contains one column's data with timestamp.
     */
    multiBindTimestamp(timestampArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.int64.size * timestampArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * timestampArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * timestampArray.length);

        timestampArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.int64.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.writeInt64LE(mbindBufferBuf, index * ref.types.int64.size, element.toString())
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_TIMESTAMP,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.int64.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: timestampArray.length,
        })
        return mbind;
    }

    /**
     * To bind tdengine's nchar through an array.
     * @param {*} strArr is an array that stores string.
     * (Null string can be defined as undefined or null,notice '' is not null.)
     * @returns A instance of struct TAOS_MULTI_BIND that contains one nchar column's data with nchar.
     */
    multiBindNchar(strArr) {
        let maxStrUFT8Length = this._maxUTF8StrArrLength(strArr);
        // console.log(`maxStrUFT8Length * strArr.length=${maxStrUFT8Length * strArr.length}`);
        let mbindBufferBuf = Buffer.alloc(maxStrUFT8Length * strArr.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * strArr.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * strArr.length);

        strArr.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, this._stringUTF8Length(element), ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.writeCString(mbindBufferBuf, index * maxStrUFT8Length, element, 'utf8');
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }
        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_NCHAR,
            buffer: mbindBufferBuf,
            buffer_length: maxStrUFT8Length,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: strArr.length,
        })
        return mbind;
    }

    /**
    * to bind unsigned tiny int through an array.
    * @param {*} uTinyIntArray is an array that stores unsigned tiny int.
    * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with unsigned tiny int.
    */
    multiBindUTinyInt(uTinyIntArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.uint8.size * uTinyIntArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * uTinyIntArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * uTinyIntArray.length);

        uTinyIntArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.uint8.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.set(mbindBufferBuf, index * ref.types.uint8.size, element, ref.types.uint8);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_TINYINT_UNSIGNED,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.uint8.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: uTinyIntArray.length,
        })
        return mbind;
    }

    /**
     * To bind unsigned small int through an array.
     * @param {*} uSmallIntArray is an array that stores unsigned small int.
     * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with unsigned small int.
     */
    multiBindUSmallInt(uSmallIntArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.uint16.size * uSmallIntArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * uSmallIntArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * uSmallIntArray.length);

        uSmallIntArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.uint16.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.set(mbindBufferBuf, index * ref.types.uint16.size, element, ref.types.uint16);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_SMALLINT_UNSIGNED,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.uint16.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: uSmallIntArray.length,
        })
        return mbind;
    }

    /**
     * To bind unsigned int through an array.
     * @param {*} uIntArray is an array that stores unsigned int.
     * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with unsigned  int.
     */
    multiBindUInt(uIntArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.uint.size * uIntArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * uIntArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * uIntArray.length);

        uIntArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.uint.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {
                ref.set(mbindBufferBuf, index * ref.types.uint.size, element, ref.types.uint);
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_INT_UNSIGNED,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.uint.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: uIntArray.length,
        })
        return mbind;
    }

    /**
    * To bind unsigned big int through an array.
    * @param {*} uBigIntArray is an array that stores unsigned big int.
    * @returns A instance of struct TAOS_MULTI_BIND that contains one column's data with unsigned big int.
    */
    multiBindUBigInt(uBigIntArray) {
        let mbindBufferBuf = Buffer.alloc(ref.types.uint64.size * uBigIntArray.length);
        let mbindLengBuf = Buffer.alloc(ref.types.int.size * uBigIntArray.length);
        let mbindIsNullBuf = Buffer.alloc(ref.types.char.size * uBigIntArray.length);

        uBigIntArray.forEach((element, index) => {
            ref.set(mbindLengBuf, index * ref.types.int.size, ref.types.uint64.size, ref.types.int)
            if (element == null || element == undefined) {
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 1, ref.types.char);
            } else {

                ref.writeUInt64LE(mbindBufferBuf, index * ref.types.uint64.size, element.toString())
                ref.set(mbindIsNullBuf, index * ref.types.char.size, 0, ref.types.char);
            }

        });

        let mbind = new TAOS_MULTI_BIND({
            buffer_type: taosConst.C_BIGINT_UNSIGNED,
            buffer: mbindBufferBuf,
            buffer_length: ref.types.uint64.size,
            length: mbindLengBuf,
            is_null: mbindIsNullBuf,
            num: uBigIntArray.length,
        })
        return mbind;
    }


    // multiBJson(jsonArray) no need to support.Since till now TDengine only support json tag
    // and there is no need to support bind json tag in TAOS_MULTI_BIND.

    /**
     * 
     * @param {*} strArr an string array
     * @returns return the max length of the element in strArr in "UFT-8" encoding.
     */
    _maxUTF8StrArrLength(strArr) {
        let max = 0;
        strArr.forEach((item) => {
            let realLeng = 0;
            let itemLength = -1;
            if (item == null || item == undefined) {
                itemLength = 0;
            } else {
                itemLength = item.length;
            }

            let charCode = -1;
            for (let i = 0; i < itemLength; i++) {
                charCode = item.charCodeAt(i);
                if (charCode >= 0 && charCode <= 128) {
                    realLeng += 1;
                } else {
                    realLeng += 3;
                }
            }
            if (max < realLeng) {
                max = realLeng
            };
        });
        return max;
    }

    /**
     * 
     * @param {*} str a string.
     * @returns return the length of the input string  encoding with utf-8.
     */
    _stringUTF8Length(str) {
        let leng = 0;
        if (str == null || str == undefined) {
            leng = 0;
        } else {
            for (let i = 0; i < str.length; i++) {
                if (str.charCodeAt(i) >= 0 && str.charCodeAt(i) <= 128) {
                    leng += 1;
                } else {
                    leng += 3;
                }
            }
        }
        return leng;
    }
}
// console.log(TAOS_MULTI_BIND.size)
module.exports = { TaosMultiBind, TAOS_MULTI_BIND };