const ref = require('ref-napi');
const { TDError } = require('./error');
const { TAOS_MULTI_BIND, TaosMultiBind } = require('./taosMultiBind');

const TAOS_MULTI_BIND_SIZE = TAOS_MULTI_BIND.size;

class TaosMultiBindArr extends TaosMultiBind {
    /**
     * The constructor，initial basic parameters and alloc buffer.
     * @param {*} numOfColumns the number of column that you want to bind parameters.
     */
    constructor(numOfColumns) {
        super();
        this.taosMBindArrBuf = Buffer.alloc(numOfColumns * TAOS_MULTI_BIND_SIZE);
        this.index = 0;
        this.bound = numOfColumns;
    }

    /**
     * Used to bind boolean column's values.
     * @param {*} boolArray An array of bool value,
     * represents the bool values you want to bind.
     */
    multiBindBool(boolArray) {
        if (this.bound > this.index) {
            let mBindBool = super.multiBindBool(boolArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindBool);
            this.index += 1;
        } else {
            throw new TDError(`multiBindArrBool() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }
    }

    /**
     * Used to bind tiny int column's values.
     * @param {*} tinyIntArray An array of tiny int value.
     * represents the tiny int values you want to bind.
     */
    multiBindTinyInt(tinyIntArray) {
        if (this.bound > this.index) {
            let mBindTinyInt = super.multiBindTinyInt(tinyIntArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindTinyInt);
            this.index += 1;
        } else {
            throw new TDError(`multiBindArrTinyInt() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }
    }

    /**
     * Used to bind small int column's value.
     * @param {*} smallIntArray An array of small int values,
     * represents the small int values you want to bind.
     */
    multiBindSmallInt(smallIntArray) {
        if (this.bound > this.index) {
            let mBindSmallInt = super.multiBindSmallInt(smallIntArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindSmallInt);
            this.index += 1;
        } else {
            throw new TDError(`multiBindSmallInt() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }

    }

    /**
     * Used to bind int column's value.
     * @param {*} intArray An array of int values,
     * represents the int values you want to bind.
     */
    multiBindInt(intArray) {
        if (this.bound > this.index) {
            let mBindInt = super.multiBindInt(intArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindInt);
            this.index += 1;
        } else {
            throw new TDError(`multiBindInt() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }

    }

    /**
     * Used to bind big int column's value.
     * @param {*} bigIntArray An array of big int values,
     * represents the big int values you want to bind.
     */
    multiBindBigInt(bigIntArray) {
        if (this.bound > this.index) {
            let mBindBigInt = super.multiBindBigInt(bigIntArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindBigInt);
            this.index += 1;
        } else {
            throw new TDError(`multiBindBigInt() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }

    }

    /**
     * Used to bind float column's value.
     * @param {*} floatArray An array of float values,
     * represents the float values you want to bind.
     */
    multiBindFloat(floatArray) {
        if (this.bound > this.index) {
            let mBindFloat = super.multiBindFloat(floatArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindFloat);
            this.index += 1;
        } else {
            throw new TDError(`multiBindFloat() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }

    }

    /**
     * Used to bind double column's value.
     * @param {*} doubleArray An array of double values,
     * represents the double values you want to bind.
     */
    multiBindDouble(doubleArray) {
        if (this.bound > this.index) {
            let mBindDouble = super.multiBindDouble(doubleArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindDouble);
            this.index += 1;
        } else {
            throw new TDError(`multiBindDouble() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }

    }

    /**
     * Used to bind binary column's value.
     * @param {*} strArr An array of binary(string) values,
     * represents the binary values you want to bind.
     * Notice '' is not equal to TDengine's "null" value.
     */
    multiBindBinary(strArr) {
        if (this.bound > this.index) {
            let mBindBinary = super.multiBindBinary(strArr);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindBinary);
            this.index += 1;
        } else {
            throw new TDError(`multiBindBinary() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }
    }

    /**
     * Used to bind timestamp column's values.
     * @param {*} timestampArray An array of timestamp values,
     * represents the timestamp values you want to bind.
     */
    multiBindTimestamp(timestampArray) {
        if (this.bound > this.index) {
            let mBindTimestamp = super.multiBindTimestamp(timestampArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindTimestamp);
            this.index += 1;
        } else {
            throw new TDError(`multiBindArrTimestamp() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }
    }

    /**
     * Used to bind nchar column's value.
     * @param {*} strArr An array of nchar(string) values,
     * represents the nchar values you want to bind.
     * Notice '' is not equal to TDengine's "null" value.
     */
    multiBindNchar(strArr) {
        if (this.bound > this.index) {
            let mBindNchar = super.multiBindNchar(strArr);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindNchar);
            this.index += 1;
        } else {
            throw new TDError(`multiBindNchar() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }
    }

    /**
     * Used to bind unsigned tiny int column's value.
     * @param {*} uTinyIntArray An array of unsigned tiny int values,
     * represents the unsigned tiny int values you want to bind.
     */
    multiBindUTinyInt(uTinyIntArray) {
        if (this.bound > this.index) {
            let mBindNchar = super.multiBindUTinyInt(uTinyIntArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindNchar);
            this.index += 1;
        } else {
            throw new TDError(`multiBindUTinyInt() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }
    }

    /**
     * Used to bind unsigned small int column's value.
     * @param {*} uSmallIntArray An array of unsigned small int value,
     * represents the unsigned small int values you want to bind.
     */
    multiBindUSmallInt(uSmallIntArray) {
        if (this.bound > this.index) {
            let mBindUSmallInt = super.multiBindUSmallInt(uSmallIntArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindUSmallInt);
            this.index += 1;
        } else {
            throw new TDError(`multiBindUSmallInt() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }
    }

    /**
     * Used to bind unsigned int column's value.
     * @param {*} uIntArray An array of unsigned int column's value,
     * represents the unsigned int values you want to bind.
     */
    multiBindUInt(uIntArray) {
        if (this.bound > this.index) {
            let mBindUInt = super.multiBindUInt(uIntArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindUInt);
            this.index += 1;
        } else {
            throw new TDError(`multiBindUInt() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }
    }

    /**
     * Used to bind unsigned big int column's value.
     * @param {*} uBigIntArray An array of unsigned big int column's value,
     * represents the unsigned big int values you want to bind.
     */
    multiBindUBigInt(uBigIntArray) {
        if (this.bound > this.index) {
            let mBindUBigInt = super.multiBindUBigInt(uBigIntArray);
            TAOS_MULTI_BIND.set(this.taosMBindArrBuf, this.index * TAOS_MULTI_BIND_SIZE, mBindUBigInt);
            this.index += 1;
        } else {
            throw new TDError(`multiBindUBigInt() failed,since index:${this.index} is out of Buffer bound ${this.bound}.`)
        }
    }

    // multiBJson(jsonArray) no need to support.Since till now TDengine only support json tag
    // and there is no need to support bind json tag in TAOS_MULTI_BIND.


    /**
     * After all the parameters have been prepared and stored 
     * in the buffer, Call this method to get the buffer.
     * @returns return the buffer which stores all the parameters.
     */
    getMultiBindArr() {
        return this.taosMBindArrBuf;
    }

}
module.exports = TaosMultiBindArr;