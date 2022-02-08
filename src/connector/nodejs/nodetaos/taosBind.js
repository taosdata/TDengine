const ref = require('ref-napi');
const StructType = require('ref-struct-di')(ref);
const taosConst = require('./constants');
const { TDError } = require('./error');

var bufferType = ref.types.int32;
var buffer = ref.refType(ref.types.void);
var bufferLength = ref.types.uint64;
var length = ref.refType(ref.types.uint64);
var isNull = ref.refType(ref.types.int32);
var is_unsigned = ref.types.int;
var error = ref.refType(ref.types.void);
var u = ref.types.int64;
var allocated = ref.types.uint32;

var TAOS_BIND = StructType({
    buffer_type: bufferType,
    buffer: buffer,
    buffer_length: bufferLength,
    length: length,
    is_null: isNull,
    is_unsigned: is_unsigned,
    error: error,
    u: u,
    allocated: allocated,
});

class TaosBind {
    constructor(num) {
        this.buf = Buffer.alloc(TAOS_BIND.size * num);
        this.num = num;
        this.index = 0;
    }
    /**
     * Used to bind null value for all data types that tdengine supports.
     */
    bindNil() {
        if (!this._isOutOfBound()) {
            let nil = new TAOS_BIND({
                buffer_type: taosConst.C_NULL,
                is_null: ref.alloc(ref.types.int32, 1),
            });

            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, nil);
            this.index++
        } else {
            throw new TDError(`bindNil() failed,since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
     * 
     * @param {bool} val is not null bool value,true or false.
     */
    bindBool(val) {
        if (!this._isOutOfBound()) {
            let bl = new TAOS_BIND({
                buffer_type: taosConst.C_BOOL,
                buffer: ref.alloc(ref.types.bool, val),
                buffer_length: ref.types.bool.size,
                length: ref.alloc(ref.types.uint64, ref.types.bool.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });

            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, bl);
            this.index++
        } else {
            throw new TDError(`bindBool() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }

    }

    /**
     * 
     * @param {int8} val is a not null tinyint value.
     */
    bindTinyInt(val) {
        if (!this._isOutOfBound()) {
            let tinnyInt = new TAOS_BIND({
                buffer_type: taosConst.C_TINYINT,
                buffer: ref.alloc(ref.types.int8, val),
                buffer_length: ref.types.int8.size,
                length: ref.alloc(ref.types.uint64, ref.types.int8.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });

            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, tinnyInt);
            this.index++
        } else {
            throw new TDError(`bindTinyInt() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
     * 
     * @param {short} val is a not null small int value.
     */
    bindSmallInt(val) {
        if (!this._isOutOfBound()) {
            let samllint = new TAOS_BIND({
                buffer_type: taosConst.C_SMALLINT,
                buffer: ref.alloc(ref.types.int16, val),
                buffer_length: ref.types.int16.size,
                length: ref.alloc(ref.types.uint64, ref.types.int16.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });
            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, samllint);
            this.index++
        } else {
            throw new TDError(`bindSmallInt() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }

    }

    /**
     * 
     * @param {int} val is a not null int value.
     */
    bindInt(val) {
        if (!this._isOutOfBound()) {
            let int = new TAOS_BIND({
                buffer_type: taosConst.C_INT,
                buffer: ref.alloc(ref.types.int32, val),
                buffer_length: ref.types.int32.size,
                length: ref.alloc(ref.types.uint64, ref.types.int32.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });
            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, int);
            this.index++
        } else {
            throw new TDError(`bindInt() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }

    }

    /**
     * 
     * @param {long} val is not null big int value.
     */
    bindBigInt(val) {
        if (!this._isOutOfBound()) {
            let bigint = new TAOS_BIND({
                buffer_type: taosConst.C_BIGINT,
                buffer: ref.alloc(ref.types.int64, val.toString()),
                buffer_length: ref.types.int64.size,
                length: ref.alloc(ref.types.uint64, ref.types.int64.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });
            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, bigint);
            this.index++
        } else {
            throw new TDError(`bindBigInt() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
    * 
    * @param {float} val is a not null float value
    */
    bindFloat(val) {
        if (!this._isOutOfBound()) {
            let float = new TAOS_BIND({
                buffer_type: taosConst.C_FLOAT,
                buffer: ref.alloc(ref.types.float, val),
                buffer_length: ref.types.float.size,
                length: ref.alloc(ref.types.uint64, ref.types.float.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });
            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, float);
            this.index++
        } else {
            throw new TDError(`bindFloat() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }
    /**
     * 
     * @param {double} val is a not null double value 
     */
    bindDouble(val) {
        if (!this._isOutOfBound()) {
            let double = new TAOS_BIND({
                buffer_type: taosConst.C_DOUBLE,
                buffer: ref.alloc(ref.types.double, val),
                buffer_length: ref.types.double.size,
                length: ref.alloc(ref.types.uint64, ref.types.double.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });
            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, double);
            this.index++
        } else {
            throw new TDError(`bindDouble() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
     * 
     * @param {string} val is a string.
     */
    bindBinary(val) {
        let cstringBuf = ref.allocCString(val, 'utf-8');
        if (!this._isOutOfBound()) {
            let binary = new TAOS_BIND({
                buffer_type: taosConst.C_BINARY,
                buffer: cstringBuf,
                buffer_length: cstringBuf.length,
                length: ref.alloc(ref.types.uint64, cstringBuf.length - 1),
                is_null: ref.alloc(ref.types.int32, 0),
            });
            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, binary);
            this.index++
        } else {
            throw new TDError(`bindBinary() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
     * 
     * @param {long} val is a not null timestamp(long) values.
     */
    bindTimestamp(val) {
        let ts = new TAOS_BIND({
            buffer_type: taosConst.C_TIMESTAMP,
            buffer: ref.alloc(ref.types.int64, val),
            buffer_length: ref.types.int64.size,
            length: ref.alloc(ref.types.uint64, ref.types.int64.size),
            is_null: ref.alloc(ref.types.int32, 0),
        });

        TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, ts);
        this.index++
    }

    /**
     * 
     * @param {string} val is a string.
     */
    bindNchar(val) {
        let cstringBuf = ref.allocCString(val, 'utf-8');
        if (!this._isOutOfBound()) {
            let nchar = new TAOS_BIND({
                buffer_type: taosConst.C_NCHAR,
                buffer: cstringBuf,
                buffer_length: cstringBuf.length,
                length: ref.alloc(ref.types.uint64, cstringBuf.length - 1),
                is_null: ref.alloc(ref.types.int32, 0),
            });
            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, nchar);
            this.index++
        } else {
            throw new TDError(`bindNchar() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
    * 
    * @param {uint8} val is a not null unsinged tinyint value.
    */
    bindUTinyInt(val) {
        if (!this._isOutOfBound()) {
            let uTinyInt = new TAOS_BIND({
                buffer_type: taosConst.C_TINYINT_UNSIGNED,
                buffer: ref.alloc(ref.types.uint8, val),
                buffer_length: ref.types.uint8.size,
                length: ref.alloc(ref.types.uint64, ref.types.uint8.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });

            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, uTinyInt);
            this.index++
        } else {
            throw new TDError(`bindUTinyInt() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
     * 
     * @param {uint16} val is a not null unsinged smallint value.
     */
    bindUSmallInt(val) {
        if (!this._isOutOfBound()) {
            let uSmallInt = new TAOS_BIND({
                buffer_type: taosConst.C_SMALLINT_UNSIGNED,
                buffer: ref.alloc(ref.types.uint16, val),
                buffer_length: ref.types.uint16.size,
                length: ref.alloc(ref.types.uint64, ref.types.uint16.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });

            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, uSmallInt);
            this.index++
        } else {
            throw new TDError(`bindUSmallInt() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
     * 
     * @param {uint32} val is a not null unsinged int value.
     */
    bindUInt(val) {
        if (!this._isOutOfBound()) {
            let uInt = new TAOS_BIND({
                buffer_type: taosConst.C_INT_UNSIGNED,
                buffer: ref.alloc(ref.types.uint32, val),
                buffer_length: ref.types.uint32.size,
                length: ref.alloc(ref.types.uint64, ref.types.uint32.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });

            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, uInt);
            this.index++
        } else {
            throw new TDError(`bindUInt() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
     * 
     * @param {uint64} val is a not null unsinged bigint value.
     */
    bindUBigInt(val) {
        if (!this._isOutOfBound()) {
            let uBigInt = new TAOS_BIND({
                buffer_type: taosConst.C_BIGINT_UNSIGNED,
                buffer: ref.alloc(ref.types.uint64, val.toString()),
                buffer_length: ref.types.uint64.size,
                length: ref.alloc(ref.types.uint64, ref.types.uint64.size),
                is_null: ref.alloc(ref.types.int32, 0),
            });

            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, uBigInt);
            this.index++
        } else {
            throw new TDError(`bindUBigInt() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }
    }

    /**
     * 
     * @param {jsonStr} val is a json string. Such as '{\"key1\":\"taosdata\"}'
     */
    bindJson(val) {
        let cstringBuf = ref.allocCString(val, 'utf-8');
        if (!this._isOutOfBound()) {
            let jsonType = new TAOS_BIND({
                buffer_type: taosConst.C_JSON_TAG,
                buffer: cstringBuf,
                buffer_length: cstringBuf.length,
                length: ref.alloc(ref.types.uint64, cstringBuf.length - 1),
                is_null: ref.alloc(ref.types.int32, 0),
            });

            TAOS_BIND.set(this.buf, this.index * TAOS_BIND.size, jsonType);
            this.index++
        } else {
            throw new TDError(`bindJson() failed with ${val},since index:${this.index} is out of Buffer bound ${this.num}.`);
        }

    }

    /**
     * 
     * @returns binded buffer.
     */
    getBind() {
        return this.buf;
    }

    _isOutOfBound() {
        if (this.num > this.index) {
            return false;
        } else {
            return true;
        }
    }
}
module.exports = TaosBind;
