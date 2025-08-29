import struct

def access_bit(data, num):
    base = num // 8
    shift = 7 - num % 8
    
    return (data[base] >> shift) & 0x1

def unpack_fix_data(data, entry_type):
    taos_type_null = 0
    taos_type_bool = 1
    taos_type_tinyint = 2
    taos_type_smallint = 3
    taos_type_int = 4
    taos_type_bigint = 5
    taos_type_float = 6
    taos_type_double = 7
    taos_type_timestamp = 9
    taos_type_utinyint = 11
    taos_type_usmallint = 12
    taos_type_uint = 13
    taos_type_ubigint = 14
    taos_type_decimal = 17
    taos_type_blob = 18
    taos_type_mediumblob = 19
    taos_type_decimal64 = 21
    # taos_type_varchar = 8
    # taos_type_binary = taos_type_varchar
    # taos_type_nchar = 10
    # taos_type_json = 15
    # taos_type_varbinary = 16
    # taos_type_geometry = 20
    
    if entry_type == taos_type_null:
        return 'null'
    elif entry_type == taos_type_bool:
        return data
    elif entry_type == taos_type_tinyint:
        return data
    elif entry_type == taos_type_smallint:
        return int(struct.unpack('<h', data)[0])
    elif entry_type == taos_type_int:
        return int(struct.unpack('<i', data)[0])
    elif entry_type == taos_type_bigint:
        return int(struct.unpack('<q', data)[0])
    elif entry_type == taos_type_float:
        return int(struct.unpack('<f', data)[0])
    elif entry_type == taos_type_double:
        return int(struct.unpack('<d', data)[0])
    elif entry_type == taos_type_timestamp:
        return int(struct.unpack('<q', data)[0])
    elif entry_type == taos_type_utinyint:
        return data
    elif entry_type == taos_type_usmallint:
        return int(struct.unpack('<H', data)[0])
    elif entry_type == taos_type_uint:
        return int(struct.unpack('<I', data)[0])
    elif entry_type == taos_type_ubigint:
        return int(struct.unpack('<Q', data)[0])
    else:
        return f'not supported yet: {data}'

def parse(data, start_idx):
    taos_type_decimal = 17
    taos_type_decimal64 = 21
    taos_type_varchar = 8
    taos_type_nchar = 10
    taos_type_json = 15
    taos_type_varbinary = 16
    taos_type_geometry = 20
    
    taos_type_var = [
        taos_type_varchar,
        taos_type_varbinary,
        taos_type_nchar,
        taos_type_json,
        taos_type_geometry,
    ]
    
    block = {}

    version = int(struct.unpack('<i', data[start_idx:start_idx + 4])[0])
    if version != 2:
        print("block version: ", start_idx, version)
        raise Exception("rawb format error")

    blen = int(struct.unpack('<i', data[start_idx + 4:start_idx + 8])[0])
    brows = int(struct.unpack('<i', data[start_idx + 8:start_idx + 12])[0])
    bcols = int(struct.unpack('<i', data[start_idx + 12:start_idx + 16])[0])
    bflag = int(struct.unpack('<i', data[start_idx + 16:start_idx + 20])[0])
    bgroup_id = int(struct.unpack('<Q', data[start_idx + 20:start_idx + 28])[0])

    # print("version: ", version, blen, brows, bcols, bflag, bgroup_id)
    bcols_schema = []
    col_schema_start = start_idx + 28
    for i in range(bcols):
        col_schema = {}
        
        col_type = data[col_schema_start + i * 5]

        col_size = 0
        size_start = col_schema_start + i * 5 + 1
        
        dec_bytes = 0
        dec_precision = 0
        dec_scale = 0
        if col_type == taos_type_decimal or col_type == taos_type_decimal64:
            dec_bytes = data[size_start]
            dec_precision = data[size_start + 2]
            dec_scale = data[size_start + 3]

            col_schema["dec_bytes"] = dec_bytes
            col_schema["dec_precision"] = dec_precision
            col_schema["dec_scale"] = dec_scale
        else:
            col_size = int(struct.unpack('<i', data[size_start:size_start + 4])[0])

        col_schema["type"] = col_type
        col_schema["size"] = col_size
        
        bcols_schema.append(col_schema)

    bcols_len = []
    col_len_start = col_schema_start + bcols * 5
    for i in range(bcols):
        size_start = col_len_start + i * 4
        bcols_len.append(int(struct.unpack('<i', data[size_start:size_start + 4])[0]))

    # print("collen: ", bcols_schema, bcols_len)
    bcols_data = []
    col_data_start = col_len_start + bcols * 4
    col_offset_start = col_data_start
    for i in range(bcols):
        # parse cols' data
        col_offset = []
        col_bitmap = []
        col_data = []
        if bcols_schema[i]["type"] in taos_type_var:
            offset_start = col_offset_start
            data_start = offset_start + 4 * brows
            rows_size = 0
            for r in range(brows):
                # print("xxx: ", col_len_start, col_data_start, offset_start, type(data), "0x", data.hex(), data[offset_start + 4 * r:offset_start + 4 * r + 4])
                col_offset.append(int(struct.unpack('<i', data[offset_start + 4 * r:offset_start + 4 * r + 4])[0]))
                if col_offset[r] == -1:
                    bcols_data.append('null var')
                else:
                    # use offset to access row data from data_start
                    # bcols_data.append()
                    row_start = data_start + col_offset[r]
                    var_start = row_start + 2
                    row_type = bcols_schema[i]["type"]
                    print(i, row_type, bcols, brows, r, col_offset[r], data_start, row_start, var_start, data[row_start:var_start])
                    varlen = int(struct.unpack('<H', data[row_start:var_start])[0])
                    print(" ", row_type, bcols, brows, r, col_offset[r], var_start, varlen, "0x", data[row_start:var_start].hex())
                    bcols_data.append(str(data[var_start:var_start + varlen], encoding='utf-8'))

                    rows_size += varlen + 2

            col_offset_start += data_start - offset_start + rows_size
        else:
            # bitmap
            bitmap_start = col_offset_start
            data_start = col_offset_start + (brows + 7) // 8
            entry_size = bcols_schema[i]["size"]
            entry_type = bcols_schema[i]["type"]

            for r in range(brows):
                bit_null = access_bit(data[bitmap_start:data_start], r)
                if bit_null:
                    bcols_data.append('null')
                else:
                    # unpack from data_start w/ type info
                    entry_start = data_start + r * entry_size
                    entry_end = entry_start + entry_size
                    bcols_data.append(unpack_fix_data(data[entry_start:entry_end], entry_type))

            col_offset_start += data_start - bitmap_start + entry_size * brows
    
    block["version"] = version
    block["len"] = blen
    block["rows"] = brows
    block["cols"] = bcols
    block["flag"] = bflag
    block["group_id"] = bgroup_id
    block["cols_schema"] = bcols_schema
    block["cols_len"] = bcols_len
    block["cols_data"] = bcols_data

    return block
    
def load(rba): # rawblock list
    bl = []
    
    rbalen = len(rba)

    start_idx = 0
    while rbalen > 0:
        block = parse(rba, start_idx)
        blen = block["len"]

        start_idx += blen
        rbalen -= blen

        bl.append(block)

    return bl
