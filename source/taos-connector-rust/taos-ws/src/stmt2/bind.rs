use byteorder::{ByteOrder, LittleEndian};
use taos_query::common::{BorrowedValue, ColumnView, Value};
use taos_query::stmt2::Stmt2BindParam;
use taos_query::RawResult;

use crate::query::messages::{BindType, ReqId, Stmt2Field, StmtId};

pub(super) const REQ_ID_POS: usize = 0;
pub(super) const STMT_ID_POS: usize = REQ_ID_POS + 8;
const ACTION_POS: usize = STMT_ID_POS + 8;
const VERSION_POS: usize = ACTION_POS + 8;
const COL_IDX_POS: usize = VERSION_POS + 2;
const FIXED_HEADER_LEN: usize = COL_IDX_POS + 4;

const TOTAL_LENGTH_POS: usize = 0;
const TABLE_COUNT_POS: usize = TOTAL_LENGTH_POS + 4;
const TAG_COUNT_POS: usize = TABLE_COUNT_POS + 4;
const COL_COUNT_POS: usize = TAG_COUNT_POS + 4;
const TABLE_NAMES_OFFSET_POS: usize = COL_COUNT_POS + 4;
const TAGS_OFFSET_POS: usize = TABLE_NAMES_OFFSET_POS + 4;
const COLS_OFFSET_POS: usize = TAGS_OFFSET_POS + 4;
const DATA_POS: usize = COLS_OFFSET_POS + 4;

// `TC` (Tag and Column) is an abbreviation used to represent tag and column.
const TC_DATA_TOTAL_LENGTH_POS: usize = 0;
const TC_DATA_TYPE_POS: usize = TC_DATA_TOTAL_LENGTH_POS + 4;
const TC_DATA_NUM_POS: usize = TC_DATA_TYPE_POS + 4;
const TC_DATA_IS_NULL_POS: usize = TC_DATA_NUM_POS + 4;

const ACTION: u64 = 9;
const VERSION: u16 = 1;
const COL_IDX: i32 = -1;

pub(super) fn bind_params_to_bytes(
    params: &[Stmt2BindParam],
    req_id: ReqId,
    stmt_id: StmtId,
    is_insert: bool,
    fields: Option<&Vec<Stmt2Field>>,
    fields_count: usize,
) -> RawResult<Vec<u8>> {
    if params.is_empty() {
        return Err("no params to bind".into());
    }

    let mut need_tbnames = false;
    let mut need_tags = false;
    let mut need_cols = false;

    let table_cnt = params.len();
    let mut tag_cnt = 0;
    let mut col_cnt = 0;

    if is_insert {
        if fields.is_none() || fields.unwrap().is_empty() {
            return Err("fields is empty".into());
        }

        for field in fields.unwrap() {
            match field.bind_type {
                BindType::TableName => {
                    need_tbnames = true;
                }
                BindType::Tag => {
                    need_tags = true;
                    tag_cnt += 1;
                }
                BindType::Column => {
                    need_cols = true;
                    col_cnt += 1;
                }
            }
        }
    } else {
        need_cols = true;
        col_cnt = fields_count;
    }

    tracing::trace!("need_tbnames: {need_tbnames}, need_tags: {need_tags}, need_cols: {need_cols}");
    tracing::trace!("table_cnt: {table_cnt}, tag_cnt: {tag_cnt}, col_cnt: {col_cnt}");

    let mut tbname_lens = vec![];
    let mut tbname_buf_len = 0;
    if need_tbnames {
        tbname_lens = get_tbname_lens(params)?;
        tbname_buf_len = tbname_lens.iter().map(|&x| x as usize).sum();
    }

    let mut tag_lens = vec![];
    let mut tag_buf_len = 0;
    if need_tags {
        tag_lens = get_tag_lens(params, tag_cnt)?;
        tag_buf_len = tag_lens.iter().map(|&x| x as usize).sum();
    }

    let mut col_lens = vec![];
    let mut col_buf_len = 0;
    if need_cols {
        col_lens = get_col_lens(params, col_cnt)?;
        col_buf_len = col_lens.iter().map(|&x| x as usize).sum();
    }

    let tbname_total_len = tbname_lens.len() * 2 + tbname_buf_len;
    let tag_total_len = tag_lens.len() * 4 + tag_buf_len;
    let col_total_len = col_lens.len() * 4 + col_buf_len;
    let total_len = DATA_POS + tbname_total_len + tag_total_len + col_total_len;

    tracing::trace!("tbname_total_len: {tbname_total_len}, tbname_buf_len: {tbname_buf_len}");
    tracing::trace!("tag_total_len: {tag_total_len}, tag_buf_len: {tag_buf_len}");
    tracing::trace!("col_total_len: {col_total_len}, col_buf_len: {col_buf_len}");
    tracing::trace!("total_len: {total_len}");

    let mut data = vec![0u8; FIXED_HEADER_LEN + total_len];
    write_fixed_headers(&mut data, req_id, stmt_id);

    let bytes = &mut data[FIXED_HEADER_LEN..];
    LittleEndian::write_u32(&mut bytes[TOTAL_LENGTH_POS..], total_len as _);
    LittleEndian::write_i32(&mut bytes[TABLE_COUNT_POS..], table_cnt as _);
    LittleEndian::write_i32(&mut bytes[TAG_COUNT_POS..], tag_cnt as _);
    LittleEndian::write_i32(&mut bytes[COL_COUNT_POS..], col_cnt as _);

    if need_tbnames {
        LittleEndian::write_u32(&mut bytes[TABLE_NAMES_OFFSET_POS..], DATA_POS as _);
        write_tbnames(&mut bytes[DATA_POS..], params, &tbname_lens);
    }

    if need_tags {
        let tags_offset = DATA_POS + tbname_total_len;
        LittleEndian::write_u32(&mut bytes[TAGS_OFFSET_POS..], tags_offset as _);
        write_tags(&mut bytes[tags_offset..], params, &tag_lens);
    }

    if need_cols {
        let cols_offset = DATA_POS + tbname_total_len + tag_total_len;
        LittleEndian::write_u32(&mut bytes[COLS_OFFSET_POS..], cols_offset as _);
        write_cols(&mut bytes[cols_offset..], params, &col_lens);
    }

    Ok(data)
}

fn get_tbname_lens(params: &[Stmt2BindParam]) -> RawResult<Vec<u16>> {
    let mut tbname_lens = vec![0u16; params.len()];
    for (i, param) in params.iter().enumerate() {
        if param.table_name().is_none_or(|s| s.is_empty()) {
            return Err("table name is empty".into());
        }
        let tbname = param.table_name().unwrap();
        // Add 1 because the table name ends with '\0'
        tbname_lens[i] = (tbname.len() + 1) as _;
    }
    Ok(tbname_lens)
}

fn get_tag_lens(params: &[Stmt2BindParam], tag_cnt: usize) -> RawResult<Vec<u32>> {
    let mut tag_lens = vec![0u32; params.len()];
    for (i, param) in params.iter().enumerate() {
        if param.tags().is_none() {
            return Err("tags is empty".into());
        }

        let tags = param.tags().unwrap();
        if tags.len() != tag_cnt {
            return Err("tags len mismatch".into());
        }

        let mut len = 0;
        for tag in tags {
            let have_len = tag.ty().fixed_length() == 0;
            len += get_tc_header_len(1, have_len);
            len += get_tag_data_len(tag);
        }
        tag_lens[i] = len as _;
    }
    Ok(tag_lens)
}

fn get_col_lens(params: &[Stmt2BindParam], col_cnt: usize) -> RawResult<Vec<u32>> {
    let mut col_lens = vec![0u32; params.len()];
    for (i, param) in params.iter().enumerate() {
        if param.columns().is_none() {
            return Err("columns is empty".into());
        }

        let cols = param.columns().unwrap();
        if cols.len() != col_cnt {
            return Err("columns len mismatch".into());
        }

        let mut len = 0;
        for col in cols {
            let have_len = col.as_ty().fixed_length() == 0;
            len += get_tc_header_len(col.len(), have_len);
            len += get_col_data_len(col);
        }
        col_lens[i] = len as _;
    }
    Ok(col_lens)
}

fn get_tc_header_len(num: usize, have_len: bool) -> usize {
    // TotalLength(4) + Type(4) + Num(4) + HaveLength(1) + BufferLength(4) + IsNull(num)
    let mut len = 17 + num;
    if have_len {
        // Length(num * 4)
        len += num * 4;
    }
    len
}

fn get_tag_data_len(tag: &Value) -> usize {
    use Value::*;
    match tag {
        Null(_) => 0,
        VarChar(v) | NChar(v) => v.len(),
        Blob(v) | MediumBlob(v) | VarBinary(v) | Geometry(v) => v.len(),
        Json(v) => serde_json::to_vec(v).unwrap().len(),
        _ => tag.ty().fixed_length(),
    }
}

fn get_col_data_len(col: &ColumnView) -> usize {
    if check_col_is_null(col) {
        return 0;
    }

    let mut len = 0;

    macro_rules! view_iter {
        ($view:ident) => {
            for val in $view.iter() {
                if let Some(v) = val {
                    len += v.len();
                }
            }
        };
    }

    use ColumnView::*;
    match col {
        VarChar(view) => view_iter!(view),
        NChar(view) => view_iter!(view),
        VarBinary(view) => view_iter!(view),
        Geometry(view) => view_iter!(view),
        Blob(view) => view_iter!(view),
        Json(_) => panic!("column does not support json type"),
        _ => len = col.as_ty().fixed_length() * col.len(),
    }

    len
}

fn check_col_is_null(col: &ColumnView) -> bool {
    for val in col.iter() {
        if !val.is_null() {
            return false;
        }
    }
    true
}

fn write_fixed_headers(bytes: &mut [u8], req_id: ReqId, stmt_id: StmtId) {
    LittleEndian::write_u64(&mut bytes[REQ_ID_POS..], req_id);
    LittleEndian::write_u64(&mut bytes[STMT_ID_POS..], stmt_id);
    LittleEndian::write_u64(&mut bytes[ACTION_POS..], ACTION);
    LittleEndian::write_u16(&mut bytes[VERSION_POS..], VERSION);
    LittleEndian::write_i32(&mut bytes[COL_IDX_POS..], COL_IDX);
}

fn write_tbnames(bytes: &mut [u8], params: &[Stmt2BindParam], tbname_lens: &[u16]) {
    // Write TableNameLength
    let mut offset = 0;
    for len in tbname_lens {
        LittleEndian::write_u16(&mut bytes[offset..], *len);
        offset += 2;
    }

    // Write TableNameBuffer
    for param in params {
        let tbname = param.table_name().unwrap();
        let len = tbname.len();
        bytes[offset..offset + len].copy_from_slice(tbname.as_bytes());
        // Add 1 because the table name end with '\0'
        offset += len + 1;
    }
}

fn write_tags(bytes: &mut [u8], params: &[Stmt2BindParam], tag_lens: &[u32]) {
    // Write TagsDataLength
    let mut offset = 0;
    for len in tag_lens {
        LittleEndian::write_u32(&mut bytes[offset..], *len);
        offset += 4;
    }

    // Write TagsBuffer
    for param in params {
        for tag in param.tags().unwrap() {
            offset += write_tag(&mut bytes[offset..], tag);
        }
    }
}

fn write_tag(bytes: &mut [u8], tag: &Value) -> usize {
    let ty = tag.ty();
    let is_null = tag.is_null();
    let have_len = ty.fixed_length() == 0;
    let header_len = get_tc_header_len(1, have_len);

    let offset = header_len;
    let mut buf_len = 0;
    if !is_null {
        buf_len = ty.fixed_length();
        let val = tag.to_borrowed_value();

        // Write Buffer
        use BorrowedValue::*;
        match val {
            Bool(v) => bytes[offset] = v as _,
            TinyInt(v) => bytes[offset] = v as _,
            UTinyInt(v) => bytes[offset] = v,
            SmallInt(v) => LittleEndian::write_i16(&mut bytes[offset..], v as _),
            USmallInt(v) => LittleEndian::write_u16(&mut bytes[offset..], v),
            Int(v) => LittleEndian::write_i32(&mut bytes[offset..], v as _),
            UInt(v) => LittleEndian::write_u32(&mut bytes[offset..], v),
            BigInt(v) => LittleEndian::write_i64(&mut bytes[offset..], v as _),
            UBigInt(v) => LittleEndian::write_u64(&mut bytes[offset..], v),
            Float(v) => LittleEndian::write_f32(&mut bytes[offset..], v),
            Double(v) => LittleEndian::write_f64(&mut bytes[offset..], v),
            Timestamp(v) => LittleEndian::write_i64(&mut bytes[offset..], v.as_raw_i64() as _),
            VarChar(v) => {
                buf_len = v.len();
                bytes[offset..offset + v.len()].copy_from_slice(v.as_bytes());
            }
            NChar(v) => {
                buf_len = v.len();
                bytes[offset..offset + v.len()].copy_from_slice(v.as_bytes());
            }
            Json(v) | VarBinary(v) | Geometry(v) => {
                buf_len = v.len();
                bytes[offset..offset + v.len()].copy_from_slice(&v);
            }
            _ => unreachable!("Unsupported Type"),
        };

        // Write BufferLength
        LittleEndian::write_u32(&mut bytes[header_len - 4..], buf_len as _);
    }

    let total_len = header_len + buf_len;

    LittleEndian::write_u32(&mut bytes[TC_DATA_TOTAL_LENGTH_POS..], total_len as _);
    LittleEndian::write_u32(&mut bytes[TC_DATA_TYPE_POS..], ty as _);
    LittleEndian::write_u32(&mut bytes[TC_DATA_NUM_POS..], 1);

    if is_null {
        bytes[TC_DATA_IS_NULL_POS] = 1;
    }

    if have_len {
        // Write HaveLength
        bytes[TC_DATA_IS_NULL_POS + 1] = 1;
        // Write Length
        LittleEndian::write_u32(&mut bytes[TC_DATA_IS_NULL_POS + 2..], buf_len as _);
    }

    total_len
}

fn write_cols(bytes: &mut [u8], params: &[Stmt2BindParam], col_lens: &[u32]) {
    // Write ColDataLength
    let mut offset = 0;
    for len in col_lens {
        LittleEndian::write_u32(&mut bytes[offset..], *len);
        offset += 4;
    }

    // Write ColBuffer
    for param in params {
        let cols = param.columns().unwrap();
        for col in cols {
            offset += write_col(&mut bytes[offset..], col);
        }
    }
}

fn write_col(bytes: &mut [u8], col: &ColumnView) -> usize {
    let num = col.len();
    let ty = col.as_ty();
    let is_null = check_col_is_null(col);
    let have_len = ty.fixed_length() == 0;
    let header_len = get_tc_header_len(num, have_len);
    let mut buf_offset = header_len;

    if is_null {
        // Write IsNull
        for b in bytes.iter_mut().skip(TC_DATA_IS_NULL_POS).take(num) {
            *b = 1;
        }
    } else {
        // Write IsNull, Length and Buffer
        let mut is_null_offset = TC_DATA_IS_NULL_POS;
        // TC_DATA_IS_NULL_POS + IsNull(num) + HaveLength(1)
        let mut len_offset = is_null_offset + num + 1;

        macro_rules! fixed_view_iter {
            ($view:ident, $ty:ty) => {
                for val in $view.iter() {
                    if val.is_none() {
                        bytes[is_null_offset] = 1;
                    }
                    let size = std::mem::size_of::<$ty>();
                    bytes[buf_offset..buf_offset + size]
                        .copy_from_slice(&val.unwrap_or_default().to_le_bytes());
                    buf_offset += size;
                    is_null_offset += 1;
                }
            };
        }

        macro_rules! variable_view_iter {
            ($view:ident) => {
                for val in $view.iter() {
                    let mut len = 0;
                    match val {
                        Some(v) => {
                            len = v.len();
                            bytes[buf_offset..buf_offset + len].copy_from_slice(v.as_bytes());
                            buf_offset += len;
                        }
                        None => bytes[is_null_offset] = 1,
                    }
                    LittleEndian::write_i32(&mut bytes[len_offset..], len as _);
                    is_null_offset += 1;
                    len_offset += 4;
                }
            };
        }

        use ColumnView::*;
        match col {
            Bool(view) => {
                for val in view.iter() {
                    match val {
                        Some(b) => bytes[buf_offset] = b as _,
                        None => bytes[is_null_offset] = 1,
                    }
                    buf_offset += 1;
                    is_null_offset += 1;
                }
            }
            Timestamp(view) => {
                for val in view.iter() {
                    let ts = match val {
                        Some(ts) => ts.as_raw_i64(),
                        None => {
                            bytes[is_null_offset] = 1;
                            0
                        }
                    };
                    LittleEndian::write_i64(&mut bytes[buf_offset..], ts);
                    buf_offset += 8;
                    is_null_offset += 1;
                }
            }
            TinyInt(view) => fixed_view_iter!(view, i8),
            UTinyInt(view) => fixed_view_iter!(view, u8),
            SmallInt(view) => fixed_view_iter!(view, i16),
            USmallInt(view) => fixed_view_iter!(view, u16),
            Int(view) => fixed_view_iter!(view, i32),
            UInt(view) => fixed_view_iter!(view, u32),
            BigInt(view) => fixed_view_iter!(view, i64),
            UBigInt(view) => fixed_view_iter!(view, u64),
            Float(view) => fixed_view_iter!(view, f32),
            Double(view) => fixed_view_iter!(view, f64),
            VarChar(view) => variable_view_iter!(view),
            NChar(view) => variable_view_iter!(view),
            Json(view) => variable_view_iter!(view),
            VarBinary(view) => variable_view_iter!(view),
            Geometry(view) => variable_view_iter!(view),
            Blob(view) => variable_view_iter!(view),
            Decimal(_) | Decimal64(_) => unimplemented!("decimal type is unsupported in stmt2"),
        }
    }

    let total_len = buf_offset;

    LittleEndian::write_u32(&mut bytes[TC_DATA_TOTAL_LENGTH_POS..], total_len as _);
    LittleEndian::write_u32(&mut bytes[TC_DATA_TYPE_POS..], ty as _);
    LittleEndian::write_u32(&mut bytes[TC_DATA_NUM_POS..], num as _);

    if have_len {
        // Write HaveLenght
        bytes[TC_DATA_IS_NULL_POS + num] = 1;
    }

    if !is_null {
        // Write BufferLength
        let buf_len = total_len - header_len;
        LittleEndian::write_u32(&mut bytes[header_len - 4..], buf_len as _);
    }

    total_len
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use taos_query::common::{ColumnView, Timestamp, Ty, Value};

    use super::{bind_params_to_bytes, Stmt2BindParam};
    use crate::query::messages::BindType;
    use crate::stmt2::Stmt2Field;

    #[test]
    fn test_bind_params_to_bytes_with_tbnames() -> anyhow::Result<()> {
        let param1 = Stmt2BindParam::new(Some("test1".to_owned()), None, None);
        let param2 = Stmt2BindParam::new(Some("test2".to_owned()), None, None);
        let param3 = Stmt2BindParam::new(Some("test3".to_owned()), None, None);
        let params = [param1, param2, param3];

        let fields = vec![Stmt2Field {
            name: "".to_string(),
            field_type: 1,
            precision: 0,
            scale: 0,
            bytes: 131584,
            bind_type: BindType::TableName,
        }];

        let res = bind_params_to_bytes(&params, 100, 200, true, Some(&fields), 0)?;

        #[rustfmt::skip]
        let expected = [
            // fixed headers
            0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // req_id
            0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // stmt_id
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // action
            0x01, 0x00, // version
            0xff, 0xff, 0xff, 0xff, // col_idx

            // data
            0x34, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // TableCount
            0x00, 0x00, 0x00, 0x00, // TagCount
            0x00, 0x00, 0x00, 0x00, // ColCount
            0x1c, 0x00, 0x00, 0x00, // TableNamesOffset
            0x00, 0x00, 0x00, 0x00, // TagsOffset
            0x00, 0x00, 0x00, 0x00, // ColsOffset

            // table names
            // TableNameLength
            0x06, 0x00,
            0x06, 0x00,
            0x06, 0x00,
            // TableNameBuffer
            0x74, 0x65, 0x73, 0x74, 0x31, 0x00,
            0x74, 0x65, 0x73, 0x74, 0x32, 0x00,
            0x74, 0x65, 0x73, 0x74, 0x33, 0x00,
        ];

        assert_eq!(res, expected);

        Ok(())
    }

    #[test]
    fn test_bind_params_to_bytes_with_tags() -> anyhow::Result<()> {
        let tags = vec![
            Value::Timestamp(Timestamp::Milliseconds(1726803356466)),
            Value::Bool(true),
            Value::TinyInt(1),
            Value::SmallInt(2),
            Value::Int(3),
            Value::BigInt(4),
            Value::Float(5.5),
            Value::Double(6.6),
            Value::UTinyInt(7),
            Value::USmallInt(8),
            Value::UInt(9),
            Value::UBigInt(10),
            Value::VarChar("varchar".to_string()),
            Value::NChar("nchar".to_string()),
            Value::Json(serde_json::json!({"key": "value"})),
            Value::VarBinary(Bytes::from("varbinary".as_bytes())),
            Value::Geometry(Bytes::from(vec![
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            ])),
        ];

        let param = Stmt2BindParam::new(None, Some(tags), None);
        let params = [param];

        let fields = vec![
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
        ];

        let res = bind_params_to_bytes(&params, 100, 200, true, Some(&fields), 0)?;

        #[rustfmt::skip]
        let expected = [
            // fixed headers
            0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // req_id
            0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // stmt_id
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // action
            0x01, 0x00, // version
            0xff, 0xff, 0xff, 0xff, // col_idx

            // data
            0xd2, 0x01, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // TableCount
            0x11, 0x00, 0x00, 0x00, // TagCount
            0x00, 0x00, 0x00, 0x00, // ColCount
            0x00, 0x00, 0x00, 0x00, // TableNamesOffset
            0x1c, 0x00, 0x00, 0x00, // TagsOffset
            0x00, 0x00, 0x00, 0x00, // ColsOffset

            // tags
            // TagsDataLength
            0xb2, 0x01, 0x00, 0x00,
            // TagsBuffer
            // table 0 tags
            // tag 0
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x09, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, // Buffer

            // tag 1
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // tag 2
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x02, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // tag 3
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x02, 0x00, // Buffer

            // tag 4
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x04, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x03, 0x00, 0x00, 0x00, // Buffer

            // tag 5
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x05, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // tag 6
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x06, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x00, 0x00, 0xb0, 0x40, // Buffer

            // tag 7
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x07, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0x40, // Buffer

            // tag 8
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x0b, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x07, // Buffer

            // tag 9
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x0c, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x08, 0x00, // Buffer

            // tag 10
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0d, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x09, 0x00, 0x00, 0x00, // Buffer

            // tag 11
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x0e, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // tag 12
            0x1d, 0x00, 0x00, 0x00, // TotalLength
            0x08, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x07, 0x00, 0x00, 0x00, // Length
            0x07, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72, // Buffer

            // tag 13
            0x1b, 0x00, 0x00, 0x00, // TotalLength
            0x0a, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x05, 0x00, 0x00, 0x00, // Length
            0x05, 0x00, 0x00, 0x00, // BufferLength
            0x6e, 0x63, 0x68, 0x61, 0x72, // Buffer

            // tag 14
            0x25, 0x00, 0x00, 0x00, // TotalLength
            0x0f, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x0f, 0x00, 0x00, 0x00, // Length
            0x0f, 0x00, 0x00, 0x00, // BufferLength
            0x7b, 0x22, 0x6b, 0x65, 0x79, 0x22, 0x3a, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x7d, // Buffer

            // tag 15
            0x1f, 0x00, 0x00, 0x00, // TotalLength
            0x10, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x09, 0x00, 0x00, 0x00, // Length
            0x09, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, // Buffer

            // tag 16
            0x2b, 0x00, 0x00, 0x00, // TotalLength
            0x14, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x15, 0x00, 0x00, 0x00, // Length
            0x15, 0x00, 0x00, 0x00, // BufferLength
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // Buffer
        ];

        assert_eq!(res, expected);

        Ok(())
    }

    #[test]
    fn test_bind_params_to_bytes_with_null_tags() -> anyhow::Result<()> {
        let tags = vec![
            Value::Null(Ty::Timestamp),
            Value::Null(Ty::Bool),
            Value::Null(Ty::TinyInt),
            Value::Null(Ty::SmallInt),
            Value::Null(Ty::Int),
            Value::Null(Ty::BigInt),
            Value::Null(Ty::Float),
            Value::Null(Ty::Double),
            Value::Null(Ty::UTinyInt),
            Value::Null(Ty::USmallInt),
            Value::Null(Ty::UInt),
            Value::Null(Ty::UBigInt),
            Value::Null(Ty::VarChar),
            Value::Null(Ty::NChar),
            Value::Null(Ty::Json),
            Value::Null(Ty::VarBinary),
            Value::Null(Ty::Geometry),
        ];

        let param = Stmt2BindParam::new(None, Some(tags), None);
        let params = [param];

        let fields = vec![
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
        ];

        let res = bind_params_to_bytes(&params, 100, 200, true, Some(&fields), 0)?;

        #[rustfmt::skip]
        let expected = [
            // fixed headers
            0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // req_id
            0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // stmt_id
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // action
            0x01, 0x00, // version
            0xff, 0xff, 0xff, 0xff, // col_idx

            // data
            0x66, 0x01, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // TableCount
            0x11, 0x00, 0x00, 0x00, // TagCount
            0x00, 0x00, 0x00, 0x00, // ColCount
            0x00, 0x00, 0x00, 0x00, // TableNamesOffset
            0x1c, 0x00, 0x00, 0x00, // TagsOffset
            0x00, 0x00, 0x00, 0x00, // ColsOffset

            // tags
            // TagsDataLength
            0x46, 0x01, 0x00, 0x00,
            // TagsBuffer
            // table 0 tags
            // tag 0
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x09, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 1
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 2
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x02, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 3
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 4
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x04, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 5
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x05, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 6
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x06, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 7
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x07, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 8
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x0b, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 9
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x0c, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 10
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x0d, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 11
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x0e, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 12
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x08, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 13
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0a, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 14
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0f, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 15
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x10, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 16
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x14, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength
        ];

        assert_eq!(res, expected);

        Ok(())
    }

    #[test]
    fn test_bind_params_to_bytes_with_tbnames_and_tags() -> anyhow::Result<()> {
        let tags1 = vec![
            Value::Timestamp(Timestamp::Milliseconds(1726803356466)),
            Value::Bool(true),
            Value::TinyInt(1),
            Value::SmallInt(2),
            Value::Int(3),
            Value::BigInt(4),
            Value::Float(5.5),
            Value::Double(6.6),
            Value::UTinyInt(7),
            Value::USmallInt(8),
            Value::UInt(9),
            Value::UBigInt(10),
            Value::VarChar("varchar".to_string()),
            Value::NChar("nchar".to_string()),
            Value::Json(serde_json::json!({"key": "value"})),
            Value::VarBinary(Bytes::from("varbinary".as_bytes())),
            Value::Geometry(Bytes::from(vec![
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            ])),
        ];

        let param1 = Stmt2BindParam::new(Some("test1".to_owned()), Some(tags1), None);

        let tags2 = vec![
            Value::Null(Ty::Timestamp),
            Value::Null(Ty::Bool),
            Value::Null(Ty::TinyInt),
            Value::Null(Ty::SmallInt),
            Value::Null(Ty::Int),
            Value::Null(Ty::BigInt),
            Value::Null(Ty::Float),
            Value::Null(Ty::Double),
            Value::Null(Ty::UTinyInt),
            Value::Null(Ty::USmallInt),
            Value::Null(Ty::UInt),
            Value::Null(Ty::UBigInt),
            Value::Null(Ty::VarChar),
            Value::Null(Ty::NChar),
            Value::Null(Ty::Json),
            Value::Null(Ty::VarBinary),
            Value::Null(Ty::Geometry),
        ];

        let param2 = Stmt2BindParam::new(Some("testnil".to_owned()), Some(tags2), None);

        let tags3 = vec![
            Value::Timestamp(Timestamp::Milliseconds(1726803356466)),
            Value::Bool(true),
            Value::TinyInt(1),
            Value::SmallInt(2),
            Value::Int(3),
            Value::BigInt(4),
            Value::Float(5.5),
            Value::Double(6.6),
            Value::UTinyInt(7),
            Value::USmallInt(8),
            Value::UInt(9),
            Value::UBigInt(10),
            Value::VarChar("varchar".to_string()),
            Value::NChar("nchar".to_string()),
            Value::Json(serde_json::json!({"key": "value"})),
            Value::VarBinary(Bytes::from("varbinary".as_bytes())),
            Value::Geometry(Bytes::from(vec![
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            ])),
        ];

        let param3 = Stmt2BindParam::new(Some("test2".to_owned()), Some(tags3), None);

        let params = [param1, param2, param3];

        let fields = vec![
            Stmt2Field {
                name: "".to_string(),
                field_type: 1,
                precision: 0,
                scale: 0,
                bytes: 131584,
                bind_type: BindType::TableName,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
        ];

        let res = bind_params_to_bytes(&params, 100, 200, true, Some(&fields), 0)?;

        #[rustfmt::skip]
        let expected = [
            // fixed headers
            0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // req_id
            0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // stmt_id
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // action
            0x01, 0x00, // version
            0xff, 0xff, 0xff, 0xff, // col_idx

            // data
            0xec, 0x04, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // TableCount
            0x11, 0x00, 0x00, 0x00, // TagCount
            0x00, 0x00, 0x00, 0x00, // ColCount
            0x1c, 0x00, 0x00, 0x00, // TableNamesOffset
            0x36, 0x00, 0x00, 0x00, // TagsOffset
            0x00, 0x00, 0x00, 0x00, // ColsOffset

            // table names
            // TableNameLength
            0x06, 0x00,
            0x08, 0x00,
            0x06, 0x00,
            // TableNameBuffer
            0x74, 0x65, 0x73, 0x74, 0x31, 0x00,
            0x74, 0x65, 0x73, 0x74, 0x6e, 0x69, 0x6c, 0x00,
            0x74, 0x65, 0x73, 0x74, 0x32, 0x00,

            // tags
            // TagsDataLength
            0xb2, 0x01, 0x00, 0x00,
            0x46, 0x01, 0x00, 0x00,
            0xb2, 0x01, 0x00, 0x00,
            // TagsBuffer
            // table 0 tags
            // tag 0
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x09, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, // Buffer

            // tag 1
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // tag 2
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x02, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // tag 3
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x02, 0x00, // Buffer

            // tag 4
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x04, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x03, 0x00, 0x00, 0x00, // Buffer

            // tag 5
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x05, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // tag 6
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x06, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x00, 0x00, 0xb0, 0x40, // Buffer

            // tag 7
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x07, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0x40, // Buffer

            // tag 8
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x0b, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x07, // Buffer

            // tag 9
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x0c, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x08, 0x00, // Buffer

            // tag 10
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0d, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x09, 0x00, 0x00, 0x00, // Buffer

            // tag 11
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x0e, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // tag 12
            0x1d, 0x00, 0x00, 0x00, // TotalLength
            0x08, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x07, 0x00, 0x00, 0x00, // Length
            0x07, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72, // Buffer

            // tag 13
            0x1b, 0x00, 0x00, 0x00, // TotalLength
            0x0a, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x05, 0x00, 0x00, 0x00, // Length
            0x05, 0x00, 0x00, 0x00, // BufferLength
            0x6e, 0x63, 0x68, 0x61, 0x72, // Buffer

            // tag 14
            0x25, 0x00, 0x00, 0x00, // TotalLength
            0x0f, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x0f, 0x00, 0x00, 0x00, // Length
            0x0f, 0x00, 0x00, 0x00, // BufferLength
            0x7b, 0x22, 0x6b, 0x65, 0x79, 0x22, 0x3a, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x7d, // Buffer

            // tag 15
            0x1f, 0x00, 0x00, 0x00, // TotalLength
            0x10, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x09, 0x00, 0x00, 0x00, // Length
            0x09, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, // Buffer

            // tag 16
            0x2b, 0x00, 0x00, 0x00, // TotalLength
            0x14, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x15, 0x00, 0x00, 0x00, // Length
            0x15, 0x00, 0x00, 0x00, // BufferLength
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // Buffer

            // table 1 tags
            // tag 0
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x09, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 1
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 2
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x02, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 3
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 4
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x04, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 5
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x05, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 6
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x06, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 7
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x07, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 8
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x0b, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 9
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x0c, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 10
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x0d, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 11
            0x12, 0x00, 0x00, 0x00, // TotalLength
            0x0e, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 12
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x08, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 13
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0a, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 14
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0f, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 15
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x10, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // tag 16
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x14, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x01, // IsNull
            0x01, // HaveLength
            0x00, 0x00, 0x00, 0x00, // Length
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // table 2 tags
            // tag 0
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x09, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, // Buffer

            // tag 1
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // tag 2
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x02, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // tag 3
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x02, 0x00, // Buffer

            // tag 4
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x04, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x03, 0x00, 0x00, 0x00, // Buffer

            // tag 5
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x05, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // tag 6
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x06, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x00, 0x00, 0xb0, 0x40, // Buffer

            // tag 7
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x07, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0x40, // Buffer

            // tag 8
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x0b, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x07, // Buffer

            // tag 9
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x0c, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x08, 0x00, // Buffer

            // tag 10
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0d, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x09, 0x00, 0x00, 0x00, // Buffer

            // tag 11
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x0e, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // tag 12
            0x1d, 0x00, 0x00, 0x00, // TotalLength
            0x08, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x07, 0x00, 0x00, 0x00, // Length
            0x07, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72, // Buffer

            // tag 13
            0x1b, 0x00, 0x00, 0x00, // TotalLength
            0x0a, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x05, 0x00, 0x00, 0x00, // Length
            0x05, 0x00, 0x00, 0x00, // BufferLength
            0x6e, 0x63, 0x68, 0x61, 0x72, // Buffer

            // tag 14
            0x25, 0x00, 0x00, 0x00, // TotalLength
            0x0f, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x0f, 0x00, 0x00, 0x00, // Length
            0x0f, 0x00, 0x00, 0x00, // BufferLength
            0x7b, 0x22, 0x6b, 0x65, 0x79, 0x22, 0x3a, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x7d, // Buffer

            // tag 15
            0x1f, 0x00, 0x00, 0x00, // TotalLength
            0x10, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x09, 0x00, 0x00, 0x00, // Length
            0x09, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, // Buffer

            // tag 16
            0x2b, 0x00, 0x00, 0x00, // TotalLength
            0x14, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x15, 0x00, 0x00, 0x00, // Length
            0x15, 0x00, 0x00, 0x00, // BufferLength
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // Buffer
        ];

        assert_eq!(res, expected);

        Ok(())
    }

    #[test]
    fn test_bind_params_to_bytes_with_cols() -> anyhow::Result<()> {
        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![1]),
            ColumnView::from_small_ints(vec![2]),
            ColumnView::from_ints(vec![3]),
            ColumnView::from_big_ints(vec![4]),
            ColumnView::from_floats(vec![5.5]),
            ColumnView::from_doubles(vec![6.6]),
            ColumnView::from_unsigned_tiny_ints(vec![7]),
            ColumnView::from_unsigned_small_ints(vec![8]),
            ColumnView::from_unsigned_ints(vec![9]),
            ColumnView::from_unsigned_big_ints(vec![10]),
            ColumnView::from_varchar(vec!["varchar"]),
            ColumnView::from_nchar(vec!["nchar"]),
            ColumnView::from_bytes(vec!["varbinary".as_bytes()]),
            ColumnView::from_geobytes(vec![vec![
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            ]]),
        ];

        let fields_cnt = cols.len();
        let param = Stmt2BindParam::new(None, None, Some(cols));
        let res = bind_params_to_bytes(&[param], 100, 200, false, None, fields_cnt)?;

        #[rustfmt::skip]
        let expected = [
            // fixed headers
            0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // req_id
            0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // stmt_id
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // action
            0x01, 0x00, // version
            0xff, 0xff, 0xff, 0xff, // col_idx

            // data
            0xad, 0x01, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // TableCount
            0x00, 0x00, 0x00, 0x00, // TagCount
            0x10, 0x00, 0x00, 0x00, // ColCount
            0x00, 0x00, 0x00, 0x00, // TableNamesOffset
            0x00, 0x00, 0x00, 0x00, // TagsOffset
            0x1c, 0x00, 0x00, 0x00, // ColsOffset

            // cols
            // ColDataLength
            0x8d, 0x01, 0x00, 0x00,
            // ColBuffer
            // table 0 cols
            // col 0
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x09, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, // Buffer

            // col 1
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // col 2
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x02, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // col 3
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x02, 0x00, // Buffer

            // col 4
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x04, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x03, 0x00, 0x00, 0x00, // Buffer

            // col 5
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x05, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // col 6
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x06, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x00, 0x00, 0xb0, 0x40, // Buffer

            // col 7
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x07, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0x40, // Buffer

            // col 8
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x0b, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x07, // Buffer

            // col 9
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x0c, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x08, 0x00, // Buffer

            // col 10
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0d, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x09, 0x00, 0x00, 0x00, // Buffer

            // col 11
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x0e, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // col 12
            0x1d, 0x00, 0x00, 0x00, // TotalLength
            0x08, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            // Length
            0x07, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72, // Buffer

            // col 13
            0x1b, 0x00, 0x00, 0x00, // TotalLength
            0x0a, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            // Length
            0x05, 0x00, 0x00, 0x00,
            0x05, 0x00, 0x00, 0x00, // BufferLength
            0x6e, 0x63, 0x68, 0x61, 0x72, // Buffer

            // col 14
            0x1f, 0x00, 0x00, 0x00, // TotalLength
            0x10, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            // Length
            0x09, 0x00, 0x00, 0x00,
            0x09, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, // Buffer

            // col 15
            0x2b, 0x00, 0x00, 0x00, // TotalLength
            0x14, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            // Length
            0x15, 0x00, 0x00, 0x00,
            0x15, 0x00, 0x00, 0x00, // BufferLength
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // Buffer
        ];

        assert_eq!(res, expected);

        Ok(())
    }

    #[test]
    fn test_bind_params_to_bytes_with_null_cols() -> anyhow::Result<()> {
        let cols = vec![
            ColumnView::null(5, Ty::Timestamp),
            ColumnView::null(5, Ty::Bool),
            ColumnView::null(5, Ty::TinyInt),
            ColumnView::null(5, Ty::SmallInt),
            ColumnView::null(5, Ty::Int),
            ColumnView::null(5, Ty::BigInt),
            ColumnView::null(5, Ty::Float),
            ColumnView::null(5, Ty::Double),
            ColumnView::null(5, Ty::UTinyInt),
            ColumnView::null(5, Ty::USmallInt),
            ColumnView::null(5, Ty::UInt),
            ColumnView::null(5, Ty::UBigInt),
            ColumnView::null(5, Ty::VarChar),
            ColumnView::null(5, Ty::NChar),
            ColumnView::from_bytes::<&[u8], _, _, _>(vec![None, None, None, None, None]),
            ColumnView::from_geobytes::<&[u8], _, _, _>(vec![None, None, None, None, None]),
        ];

        let fields_cnt = cols.len();
        let param = Stmt2BindParam::new(None, None, Some(cols));
        let res = bind_params_to_bytes(&[param], 100, 200, false, None, fields_cnt)?;

        #[rustfmt::skip]
        let expected = [
            // fixed headers
            0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // req_id
            0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // stmt_id
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // action
            0x01, 0x00, // version
            0xff, 0xff, 0xff, 0xff, // col_idx

            // data
            0xd0, 0x01, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // TableCount
            0x00, 0x00, 0x00, 0x00, // TagCount
            0x10, 0x00, 0x00, 0x00, // ColCount
            0x00, 0x00, 0x00, 0x00, // TableNamesOffset
            0x00, 0x00, 0x00, 0x00, // TagsOffset
            0x1c, 0x00, 0x00, 0x00, // ColsOffset

            // cols
            // ColDataLength
            0xb0, 0x01, 0x00, 0x00,
            // ColBuffer
            // table 0 cols
            // col 0
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x09, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 1
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 2
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x02, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 3
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 4
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x04, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 5
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x05, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 6
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x06, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 7
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x07, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 8
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0b, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 9
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0c, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 10
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0d, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 11
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0e, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x00, // HaveLength
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 12
            0x2a, 0x00, 0x00, 0x00, // TotalLength
            0x08, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x01, // HaveLength
            // Length
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 13
            0x2a, 0x00, 0x00, 0x00, // TotalLength
            0x0a, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x01, // HaveLength
            // Length
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 14
            0x2a, 0x00, 0x00, 0x00, // TotalLength
            0x10, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x01, // HaveLength
            // Length
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, // BufferLength

            // col 15
            0x2a, 0x00, 0x00, 0x00, // TotalLength
            0x14, 0x00, 0x00, 0x00, // Type
            0x05, 0x00, 0x00, 0x00, // Num
            0x01, 0x01, 0x01, 0x01, 0x01, // IsNull
            0x01, // HaveLength
            // Length
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, // BufferLength
        ];

        assert_eq!(res, expected);

        Ok(())
    }

    #[test]
    fn test_bind_params_to_bytes_with_tbnames_tags_and_cols() -> anyhow::Result<()> {
        let tags = vec![
            Value::Timestamp(Timestamp::Milliseconds(1726803356466)),
            Value::Bool(true),
            Value::TinyInt(1),
            Value::SmallInt(2),
            Value::Int(3),
            Value::BigInt(4),
            Value::Float(5.5),
            Value::Double(6.6),
            Value::UTinyInt(7),
            Value::USmallInt(8),
            Value::UInt(9),
            Value::UBigInt(10),
            Value::VarChar("varchar".to_string()),
            Value::NChar("nchar".to_string()),
            Value::Json(serde_json::json!({"key": "value"})),
            Value::VarBinary(Bytes::from("varbinary".as_bytes())),
            Value::Geometry(Bytes::from(vec![
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            ])),
        ];

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![Some(1726803356466), None, Some(1726803358466)]),
            ColumnView::from_bools(vec![Some(true), None, Some(false)]),
            ColumnView::from_tiny_ints(vec![Some(11), None, Some(12)]),
            ColumnView::from_small_ints(vec![Some(11), None, Some(12)]),
            ColumnView::from_ints(vec![Some(11), None, Some(12)]),
            ColumnView::from_big_ints(vec![Some(11), None, Some(12)]),
            ColumnView::from_floats(vec![Some(11.2), None, Some(12.2)]),
            ColumnView::from_doubles(vec![Some(11.2), None, Some(12.2)]),
            ColumnView::from_unsigned_tiny_ints(vec![Some(11), None, Some(12)]),
            ColumnView::from_unsigned_small_ints(vec![Some(11), None, Some(12)]),
            ColumnView::from_unsigned_ints(vec![Some(11), None, Some(12)]),
            ColumnView::from_unsigned_big_ints(vec![Some(11), None, Some(12)]),
            ColumnView::from_varchar::<&str, _, _, _>(vec![
                Some("varchar1"),
                None,
                Some("varchar2"),
            ]),
            ColumnView::from_nchar::<&str, _, _, _>(vec![Some("nchar1"), None, Some("nchar2")]),
            ColumnView::from_bytes::<&[u8], _, _, _>(vec![
                Some("varbinary1".as_bytes()),
                None,
                Some("varbinary2".as_bytes()),
            ]),
            ColumnView::from_geobytes::<Vec<u8>, _, _, _>(vec![
                Some(vec![
                    0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
                ]),
                None,
                Some(vec![
                    0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
                ]),
            ]),
        ];

        let param = Stmt2BindParam::new(Some("test1".to_owned()), Some(tags), Some(cols));

        let fields = vec![
            Stmt2Field {
                name: "".to_string(),
                field_type: 1,
                precision: 0,
                scale: 0,
                bytes: 131584,
                bind_type: BindType::TableName,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "ts".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "a".to_string(),
                field_type: 9,
                precision: 0,
                scale: 0,
                bytes: 8,
                bind_type: BindType::Column,
            },
        ];

        let res = bind_params_to_bytes(&[param], 100, 200, true, Some(&fields), 0)?;

        #[rustfmt::skip]
        let expected = [
            // fixed headers
            0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // req_id
            0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // stmt_id
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // action
            0x01, 0x00, // version
            0xff, 0xff, 0xff, 0xff, // col_id

            // data
            0x41, 0x04, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // TableCount
            0x11, 0x00, 0x00, 0x00, // TagCount
            0x10, 0x00, 0x00, 0x00, // ColCount
            0x1c, 0x00, 0x00, 0x00, // TableNamesOffset
            0x24, 0x00, 0x00, 0x00, // TagsOffset
            0xda, 0x01, 0x00, 0x00, // ColsOffset

            // table names
            // TableNameLength
            0x06, 0x00,
            // TableNameBuffer
            0x74, 0x65, 0x73, 0x74, 0x31, 0x00,

            // tags
            // TagsDataLength
            0xb2, 0x01, 0x00, 0x00,
            // TagsBuffer
            // table 0 tags
            // tag 0
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x09, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, // Buffer

            // tag 1
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // tag 2
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x02, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x01, // Buffer

            // tag 3
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x02, 0x00, // Buffer

            // tag 4
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x04, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x03, 0x00, 0x00, 0x00, // Buffer

            // tag 5
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x05, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // tag 6
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x06, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x00, 0x00, 0xb0, 0x40, // Buffer

            // tag 7
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x07, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0x40, // Buffer

            // tag 8
            0x13, 0x00, 0x00, 0x00, // TotalLength
            0x0b, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x01, 0x00, 0x00, 0x00, // BufferLength
            0x07, // Buffer

            // tag 9
            0x14, 0x00, 0x00, 0x00, // TotalLength
            0x0c, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x02, 0x00, 0x00, 0x00, // BufferLength
            0x08, 0x00, // Buffer

            // tag 10
            0x16, 0x00, 0x00, 0x00, // TotalLength
            0x0d, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x04, 0x00, 0x00, 0x00, // BufferLength
            0x09, 0x00, 0x00, 0x00, // Buffer

            // tag 11
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x0e, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x00, // HaveLength
            0x08, 0x00, 0x00, 0x00, // BufferLength
            0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // tag 12
            0x1d, 0x00, 0x00, 0x00, // TotalLength
            0x08, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x07, 0x00, 0x00, 0x00, // Length
            0x07, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72, // Buffer

            // tag 13
            0x1b, 0x00, 0x00, 0x00, // TotalLength
            0x0a, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x05, 0x00, 0x00, 0x00, // Length
            0x05, 0x00, 0x00, 0x00, // BufferLength
            0x6e, 0x63, 0x68, 0x61, 0x72, // Buffer

            // tag 14
            0x25, 0x00, 0x00, 0x00, // TotalLength
            0x0f, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x0f, 0x00, 0x00, 0x00, // Length
            0x0f, 0x00, 0x00, 0x00, // BufferLength
            0x7b, 0x22, 0x6b, 0x65, 0x79, 0x22, 0x3a, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x7d, // Buffer

            // tag 15
            0x1f, 0x00, 0x00, 0x00, // TotalLength
            0x10, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x09, 0x00, 0x00, 0x00, // Length
            0x09, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, // Buffer

            // tag 16
            0x2b, 0x00, 0x00, 0x00, // TotalLength
            0x14, 0x00, 0x00, 0x00, // Type
            0x01, 0x00, 0x00, 0x00, // Num
            0x00, // IsNull
            0x01, // HaveLength
            0x15, 0x00, 0x00, 0x00, // Length
            0x15, 0x00, 0x00, 0x00, // BufferLength
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // Buffer


            // cols
            // ColDataLength
            0x63, 0x02, 0x00, 0x00,
            // ColBuffer
            // table 0 cols
            // col 0
            0x2c, 0x00, 0x00, 0x00, // TotalLength
            0x09, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x18, 0x00, 0x00, 0x00, // BufferLength
            0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x33, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, // Buffer

            // col 1
            0x17, 0x00, 0x00, 0x00, // TotalLength
            0x01, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x03, 0x00, 0x00, 0x00, // BufferLength
            0x01, 0x00, 0x00, // Buffer

            // col 2
            0x17, 0x00, 0x00, 0x00, // TotalLength
            0x02, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x03, 0x00, 0x00, 0x00, // BufferLength
            0x0b, 0x00, 0x0c, // Buffer

            // col 3
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x03, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x06, 0x00, 0x00, 0x00, // BufferLength
            0x0b, 0x00, 0x00, 0x00, 0x0c, 0x00, // Buffer

            // col 4
            0x20, 0x00, 0x00, 0x00, // TotalLength
            0x04, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x0c, 0x00, 0x00, 0x00, // BufferLength
            0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, // Buffer

            // col 5
            0x2c, 0x00, 0x00, 0x00, // TotalLength
            0x05, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x18, 0x00, 0x00, 0x00, // BufferLength
            0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // col 6
            0x20, 0x00, 0x00, 0x00, // TotalLength
            0x06, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x0c, 0x00, 0x00, 0x00, // BufferLength
            0x33, 0x33, 0x33, 0x41, 0x00, 0x00, 0x00, 0x00, 0x33, 0x33, 0x43, 0x41, // Buffer

            // col 7
            0x2c, 0x00, 0x00, 0x00, // TotalLength
            0x07, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x18, 0x00, 0x00, 0x00, // BufferLength
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x26, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x28, 0x40, // Buffer

            // col 8
            0x17, 0x00, 0x00, 0x00, // TotalLength
            0x0b, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x03, 0x00, 0x00, 0x00, // BufferLength
            0x0b, 0x00, 0x0c, // Buffer

            // col 9
            0x1a, 0x00, 0x00, 0x00, // TotalLength
            0x0c, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x06, 0x00, 0x00, 0x00, // BufferLength
            0x0b, 0x00, 0x00, 0x00, 0x0c, 0x00, // Buffer

            // col 10
            0x20, 0x00, 0x00, 0x00, // TotalLength
            0x0d, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x0c, 0x00, 0x00, 0x00, // BufferLength
            0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, // Buffer

            // col 11
            0x2c, 0x00, 0x00, 0x00, // TotalLength
            0x0e, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x00, // HaveLength
            0x18, 0x00, 0x00, 0x00, // BufferLength
            0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Buffer

            // col 12
            0x30, 0x00, 0x00, 0x00, // TotalLength
            0x08, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x01, // HaveLength
            // Length
            0x08, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x08, 0x00, 0x00, 0x00,
            0x10, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72, 0x31, 0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72, 0x32, // Buffer

            // col 13
            0x2c, 0x00, 0x00, 0x00, // TotalLength
            0x0a, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x01, // HaveLength
            // Length
            0x06, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x06, 0x00, 0x00, 0x00,
            0x0c, 0x00, 0x00, 0x00, // BufferLength
            0x6e, 0x63, 0x68, 0x61, 0x72, 0x31, 0x6e, 0x63, 0x68, 0x61, 0x72, 0x32, // Buffer

            // col 14
            0x34, 0x00, 0x00, 0x00, // TotalLength
            0x10, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x01, // HaveLength
            // Length
            0x0a, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x0a, 0x00, 0x00, 0x00,
            0x14, 0x00, 0x00, 0x00, // BufferLength
            0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x31, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32, // Buffer

            // col 15
            0x4a, 0x00, 0x00, 0x00, // TotalLength
            0x14, 0x00, 0x00, 0x00, // Type
            0x03, 0x00, 0x00, 0x00, // Num
            0x00, 0x01, 0x00, // IsNull
            0x01, // HaveLength
            // Length
            0x15, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x15, 0x00, 0x00, 0x00,
            0x2a, 0x00, 0x00, 0x00, // BufferLength
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // Buffer
        ];

        assert_eq!(res, expected);

        Ok(())
    }

    #[test]
    #[should_panic = "no params to bind"]
    fn test_bind_params_to_bytes_without_params() {
        let _ = bind_params_to_bytes(&[], 100, 200, true, None, 0).unwrap();
    }

    #[test]
    fn test_bind_params_to_bytes_without_fields() {
        let fields = vec![];
        let test_cases = vec![None, Some(&fields)];
        for fields in test_cases {
            let param = Stmt2BindParam::new(None, None, None);
            let res = bind_params_to_bytes(&[param], 100, 200, true, fields, 0);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_bind_params_to_bytes_without_tbnames() {
        let test_cases = vec![None, Some(String::new())];
        for tbname in test_cases {
            let param = Stmt2BindParam::new(tbname, None, None);

            let fields = vec![Stmt2Field {
                name: "".to_string(),
                field_type: 1,
                precision: 0,
                scale: 0,
                bytes: 131584,
                bind_type: BindType::TableName,
            }];

            let res = bind_params_to_bytes(&[param], 100, 200, true, Some(&fields), 0);
            assert!(res.is_err());
        }
    }

    #[test]
    #[should_panic = "tags is empty"]
    fn test_bind_params_to_bytes_without_tags() {
        let param = Stmt2BindParam::new(None, None, None);

        let fields = vec![Stmt2Field {
            name: "".to_string(),
            field_type: 1,
            precision: 0,
            scale: 0,
            bytes: 131584,
            bind_type: BindType::Tag,
        }];

        let _ = bind_params_to_bytes(&[param], 100, 200, true, Some(&fields), 0).unwrap();
    }

    #[test]
    #[should_panic = "tags len mismatch"]
    fn test_bind_params_to_bytes_tags_len_mismatch() {
        let tags = vec![Value::Int(1)];
        let param = Stmt2BindParam::new(None, Some(tags), None);

        let fields = vec![
            Stmt2Field {
                name: "".to_string(),
                field_type: 1,
                precision: 0,
                scale: 0,
                bytes: 131584,
                bind_type: BindType::Tag,
            },
            Stmt2Field {
                name: "".to_string(),
                field_type: 1,
                precision: 0,
                scale: 0,
                bytes: 131584,
                bind_type: BindType::Tag,
            },
        ];

        let _ = bind_params_to_bytes(&[param], 100, 200, true, Some(&fields), 0).unwrap();
    }

    #[test]
    #[should_panic = "columns is empty"]
    fn test_bind_params_to_bytes_without_cols() {
        let param = Stmt2BindParam::new(None, None, None);

        let fields = vec![Stmt2Field {
            name: "".to_string(),
            field_type: 1,
            precision: 0,
            scale: 0,
            bytes: 131584,
            bind_type: BindType::Column,
        }];

        let _ = bind_params_to_bytes(&[param], 100, 200, true, Some(&fields), 0).unwrap();
    }

    #[test]
    #[should_panic = "columns len mismatch"]
    fn test_bind_params_to_bytes_cols_len_mismatch() {
        let cols = vec![ColumnView::from_ints(vec![1])];
        let param = Stmt2BindParam::new(None, None, Some(cols));

        let fields = vec![
            Stmt2Field {
                name: "".to_string(),
                field_type: 1,
                precision: 0,
                scale: 0,
                bytes: 131584,
                bind_type: BindType::Column,
            },
            Stmt2Field {
                name: "".to_string(),
                field_type: 1,
                precision: 0,
                scale: 0,
                bytes: 131584,
                bind_type: BindType::Column,
            },
        ];

        let _ = bind_params_to_bytes(&[param], 100, 200, true, Some(&fields), 0).unwrap();
    }

    #[test]
    #[should_panic = "column does not support json type"]
    fn test_bind_params_to_bypes_with_json_col() {
        let cols = vec![ColumnView::from_json(vec!["{\"key\":\"value\"}"])];
        let param = Stmt2BindParam::new(None, None, Some(cols));

        let fields = vec![Stmt2Field {
            name: "".to_string(),
            field_type: 1,
            precision: 0,
            scale: 0,
            bytes: 131584,
            bind_type: BindType::Column,
        }];

        let _ = bind_params_to_bytes(&[param], 100, 200, true, Some(&fields), 0).unwrap();
    }
}
