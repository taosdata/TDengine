use bytes::Bytes;

pub fn hex_string_to_bytes(s: &str) -> Bytes {
    let b: Vec<u8> = s
        .as_bytes()
        .chunks(2)
        .map(|chunk| std::str::from_utf8(chunk).unwrap())
        .map(|chunk| u8::from_str_radix(chunk, 16).unwrap())
        .collect();
    Bytes::from(b)
}

pub fn bytes_to_hex_string(bytes: Bytes) -> String {
    const HEX_CHARS: &[u8] = b"0123456789abcdef";

    let mut res = String::with_capacity(bytes.len() * 2);
    for &byte in bytes.iter() {
        res.push(HEX_CHARS[(byte >> 4) as usize] as char);
        res.push(HEX_CHARS[(byte & 0xf) as usize] as char);
    }
    res
}

pub fn bytes_to_hex_string_upper(bytes: Bytes) -> String {
    const HEX_CHARS: &[u8] = b"0123456789ABCDEF";

    let mut res = String::with_capacity(bytes.len() * 2);
    for &byte in bytes.iter() {
        res.push(HEX_CHARS[(byte >> 4) as usize] as char);
        res.push(HEX_CHARS[(byte & 0xf) as usize] as char);
    }
    res
}

pub fn slice_to_hex_upper_with_prefix(slice: &[u8]) -> Vec<u8> {
    const HEX_CHARS: &[u8] = b"0123456789ABCDEF";
    let mut hex_buf = Vec::with_capacity(2 + slice.len() * 2);
    hex_buf.extend_from_slice(b"\\x");
    for &byte in slice.iter() {
        hex_buf.push(HEX_CHARS[(byte >> 4) as usize]);
        hex_buf.push(HEX_CHARS[(byte & 0xf) as usize]);
    }
    hex_buf
}

pub fn bytes_to_sql_hex_string(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut out = vec![0u8; bytes.len() * 2 + 4];
    out[0] = b'"';
    out[1] = b'\\';
    out[2] = b'x';

    let mut index = 3;
    for &byte in bytes {
        out[index] = HEX[(byte >> 4) as usize];
        out[index + 1] = HEX[(byte & 0x0f) as usize];
        index += 2;
    }

    out[index] = b'"';

    // SAFETY: out only contains ASCII bytes ('"', '\\', 'x', '0'..'9', 'A'..'F'), always valid UTF-8.
    unsafe { String::from_utf8_unchecked(out) }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_hex_string_to_bytes() {
        assert_eq!(
            Bytes::from(vec![0x12, 0x34, 0x56]),
            hex_string_to_bytes("123456")
        );
    }

    #[test]
    fn test_bytes_to_hex_and_back() {
        let original_bytes = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]);
        let hex_string = bytes_to_hex_string(original_bytes.clone());
        let converted_bytes = hex_string_to_bytes(&hex_string);
        assert_eq!(original_bytes, converted_bytes);
    }

    #[test]
    fn test_bytes_to_hex_string() {
        let bytes = Bytes::from(vec![
            0x00, 0x01, 0x10, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF,
        ]);
        let hex_string = bytes_to_hex_string(bytes);
        assert_eq!(hex_string, "000110123456789abcdeff");
    }

    #[test]
    fn test_bytes_to_hex_string_upper() {
        let bytes = Bytes::from(vec![
            0x00, 0x01, 0x10, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF,
        ]);
        let hex_string = bytes_to_hex_string_upper(bytes);
        assert_eq!(hex_string, "000110123456789ABCDEFF");
    }

    #[test]
    fn test_slice_to_hex_upper_with_prefix() {
        let slice = &[
            0x00, 0x01, 0x10, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF,
        ];
        let hex = slice_to_hex_upper_with_prefix(slice);
        assert_eq!(hex, b"\\x000110123456789ABCDEFF");
    }

    #[test]
    fn test_bytes_to_sql_hex_string() {
        let bytes = [
            0x00, 0x01, 0x10, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF,
        ];
        let hex = bytes_to_sql_hex_string(&bytes);
        assert_eq!(hex, "\"\\x000110123456789ABCDEFF\"");
    }
}
