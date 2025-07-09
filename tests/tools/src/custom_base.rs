use num_bigint::BigUint;
use num_integer::Integer;
use num_traits::ToPrimitive;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    BaseCharsTooShort,
    UnexpectedChar,
}

type Result<T> = std::result::Result<T, Error>;

/// 将字符串转换为自定义进制表示
pub fn encode_to_custom_base(input: &[u8], charset: &str) -> Result<String> {
    // 获取进制基数
    let base = charset.chars().count();
    snafu::ensure!(base >= 2, BaseCharsTooShortSnafu);

    // 首先将字符串转换为大整数（将每个字符视为 256 进制的一位）
    let mut number: BigUint = BigUint::from(0u32);

    for &byte in input {
        number = number * BigUint::from(256u32) + BigUint::from(byte);
    }

    // 然后将这个大整数转换为自定义进制
    if number == BigUint::from(0u32) {
        return Ok(charset[0..1].to_string());
    }

    let base_biguint = BigUint::from(base as u32);
    let mut result = String::new();

    while number > BigUint::from(0u32) {
        let (next_number, remainder) = number.div_rem(&base_biguint);
        let idx = remainder.to_u32().unwrap() as usize;

        // 获取对应索引的字符
        let ch = charset.chars().nth(idx).unwrap();
        result.push(ch);

        number = next_number;
    }

    // 结果需要反转，因为我们是从低位到高位构建的
    Ok(result.chars().rev().collect())
}

/// 将自定义进制表示转换回字符串
pub fn decode_from_custom_base(encoded: &str, charset: &str) -> Result<Vec<u8>> {
    let base = charset.chars().count();
    snafu::ensure!(base >= 2, BaseCharsTooShortSnafu);

    // 首先将自定义进制转换为大整数
    let mut number = BigUint::from(0u32);
    let base_biguint = BigUint::from(base as u32);

    for ch in encoded.chars() {
        // 查找字符在字符集中的索引
        let value = match charset.find(ch) {
            Some(idx) => idx,
            None => return UnexpectedCharSnafu.fail(),
        };

        number = number * base_biguint.clone() + BigUint::from(value as u32);
    }

    // 然后将大整数转换回字符串
    let mut bytes = Vec::new();
    while number > BigUint::from(0u32) {
        let (next_number, remainder) = number.div_rem(&BigUint::from(256u32));
        bytes.push(remainder.to_u32().unwrap() as u8);
        number = next_number;
    }

    bytes.reverse();
    Ok(bytes)
}
