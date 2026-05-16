use byteorder::ByteOrder;
use hmac::{Hmac, Mac};
use rand::{seq::SliceRandom, Rng};

const LOWERCASE: &str = "abcdefghijklmnopqrstuvwxyz";
const UPPERCASE: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const DIGITS: &str = "0123456789";
const ALL_CHARS: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

pub fn generate_totp_seed(len: usize) -> String {
    assert!(len >= 3);
    let mut rng = rand::rng();
    let mut buf = Vec::with_capacity(len);
    buf.push(LOWERCASE.as_bytes()[rng.random_range(0..LOWERCASE.len())]);
    buf.push(UPPERCASE.as_bytes()[rng.random_range(0..UPPERCASE.len())]);
    buf.push(DIGITS.as_bytes()[rng.random_range(0..DIGITS.len())]);
    for _ in 3..len {
        buf.push(ALL_CHARS.as_bytes()[rng.random_range(0..ALL_CHARS.len())]);
    }
    buf.shuffle(&mut rng);
    String::from_utf8(buf).unwrap()
}

pub fn generate_totp_code(secret: &[u8]) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let counter = now / 30;
    let code = hotp(secret, counter, 6);
    format!("{:06}", code)
}

fn hotp(secret: &[u8], counter: u64, digits: usize) -> u32 {
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(secret).unwrap();
    mac.update(&counter.to_be_bytes());
    let result = mac.finalize().into_bytes();
    let offset = (result[result.len() - 1] & 0x0F) as usize;
    let code = byteorder::BigEndian::read_u32(&result[offset..offset + 4]) & 0x7FFFFFFF;
    let m = 10u32.pow(digits.min(8) as u32);
    code % m
}

pub fn generate_totp_secret(seed: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&[]).unwrap();
    mac.update(seed);
    mac.finalize().into_bytes().to_vec()
}

pub fn totp_secret_encode(secret: &[u8]) -> String {
    base32::encode(base32::Alphabet::Rfc4648 { padding: false }, secret)
}

pub fn totp_secret_decode(secret: &str) -> Option<Vec<u8>> {
    base32::decode(base32::Alphabet::Rfc4648 { padding: false }, secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_totp_seed() {
        let seed = generate_totp_seed(16);
        assert_eq!(seed.len(), 16);
        assert!(seed.chars().any(|c| c.is_lowercase()));
        assert!(seed.chars().any(|c| c.is_uppercase()));
        assert!(seed.chars().any(|c| c.is_digit(10)));
    }

    #[test]
    fn test_generate_totp_code() {
        let secret = generate_totp_secret(b"12345678901234567890");
        let code = generate_totp_code(&secret);
        assert_eq!(code.len(), 6);
    }

    #[test]
    fn test_hotp() {
        let counter = 1765854733 / 30;
        let digits = 6;
        assert_eq!(
            hotp(
                &generate_totp_secret(b"12345678901234567890"),
                counter,
                digits
            ),
            383089
        );
        assert_eq!(
            hotp(
                &generate_totp_secret(b"abcdefghijklmnopqrstuvwxyz"),
                counter,
                digits
            ),
            269095
        );
        assert_eq!(
            hotp(
                &generate_totp_secret(b"!@#$%^&*()_+-=[]{}|;':,.<>/?`~"),
                counter,
                digits
            ),
            203356
        );
    }

    #[test]
    fn test_generate_totp_secret() {
        let secret = generate_totp_secret(b"12345678901234567890");
        let totp_secret = totp_secret_encode(&secret);
        assert_eq!(
            totp_secret,
            "VR62SA7EK3RP7MRTH7QXSIVZXXS57OY2SRUMGLKDJPREZ62OHFEQ"
        );
        assert_eq!(totp_secret_decode(&totp_secret).unwrap(), secret);

        let secret = generate_totp_secret(b"abcdefghijklmnopqrstuvwxyz");
        let totp_secret = totp_secret_encode(&secret);
        assert_eq!(
            totp_secret,
            "OMYPD744HIZB2KZPAUNLEWUFBNRQBILEPWD2FGPUDYCZMFTCRFXQ"
        );
        assert_eq!(totp_secret_decode(&totp_secret).unwrap(), secret);

        let secret = generate_totp_secret(b"!@#$%^&*()_+-=[]{}|;':,.<>/?`~");
        let totp_secret = totp_secret_encode(&secret);
        assert_eq!(
            totp_secret,
            "FURKOZ6REIGLQHP5OMKLZZFUQNOCRDJPCOSDP5ESH2VR3IU7NAYA"
        );
        assert_eq!(totp_secret_decode(&totp_secret).unwrap(), secret);
    }
}
