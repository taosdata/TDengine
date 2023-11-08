# Encryption

## Symmetric Encryption
Both sides use the same key to encrypt and decrypt data.
- Key distribution is a problem.
  - Hard to revoke a key.
  - Disastrous if key is compromised.
  - Hard to distribute keys to many users.
  - Impossible for the web.

### Data Encryption Standard (DES)
- DES is a block cipher with 56-bit key.
- 56 bit key + 8 bits for parity.
- 64 bits at a time.
- Perform 16 rounds of encryption.
- Data transmitted in 64-bit blocks, each may be coded independently.
- Original DES no longer considered secure.	Use **triple DES**.

### **Triple DES (3DES)**
- 3DES is a block cipher with 168-bit key.
- 168 bit key + 24 bits for parity.

### Advanced Encryption Standard (AES)


### Blowfish

### Twofish

## Asymmetric Encryption
Asymmetric encryption uses two keys, a public key known to everyone and a private key that only the recipient of messages uses.

- A message encrypted with a public key can only be decrypted with the corresponding private key.
- You can't derive the private key from the public key.
- Asymmetric encryption is slow.
- **Use asymmetric encryption to exchange a symmetric key**.
- **Use symmetric encryption to encrypt the data**.

### RSA (used for asymmetric encryption)

### Elliptic Curve Cryptography (ECC) (also used for asymmetric encryption)

## Cryptographic Hash Functions
Cryptographic hash functions are used encrypt data and to verify the integrity of data.

### MD5
- MD5 is commonly used for data integrity checks.
- MD5 is **vulnerable** to attack when used for encryption. Compromised in 1996 in ~seconds to ~hours.
- MD5 is **not** recommended for use in new systems.
- MD5 is **vulnerable** to collision attacks.

### SHA-1

### SHA-2

### SHA-3


# References:
1. [How to choose an AES encryption mode (CBC ECB CTR OCB CFB)?](https://stackoverflow.com/questions/1220751/how-to-choose-an-aes-encryption-mode-cbc-ecb-ctr-ocb-cfb)
2. [Encrypt files using AES with OPENSSL](https://kekayan.medium.com/encrypt-files-using-aes-with-openssl-dabb86d5b748)
3. [SQLite with encryption/password protection](https://stackoverflow.com/questions/5669905/sqlite-with-encryption-password-protection)