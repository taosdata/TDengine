package common

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
)

// GenerateTOTPCode generates a TOTP code based on the provided key, counter, and number of digits.
func GenerateTOTPCode(key []byte, counter uint64, digits int) int {
	h := hmac.New(sha1.New, key)
	counterBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(counterBytes, counter)
	h.Write(counterBytes)
	sum := h.Sum(nil)
	offset := sum[len(sum)-1] & 0x0F
	v := binary.BigEndian.Uint32(sum[offset:]) & 0x7FFFFFFF
	d := uint32(1)
	for i := 0; i < digits && i < 8; i++ {
		d *= 10
	}
	return int(v % d)
}

// GenerateTOTPSecret generates a TOTP secret using HMAC-SHA256 with the provided seed.
func GenerateTOTPSecret(seed []byte) []byte {
	h := hmac.New(sha256.New, nil)
	h.Write(seed)
	hmacResult := h.Sum(nil)
	return hmacResult
}

// TOTPSecretStr converts the given TOTP secret bytes into a base32-encoded
// string without padding and returns the resulting string representation.
func TOTPSecretStr(secret []byte) string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(secret)
}
