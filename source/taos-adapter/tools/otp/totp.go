package otp

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
)

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

func GenerateTOTPSecret(seed []byte) []byte {
	h := hmac.New(sha256.New, nil)
	h.Write(seed)
	hmacResult := h.Sum(nil)
	return hmacResult
}

func TOTPSecretStr(secret []byte) string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(secret)
}
