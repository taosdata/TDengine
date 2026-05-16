package tools

import (
	"encoding/base64"
	"errors"
	"strings"
)

func DecodeBasic(auth string) (user, password string, err error) {
	b, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return "", "", err
	}
	sl := strings.SplitN(string(b), ":", 2)
	if len(sl) != 2 {
		return "", "", errors.New("wrong basic auth")
	}
	return sl[0], sl[1], nil
}
