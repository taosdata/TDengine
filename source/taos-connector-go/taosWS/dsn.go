package taosWS

import (
	"errors"

	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

var (
	ErrInvalidDSNUnescaped = &taosErrors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: did you forget to escape a param value?"}
	ErrInvalidDSNAddr      = &taosErrors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: network address not terminated (missing closing brace)"}
	ErrInvalidDSNPort      = &taosErrors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: network port is not a valid number"}
	ErrInvalidDSNNoSlash   = &taosErrors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: missing the slash separating the database name"}
)

// Config is taosWS DSN config model.
// It aliases unified.Config so websocket adapters share one config definition.
type Config = unified.Config

// NewConfig creates a new Config and sets default values.
func NewConfig() *Config {
	return &unified.Config{
		InterpolateParams: true,
	}
}

// ParseDSN parses the DSN string to a Config.
func ParseDSN(dsn string) (cfg *Config, err error) {
	cfg, err = unified.ParseDSN(dsn)
	if err != nil {
		return nil, mapUnifiedDSNError(err)
	}
	return cfg, nil
}

func mapUnifiedDSNError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, unified.ErrInvalidDSNUnescaped):
		return ErrInvalidDSNUnescaped
	case errors.Is(err, unified.ErrInvalidDSNAddr):
		return ErrInvalidDSNAddr
	case errors.Is(err, unified.ErrInvalidDSNPort):
		return ErrInvalidDSNPort
	case errors.Is(err, unified.ErrInvalidDSNNoSlash):
		return ErrInvalidDSNNoSlash
	default:
		return &taosErrors.TaosError{
			Code:   0xffff,
			ErrStr: err.Error(),
		}
	}
}
