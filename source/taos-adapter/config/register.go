package config

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Register struct {
	Instance    string
	Description string
	Duration    int
	Expire      int
}

func initRegister(v *viper.Viper) {
	v.SetDefault("register.instance", "")
	_ = v.BindEnv("register.instance", "TAOS_ADAPTER_REGISTER_INSTANCE")

	v.SetDefault("register.description", "")
	_ = v.BindEnv("register.description", "TAOS_ADAPTER_REGISTER_DESCRIPTION")

	v.SetDefault("register.duration", 10)
	_ = v.BindEnv("register.duration", "TAOS_ADAPTER_REGISTER_DURATION")

	v.SetDefault("register.expire", 30)
	_ = v.BindEnv("register.expire", "TAOS_ADAPTER_REGISTER_EXPIRE")
}

func registerRegisterFlags() {
	pflag.String("register.instance", "", `The endpoint for this adapter instance. Env "TAOS_ADAPTER_REGISTER_INSTANCE"`)
	pflag.String("register.description", "", `A brief description of this adapter instance. Env "TAOS_ADAPTER_REGISTER_DESCRIPTION"`)
	pflag.Int("register.duration", 10, `The duration (in seconds) for which the registration is valid. Env "TAOS_ADAPTER_REGISTER_DURATION"`)
	pflag.Int("register.expire", 30, `The expiration time (in seconds) for the registration. Env "TAOS_ADAPTER_REGISTER_EXPIRE"`)
}

func (r *Register) setValue(v *viper.Viper) error {
	r.Instance = v.GetString("register.instance")
	if r.Instance == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("register.instance is empty and get hostname error: %s", err)
		}
		port := v.GetInt("port")

		if v.GetBool("ssl.enable") {
			r.Instance = fmt.Sprintf("https://%s:%d", hostname, port)
		} else {
			r.Instance = fmt.Sprintf("%s:%d", hostname, port)
		}
	}
	if len(r.Instance) > 255 {
		return fmt.Errorf("register.instance length exceeds 255 characters, length: %d, instance: %s", len(r.Instance), r.Instance)
	}
	r.Description = v.GetString("register.description")
	if len(r.Description) > 511 {
		return fmt.Errorf("register.description length exceeds 511 characters, length: %d, description: %s", len(r.Description), r.Description)
	}
	r.Duration = v.GetInt("register.duration")
	if r.Duration <= 0 {
		return fmt.Errorf("register.duration must be greater than 0, duration: %d", r.Duration)
	}
	r.Expire = v.GetInt("register.expire")
	if r.Expire <= 0 {
		return fmt.Errorf("register.expire must be greater than 0, expire: %d", r.Expire)
	}
	if r.Expire <= r.Duration {
		return fmt.Errorf("register.expire must be greater than register.duration, expire: %d, duration: %d", r.Expire, r.Duration)
	}
	return nil
}
