package config

import (
	"github.com/gin-contrib/cors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type CorsConfig struct {
	AllowAllOrigins  bool
	AllowOrigins     []string
	AllowHeaders     []string
	ExposeHeaders    []string
	AllowCredentials bool
	AllowWebSockets  bool
}

func (conf *CorsConfig) GetConfig() cors.Config {
	corsConfig := cors.DefaultConfig()
	if conf.AllowAllOrigins {
		corsConfig.AllowAllOrigins = true
	} else {
		if len(conf.AllowOrigins) == 0 {
			corsConfig.AllowOrigins = []string{
				"http://127.0.0.1",
			}
		} else {
			corsConfig.AllowOrigins = conf.AllowOrigins
		}
	}
	corsConfig.AddAllowHeaders("Authorization")
	if len(conf.AllowHeaders) > 0 {
		corsConfig.AddAllowHeaders(conf.AllowHeaders...)
	}

	corsConfig.AllowCredentials = conf.AllowCredentials
	corsConfig.AllowWebSockets = conf.AllowWebSockets
	corsConfig.AllowWildcard = true
	corsConfig.ExposeHeaders = []string{"Authorization"}
	return corsConfig
}

func initCors() {
	viper.SetDefault("cors.allowAllOrigins", true)
	_ = viper.BindEnv("cors.allowAllOrigins", "TAOS_ADAPTER_CORS_ALLOW_ALL_ORIGINS")
	pflag.Bool("cors.allowAllOrigins", true, `cors allow all origins. Env "TAOS_ADAPTER_CORS_ALLOW_ALL_ORIGINS"`)

	viper.SetDefault("cors.allowOrigins", nil)
	_ = viper.BindEnv("cors.allowOrigins", "TAOS_ADAPTER_ALLOW_ORIGINS")
	pflag.StringArray("cors.allowOrigins", nil, `cors allow origins. Env "TAOS_ADAPTER_ALLOW_ORIGINS"`)

	viper.SetDefault("cors.allowHeaders", nil)
	_ = viper.BindEnv("cors.allowHeaders", "TAOS_ADAPTER_ALLOW_HEADERS")
	pflag.StringArray("cors.allowHeaders", nil, `cors allow HEADERS. Env "TAOS_ADAPTER_ALLOW_HEADERS"`)

	viper.SetDefault("cors.exposeHeaders", nil)
	_ = viper.BindEnv("cors.exposeHeaders", "TAOS_ADAPTER_Expose_Headers")
	pflag.StringArray("cors.exposeHeaders", nil, `cors expose headers. Env "TAOS_ADAPTER_Expose_Headers"`)

	viper.SetDefault("cors.allowCredentials", false)
	_ = viper.BindEnv("cors.allowCredentials", "TAOS_ADAPTER_CORS_ALLOW_Credentials")
	pflag.Bool("cors.allowCredentials", false, `cors allow credentials. Env "TAOS_ADAPTER_CORS_ALLOW_Credentials"`)

	viper.SetDefault("cors.allowWebSockets", false)
	_ = viper.BindEnv("cors.allowWebSockets", "TAOS_ADAPTER_CORS_ALLOW_WebSockets")
	pflag.Bool("cors.allowWebSockets", false, `cors allow WebSockets. Env "TAOS_ADAPTER_CORS_ALLOW_WebSockets"`)

}

func (conf *CorsConfig) setValue() {
	conf.AllowAllOrigins = viper.GetBool("cors.allowAllOrigins")
	conf.AllowOrigins = viper.GetStringSlice("cors.allowOrigins")
	conf.AllowHeaders = viper.GetStringSlice("cors.allowHeaders")
	conf.ExposeHeaders = viper.GetStringSlice("cors.exposeHeaders")
	conf.AllowCredentials = viper.GetBool("cors.allowCredentials")
	conf.AllowWebSockets = viper.GetBool("cors.allowWebSockets")
}
