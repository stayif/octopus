package conf

import (
	"fmt"
	"os"
	"strings"

	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/spf13/viper"
)

type Server struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type Log struct {
	Level string `mapstructure:"level"`
}

type Database struct {
	Type string `mapstructure:"type"`
	Path string `mapstructure:"path"`
}

type Billing struct {
	BaseURL                    string `mapstructure:"base_url"`
	ServiceToken               string `mapstructure:"service_token"`
	ServiceTokenFile           string `mapstructure:"service_token_file"`
	TimeoutSeconds             int    `mapstructure:"timeout_seconds"`
	PendingMaxAmountMicrounits int64  `mapstructure:"pending_max_amount_microunits"`
	PendingMaxCount            int64  `mapstructure:"pending_max_count"`
	PendingMaxAgeSeconds       int64  `mapstructure:"pending_max_age_seconds"`
	SettlementPollMillis       int64  `mapstructure:"settlement_poll_millis"`
}

type Config struct {
	Server   Server   `mapstructure:"server"`
	Log      Log      `mapstructure:"log"`
	Database Database `mapstructure:"database"`
	Billing  Billing  `mapstructure:"billing"`
}

var AppConfig Config

func Load(path string) error {
	if path != "" {
		viper.SetConfigFile(path)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("json")
		viper.AddConfigPath("data")
	}

	viper.AutomaticEnv()
	viper.SetEnvPrefix(APP_NAME)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	setDefaults()

	if err := viper.ReadInConfig(); err == nil {
		log.Infof("Using config file: %s", viper.ConfigFileUsed())
	} else {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Infof("Config file not found, creating default config")
			if err := os.MkdirAll("data", 0755); err != nil {
				log.Errorf("Failed to create data directory: %v", err)
			}
			if err := viper.SafeWriteConfigAs("data/config.json"); err != nil {
				log.Errorf("Failed to create default config: %v", err)
			}
		} else {
			return fmt.Errorf("error reading config file: %w", err)
		}
	}

	if err := viper.Unmarshal(&AppConfig); err != nil {
		return fmt.Errorf("unable to decode config into struct: %w", err)
	}
	return nil
}

func setDefaults() {
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("database.type", "sqlite")
	viper.SetDefault("database.path", "data/data.db")
	viper.SetDefault("log.level", "info")
	viper.SetDefault("billing.base_url", "")
	viper.SetDefault("billing.service_token", "")
	viper.SetDefault("billing.service_token_file", "")
	viper.SetDefault("billing.timeout_seconds", 10)
	viper.SetDefault("billing.pending_max_amount_microunits", 10_000_000)
	viper.SetDefault("billing.pending_max_count", 100)
	viper.SetDefault("billing.pending_max_age_seconds", 3600)
	viper.SetDefault("billing.settlement_poll_millis", 250)
}
