package server

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/billing"
	"github.com/bestruirui/octopus/internal/conf"
	"github.com/bestruirui/octopus/internal/relay"
	_ "github.com/bestruirui/octopus/internal/server/handlers"
	"github.com/bestruirui/octopus/internal/server/middleware"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/server/router"
	"github.com/bestruirui/octopus/internal/settlement"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/static"
	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/llm"
)

var (
	httpSrv          http.Server
	settlementWorker *settlement.Service
)

func Start() error {
	if strings.TrimSpace(conf.AppConfig.Billing.BaseURL) != "" ||
		strings.TrimSpace(conf.AppConfig.Billing.ServiceToken) != "" ||
		strings.TrimSpace(conf.AppConfig.Billing.ServiceTokenFile) != "" {
		if conf.AppConfig.Billing.TimeoutSeconds < 1 || conf.AppConfig.Billing.TimeoutSeconds > 60 {
			return fmt.Errorf("billing timeout must be between 1 and 60 seconds")
		}
		serviceToken, err := billingServiceToken(conf.AppConfig.Billing)
		if err != nil {
			return fmt.Errorf("billing configuration is invalid: %w", err)
		}
		client, err := billing.NewHTTPClient(
			conf.AppConfig.Billing.BaseURL,
			serviceToken,
			&http.Client{Timeout: time.Duration(conf.AppConfig.Billing.TimeoutSeconds) * time.Second},
		)
		if err != nil {
			return fmt.Errorf("billing configuration is invalid: %w", err)
		}
		billing.SetDefaultClient(client)
		settlementWorker, err = settlement.NewService(client, settlement.Limits{
			MaxPendingAmountMicrounits: conf.AppConfig.Billing.PendingMaxAmountMicrounits,
			MaxPendingCount:            conf.AppConfig.Billing.PendingMaxCount,
			MaxPendingAge:              time.Duration(conf.AppConfig.Billing.PendingMaxAgeSeconds) * time.Second,
		}, time.Duration(conf.AppConfig.Billing.SettlementPollMillis)*time.Millisecond)
		if err != nil {
			return fmt.Errorf("settlement configuration is invalid: %w", err)
		}
		settlement.SetDefaultService(settlementWorker)
		settlementWorker.Start()
	}
	if conf.IsDebug() {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		resp.Error(c, http.StatusInternalServerError, resp.ErrInternalServer)
		c.Abort()
	}))

	if conf.IsDebug() {
		r.Use(middleware.Logger())
	}
	r.Use(middleware.Cors())
	r.Use(middleware.StaticEmbed("/", static.StaticFS))

	registerRelayRoutes(r)
	router.RegisterAll(r)

	httpSrv.Addr = fmt.Sprintf("%s:%d", conf.AppConfig.Server.Host, conf.AppConfig.Server.Port)
	httpSrv.Handler = r
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("http server listen and serve error: %v", err)
		}
	}()
	return nil
}

func billingServiceToken(config conf.Billing) (string, error) {
	direct := strings.TrimSpace(config.ServiceToken)
	path := strings.TrimSpace(config.ServiceTokenFile)
	if direct != "" && path != "" {
		return "", fmt.Errorf("configure either billing service_token or service_token_file")
	}
	if path == "" {
		return direct, nil
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("billing service token file must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("read billing service token file metadata: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || info.Size() > 16*1024 {
		return "", fmt.Errorf("billing service token file is insecure")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read billing service token file: %w", err)
	}
	token := strings.TrimSpace(string(body))
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return "", fmt.Errorf("billing service token file is invalid")
	}
	return token, nil
}

func Close() error {
	var errs []error
	if err := httpSrv.Close(); err != nil {
		errs = append(errs, err)
	}
	if settlementWorker != nil {
		if err := settlementWorker.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	settlement.SetDefaultService(nil)
	billing.SetDefaultClient(nil)
	return errors.Join(errs...)
}

func registerRelayRoutes(r *gin.Engine) {
	v1 := r.Group("/v1", middleware.APIKeyAuth())
	v1.POST("/chat/completions", middleware.RequireJSON(), relay.Handler(llm.APIFormatOpenAIChatCompletion))
	v1.POST("/responses", middleware.RequireJSON(), relay.Handler(llm.APIFormatOpenAIResponse))
	v1.POST("/messages", middleware.RequireJSON(), relay.Handler(llm.APIFormatAnthropicMessage))
	v1.POST("/embeddings", middleware.RequireJSON(), relay.Handler(llm.APIFormatOpenAIEmbedding))
	v1.POST("/images/generations", middleware.RequireJSON(), relay.Handler(llm.APIFormatOpenAIImageGeneration))
	v1.POST("/images/edits", relay.Handler(llm.APIFormatOpenAIImageEdit))
	v1.POST("/images/variations", relay.Handler(llm.APIFormatOpenAIImageVariation))
}
