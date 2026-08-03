package server

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/billing"
	"github.com/bestruirui/octopus/internal/conf"
	"github.com/bestruirui/octopus/internal/relay"
	_ "github.com/bestruirui/octopus/internal/server/handlers"
	"github.com/bestruirui/octopus/internal/server/middleware"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/server/router"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/static"
	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/llm"
)

var httpSrv http.Server

func Start() error {
	if strings.TrimSpace(conf.AppConfig.Billing.BaseURL) != "" || strings.TrimSpace(conf.AppConfig.Billing.ServiceToken) != "" {
		if conf.AppConfig.Billing.TimeoutSeconds < 1 || conf.AppConfig.Billing.TimeoutSeconds > 60 {
			return fmt.Errorf("billing timeout must be between 1 and 60 seconds")
		}
		client, err := billing.NewHTTPClient(
			conf.AppConfig.Billing.BaseURL,
			conf.AppConfig.Billing.ServiceToken,
			&http.Client{Timeout: time.Duration(conf.AppConfig.Billing.TimeoutSeconds) * time.Second},
		)
		if err != nil {
			return fmt.Errorf("billing configuration is invalid: %w", err)
		}
		billing.SetDefaultClient(client)
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

func Close() error {
	return httpSrv.Close()
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
