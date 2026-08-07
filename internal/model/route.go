package model

import "github.com/looplj/axonhub/llm"

// IsImageRoute classifies an image route from either structured signal.
// Content and keyword inspection must never participate in this decision.
func IsImageRoute(routeType, routeFormat string) bool {
	if llm.RequestType(routeType) == llm.RequestTypeImage {
		return true
	}
	switch llm.APIFormat(routeFormat) {
	case llm.APIFormatOpenAIImageGeneration,
		llm.APIFormatOpenAIImageEdit,
		llm.APIFormatOpenAIImageVariation:
		return true
	default:
		return false
	}
}
