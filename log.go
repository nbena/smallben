package smallben

import (
	"github.com/go-logr/zapr"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

// DefaultLogger is the Zap logger
// that is used in case none is provided.
var DefaultLogger cron.Logger

func init() {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		// recover using example
		zapLogger = zap.NewExample()
	}
	DefaultLogger = zapr.NewLogger(zapLogger)
}
