package reporter

import (
	"context"
	"log/slog"

	"github.com/gopherex/xprobe/pkg/probe"
)

// Slog returns a Reporter that logs every transition via the given slog.Logger.
// Up transitions log at Info, anything else at Warn.
func Slog(logger *slog.Logger) Reporter {
	if logger == nil {
		logger = slog.Default()
	}
	return Func(func(ctx context.Context, ev Event) {
		level := slog.LevelWarn
		if ev.Cur == probe.StatusUp {
			level = slog.LevelInfo
		}
		logger.LogAttrs(ctx, level, "probe status changed",
			slog.String("name", ev.Name),
			slog.String("prev", ev.Prev.String()),
			slog.String("cur", ev.Cur.String()),
		)
	})
}
