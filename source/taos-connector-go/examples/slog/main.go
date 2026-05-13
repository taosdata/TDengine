package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"

	tLog "github.com/taosdata/driver-go/v3/log"
	_ "github.com/taosdata/driver-go/v3/taosWS"
)

type slogAdapter struct {
	logger *slog.Logger
}

func (a *slogAdapter) Log(level tLog.LogLevel, qid uint64, msg string) {
	a.logger.LogAttrs(
		context.Background(),
		toSlogLevel(level),
		msg,
		slog.String("qid", fmt.Sprintf("0x%x", qid)),
		slog.Int("driver_level", int(level)),
	)
}

func toSlogLevel(level tLog.LogLevel) slog.Level {
	switch level {
	case tLog.LogLevelDebug:
		return slog.LevelDebug
	case tLog.LogLevelInfo:
		return slog.LevelInfo
	case tLog.LogLevelWarn:
		return slog.LevelWarn
	case tLog.LogLevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func main() {
	// You can override the endpoint with TAOS_WS_DSN.
	dsn := os.Getenv("TAOS_WS_DSN")
	if dsn == "" {
		dsn = "root:taosdata@ws(localhost:6041)/"
	}

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)

	tLog.SetLogger(&slogAdapter{logger: logger})
	tLog.SetLevel(tLog.LogLevelInfo)
	tLog.SetPacketLogging(true)
	tLog.SetMaxPacketLogBytes(512)

	db, err := sql.Open("taosWS", dsn)
	if err != nil {
		log.Fatalf("open taosWS failed, dsn=%s, err=%v", dsn, err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("ping failed, dsn=%s, err=%v", dsn, err)
	}
	fmt.Println("connected to", dsn)

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS example_slog"); err != nil {
		log.Fatalf("create database failed: %v", err)
	}
	if _, err := db.Exec("CREATE STABLE IF NOT EXISTS example_slog.meters (ts TIMESTAMP, current FLOAT) TAGS (location BINARY(64))"); err != nil {
		log.Fatalf("create stable failed: %v", err)
	}
	if _, err := db.Exec("INSERT INTO example_slog.d001 USING example_slog.meters TAGS('beijing') VALUES (NOW, 12.3)"); err != nil {
		log.Fatalf("insert failed: %v", err)
	}

	var cnt int64
	if err := db.QueryRow("SELECT COUNT(*) FROM example_slog.meters").Scan(&cnt); err != nil {
		log.Fatalf("query count failed: %v", err)
	}
	fmt.Printf("row count in example_slog.meters: %d\n", cnt)
}
