/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package log

import (
	"github.com/taosdata/alert/utils"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

func Init() error {
	var cfg zap.Config

	if utils.Cfg.Log.Level == "debug" {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}

	if len(utils.Cfg.Log.Path) > 0 {
		cfg.OutputPaths = []string{utils.Cfg.Log.Path}
	}

	l, e := cfg.Build()
	if e != nil {
		return e
	}

	logger = l.Sugar()
	return nil
}

// Debug package logger
func Debug(args ...interface{}) {
	logger.Debug(args...)
}

// Debugf package logger
func Debugf(template string, args ...interface{}) {
	logger.Debugf(template, args...)
}

// Info package logger
func Info(args ...interface{}) {
	logger.Info(args...)
}

// Infof package logger
func Infof(template string, args ...interface{}) {
	logger.Infof(template, args...)
}

// Warn package logger
func Warn(args ...interface{}) {
	logger.Warn(args...)
}

// Warnf package logger
func Warnf(template string, args ...interface{}) {
	logger.Warnf(template, args...)
}

// Error package logger
func Error(args ...interface{}) {
	logger.Error(args...)
}

// Errorf package logger
func Errorf(template string, args ...interface{}) {
	logger.Errorf(template, args...)
}

// Fatal package logger
func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

// Fatalf package logger
func Fatalf(template string, args ...interface{}) {
	logger.Fatalf(template, args...)
}

// Panic package logger
func Panic(args ...interface{}) {
	logger.Panic(args...)
}

// Panicf package logger
func Panicf(template string, args ...interface{}) {
	logger.Panicf(template, args...)
}

func Sync() error {
	return logger.Sync()
}
