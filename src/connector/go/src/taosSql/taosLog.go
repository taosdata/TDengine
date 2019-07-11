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

package taosSql

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// Various errors the driver might return.
var (
	errInvalidConn    = errors.New("invalid connection")
	errConnNoExist    = errors.New("no existent connection ")
)

var taosLog *log.Logger

// SetLogger is used to set the logger for critical errors.
// The initial logger
func taosLogInit() {
	cfgName := "/etc/taos/taos.cfg"
	logNameDefault := "/var/log/taos/taosgo.log"
	var logName string

	// get log path from cfg file
	cfgFile, err := os.OpenFile(cfgName, os.O_RDONLY, 0644)
	defer cfgFile.Close()
	if err != nil {
		fmt.Println(err)
		logName = logNameDefault
	} else {
		logName, err = getLogNameFromCfg(cfgFile)
		if err != nil {
			fmt.Println(err)
			logName = logNameDefault
		}
	}

	logFile, err := os.OpenFile(logName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	taosLog = log.New(logFile, "", log.LstdFlags)
	taosLog.SetPrefix("TAOS DRIVER ")
	taosLog.SetFlags(log.LstdFlags|log.Lshortfile)
}

func getLogNameFromCfg(f *os.File) (string, error) {
		// Create file buf, *Reader
	r := bufio.NewReader(f)
	for {
		//read one line, return to slice b
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		// Remove space of left and right
		s := strings.TrimSpace(string(b))
		if strings.Index(s, "#") == 0 {
			// comment line
			continue
		}

		if len(s) == 0 {
			continue
		}

		var ns string
		// If there is a comment on the right of the line, must be remove
		index := strings.Index(s, "#")
		if index > 0 {
			// Gets the string to the left of the comment to determine whether it is empty
			ns = s[:index]
			if len(ns) == 0 {
				continue
			}
		} else {
			ns = s;
		}

		ss := strings.Fields(ns)
        if strings.Compare("logDir", ss[0]) != 0 {
        	continue
		}

		if len(ss) < 2 {
			break
		}

		// Add a filename after the path
		logName := ss[1] + "/taosgo.log"
		return logName,nil
	}

    return "", errors.New("no config log path, use default")
}

