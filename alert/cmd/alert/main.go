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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/taosdata/alert/app"
	"github.com/taosdata/alert/models"
	"github.com/taosdata/alert/utils"
	"github.com/taosdata/alert/utils/log"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/taosdata/driver-go/taosSql"
)

type httpHandler struct {
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	path := r.URL.Path
	http.DefaultServeMux.ServeHTTP(w, r)
	duration := time.Now().Sub(start)
	log.Debugf("[%s]\t%s\t%s", r.Method, path, duration)
}

func serveWeb() *http.Server {
	log.Info("Listening at port: ", utils.Cfg.Port)

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(int(utils.Cfg.Port)),
		Handler: &httpHandler{},
	}
	go func() {
		if e := srv.ListenAndServe(); e != nil {
			log.Error(e.Error())
		}
	}()

	return srv
}

func copyFile(dst, src string) error {
	if dst == src {
		return nil
	}

	in, e := os.Open(src)
	if e != nil {
		return e
	}
	defer in.Close()

	out, e := os.Create(dst)
	if e != nil {
		return e
	}
	defer out.Close()

	_, e = io.Copy(out, in)
	return e
}

func doSetup(cfgPath string) error {
	exePath, e := os.Executable()
	if e != nil {
		fmt.Fprintf(os.Stderr, "failed to get executable path: %s\n", e.Error())
		return e
	}

	if !filepath.IsAbs(cfgPath) {
		dir := filepath.Dir(exePath)
		cfgPath = filepath.Join(dir, cfgPath)
	}

	e = copyFile("/etc/taos/alert.cfg", cfgPath)
	if e != nil {
		fmt.Fprintf(os.Stderr, "failed copy configuration file: %s\n", e.Error())
		return e
	}

	f, e := os.Create("/etc/systemd/system/alert.service")
	if e != nil {
		fmt.Printf("failed to create alert service: %s\n", e.Error())
		return e
	}
	defer f.Close()

	const content = `[Unit]
Description=Alert (TDengine Alert Service)
After=syslog.target
After=network.target

[Service]
RestartSec=2s
Type=simple
WorkingDirectory=/var/lib/taos/
ExecStart=%s -cfg /etc/taos/alert.cfg
Restart=always

[Install]
WantedBy=multi-user.target
`
	_, e = fmt.Fprintf(f, content, exePath)
	if e != nil {
		fmt.Printf("failed to create alert.service: %s\n", e.Error())
		return e
	}

	return nil
}

var version = "2.0.0.1s"

func main() {
	var (
		cfgPath     string
		setup       bool
		showVersion bool
	)
	flag.StringVar(&cfgPath, "cfg", "alert.cfg", "path of configuration file")
	flag.BoolVar(&setup, "setup", false, "setup the service as a daemon")
	flag.BoolVar(&showVersion, "version", false, "show version information")
	flag.Parse()

	if showVersion {
		fmt.Println("TDengine alert v" + version)
		return
	}

	if setup {
		if runtime.GOOS == "linux" {
			doSetup(cfgPath)
		} else {
			fmt.Fprintln(os.Stderr, "can only run as a daemon mode in linux.")
		}
		return
	}

	if e := utils.LoadConfig(cfgPath); e != nil {
		fmt.Fprintln(os.Stderr, "failed to load configuration")
		return
	}

	if e := log.Init(); e != nil {
		fmt.Fprintln(os.Stderr, "failed to initialize logger:", e.Error())
		return
	}
	defer log.Sync()

	if e := models.Init(); e != nil {
		log.Fatal("failed to initialize database:", e.Error())
	}

	if e := app.Init(); e != nil {
		log.Fatal("failed to initialize application:", e.Error())
	}
	// start web server
	srv := serveWeb()

	// wait `Ctrl-C` or `Kill` to exit, `Kill` does not work on Windows
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)
	signal.Notify(interrupt, os.Kill)
	<-interrupt
	fmt.Println("'Ctrl + C' received, exiting...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	srv.Shutdown(ctx)
	cancel()

	app.Uninit()
	models.Uninit()
}
