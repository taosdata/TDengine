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

package app

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/taosdata/alert/models"
	"github.com/taosdata/alert/utils"
	"github.com/taosdata/alert/utils/log"
)

func Init() error {
	if e := initRule(); e != nil {
		return e
	}

	http.HandleFunc("/api/list-rule", onListRule)
	http.HandleFunc("/api/list-alert", onListAlert)
	http.HandleFunc("/api/update-rule", onUpdateRule)
	http.HandleFunc("/api/enable-rule", onEnableRule)
	http.HandleFunc("/api/delete-rule", onDeleteRule)

	return nil
}

func Uninit() error {
	uninitRule()
	return nil
}

func onListRule(w http.ResponseWriter, r *http.Request) {
	var res []*Rule
	rules.Range(func(k, v interface{}) bool {
		res = append(res, v.(*Rule))
		return true
	})

	w.Header().Add("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(res)
}

func onListAlert(w http.ResponseWriter, r *http.Request) {
	var alerts []*Alert
	rn := r.URL.Query().Get("rule")
	rules.Range(func(k, v interface{}) bool {
		if len(rn) > 0 && rn != k.(string) {
			return true
		}

		rule := v.(*Rule)
		rule.Alerts.Range(func(k, v interface{}) bool {
			alert := v.(*Alert)
			// TODO: not go-routine safe
			if alert.State != AlertStateWaiting {
				alerts = append(alerts, v.(*Alert))
			}
			return true
		})
		return true
	})
	w.Header().Add("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(alerts)
}

func onUpdateRule(w http.ResponseWriter, r *http.Request) {
	data, e := ioutil.ReadAll(r.Body)
	if e != nil {
		log.Error("failed to read request body: ", e.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	rule, e := newRule(string(data))
	if e != nil {
		log.Error("failed to parse rule: ", e.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if e = doUpdateRule(rule, string(data)); e != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func doUpdateRule(rule *Rule, ruleStr string) error {
	if _, ok := rules.Load(rule.Name); ok {
		if len(utils.Cfg.Database) > 0 {
			e := models.UpdateRule(rule.Name, ruleStr)
			if e != nil {
				log.Errorf("[%s]: update failed: %s", rule.Name, e.Error())
				return e
			}
		}
		log.Infof("[%s]: update succeeded.", rule.Name)
	} else {
		if len(utils.Cfg.Database) > 0 {
			e := models.AddRule(&models.Rule{
				Name:    rule.Name,
				Content: ruleStr,
			})
			if e != nil {
				log.Errorf("[%s]: add failed: %s", rule.Name, e.Error())
				return e
			}
		}
		log.Infof("[%s]: add succeeded.", rule.Name)
	}

	rules.Store(rule.Name, rule)
	return nil
}

func onEnableRule(w http.ResponseWriter, r *http.Request) {
	var rule *Rule
	name := r.URL.Query().Get("name")
	enable := strings.ToLower(r.URL.Query().Get("enable")) == "true"

	if x, ok := rules.Load(name); ok {
		rule = x.(*Rule)
	} else {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if rule.isEnabled() == enable {
		return
	}

	if len(utils.Cfg.Database) > 0 {
		if e := models.EnableRule(name, enable); e != nil {
			if enable {
				log.Errorf("[%s]: enable failed: ", name, e.Error())
			} else {
				log.Errorf("[%s]: disable failed: ", name, e.Error())
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	if enable {
		rule = rule.clone()
		rule.setNextRunTime(time.Now())
		rules.Store(rule.Name, rule)
		log.Infof("[%s]: enable succeeded.", name)
	} else {
		rule.setState(RuleStateDisabled)
		log.Infof("[%s]: disable succeeded.", name)
	}
}

func onDeleteRule(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if len(name) == 0 {
		return
	}
	if e := doDeleteRule(name); e != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func doDeleteRule(name string) error {
	if len(utils.Cfg.Database) > 0 {
		if e := models.DeleteRule(name); e != nil {
			log.Errorf("[%s]: delete failed: %s", name, e.Error())
			return e
		}
	}
	rules.Delete(name)
	log.Infof("[%s]: delete succeeded.", name)
	return nil
}
