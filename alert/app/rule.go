package app

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"text/scanner"
	"text/template"
	"time"

	"github.com/taosdata/alert/app/expr"
	"github.com/taosdata/alert/models"
	"github.com/taosdata/alert/utils"
	"github.com/taosdata/alert/utils/log"
)

type Duration struct{ time.Duration }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) doUnmarshal(v interface{}) error {
	switch value := v.(type) {
	case float64:
		*d = Duration{time.Duration(value)}
		return nil
	case string:
		if duration, e := time.ParseDuration(value); e != nil {
			return e
		} else {
			*d = Duration{duration}
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if e := json.Unmarshal(b, &v); e != nil {
		return e
	}
	return d.doUnmarshal(v)
}

const (
	AlertStateWaiting = iota
	AlertStatePending
	AlertStateFiring
)

type Alert struct {
	State         uint8                  `json:"-"`
	LastRefreshAt time.Time              `json:"-"`
	StartsAt      time.Time              `json:"startsAt,omitempty"`
	EndsAt        time.Time              `json:"endsAt,omitempty"`
	Values        map[string]interface{} `json:"values"`
	Labels        map[string]string      `json:"labels"`
	Annotations   map[string]string      `json:"annotations"`
}

func (alert *Alert) doRefresh(firing bool, rule *Rule) bool {
	switch {
	case (!firing) && (alert.State == AlertStateWaiting):
		return false

	case (!firing) && (alert.State == AlertStatePending):
		alert.State = AlertStateWaiting
		return false

	case (!firing) && (alert.State == AlertStateFiring):
		alert.State = AlertStateWaiting
		alert.EndsAt = time.Now()

	case firing && (alert.State == AlertStateWaiting):
		alert.StartsAt = time.Now()
		alert.EndsAt = time.Time{}
		if rule.For.Nanoseconds() > 0 {
			alert.State = AlertStatePending
			return false
		}
		alert.State = AlertStateFiring

	case firing && (alert.State == AlertStatePending):
		if time.Now().Sub(alert.StartsAt) < rule.For.Duration {
			return false
		}
		alert.StartsAt = alert.StartsAt.Add(rule.For.Duration)
		alert.EndsAt = time.Time{}
		alert.State = AlertStateFiring

	case firing && (alert.State == AlertStateFiring):
	}

	return true
}

func (alert *Alert) refresh(rule *Rule, values map[string]interface{}) {
	alert.LastRefreshAt = time.Now()

	defer func() {
		switch x := recover().(type) {
		case nil:
		case error:
			rule.setState(RuleStateError)
			log.Errorf("[%s]: failed to evaluate: %s", rule.Name, x.Error())
		default:
			rule.setState(RuleStateError)
			log.Errorf("[%s]: failed to evaluate: unknown error", rule.Name)
		}
	}()

	alert.Values = values
	res := rule.Expr.Eval(func(key string) interface{} {
		// ToLower is required as column name in result is in lower case
		i := alert.Values[strings.ToLower(key)]
		switch v := i.(type) {
		case int8:
			return int64(v)
		case int16:
			return int64(v)
		case int:
			return int64(v)
		case int32:
			return int64(v)
		case float32:
			return float64(v)
		default:
			return v
		}
	})

	val, ok := res.(bool)
	if !ok {
		rule.setState(RuleStateError)
		log.Errorf("[%s]: result type is not bool", rule.Name)
		return
	}

	if !alert.doRefresh(val, rule) {
		return
	}

	buf := bytes.Buffer{}
	alert.Annotations = map[string]string{}
	for k, v := range rule.Annotations {
		if e := v.Execute(&buf, alert); e != nil {
			log.Errorf("[%s]: failed to generate annotation '%s': %s", rule.Name, k, e.Error())
		} else {
			alert.Annotations[k] = buf.String()
		}
		buf.Reset()
	}

	buf.Reset()
	if e := json.NewEncoder(&buf).Encode(alert); e != nil {
		log.Errorf("[%s]: failed to serialize alert to JSON: %s", rule.Name, e.Error())
	} else {
		chAlert <- buf.String()
	}
}

const (
	RuleStateNormal = iota
	RuleStateError
	RuleStateDisabled
	RuleStateRunning = 0x04
)

type Rule struct {
	Name           string                        `json:"name"`
	State          uint32                        `json:"state"`
	SQL            string                        `json:"sql"`
	GroupByCols    []string                      `json:"-"`
	For            Duration                      `json:"for"`
	Period         Duration                      `json:"period"`
	NextRunTime    time.Time                     `json:"-"`
	RawExpr        string                        `json:"expr"`
	Expr           expr.Expr                     `json:"-"`
	Labels         map[string]string             `json:"labels"`
	RawAnnotations map[string]string             `json:"annotations"`
	Annotations    map[string]*template.Template `json:"-"`
	Alerts         sync.Map                      `json:"-"`
}

func (rule *Rule) clone() *Rule {
	return &Rule{
		Name:           rule.Name,
		State:          RuleStateNormal,
		SQL:            rule.SQL,
		GroupByCols:    rule.GroupByCols,
		For:            rule.For,
		Period:         rule.Period,
		NextRunTime:    time.Time{},
		RawExpr:        rule.RawExpr,
		Expr:           rule.Expr,
		Labels:         rule.Labels,
		RawAnnotations: rule.RawAnnotations,
		Annotations:    rule.Annotations,
		// don't copy alerts
	}
}

func (rule *Rule) setState(s uint32) {
	for {
		old := atomic.LoadUint32(&rule.State)
		new := old&0xffffffc0 | s
		if atomic.CompareAndSwapUint32(&rule.State, old, new) {
			break
		}
	}
}

func (rule *Rule) state() uint32 {
	return atomic.LoadUint32(&rule.State) & 0xffffffc0
}

func (rule *Rule) isEnabled() bool {
	state := atomic.LoadUint32(&rule.State)
	return state&RuleStateDisabled == 0
}

func (rule *Rule) setNextRunTime(tm time.Time) {
	rule.NextRunTime = tm.Round(rule.Period.Duration)
	if rule.NextRunTime.Before(tm) {
		rule.NextRunTime = rule.NextRunTime.Add(rule.Period.Duration)
	}
}

func parseGroupBy(sql string) (cols []string, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()

	s := scanner.Scanner{
		Error: func(s *scanner.Scanner, msg string) {
			panic(errors.New(msg))
		},
		Mode: scanner.ScanIdents | scanner.ScanInts | scanner.ScanFloats,
	}

	s.Init(strings.NewReader(sql))
	if s.Scan() != scanner.Ident || strings.ToLower(s.TokenText()) != "select" {
		err = errors.New("only select statement is allowed.")
		return
	}

	hasGroupBy := false
	for t := s.Scan(); t != scanner.EOF; t = s.Scan() {
		if t != scanner.Ident {
			continue
		}
		if strings.ToLower(s.TokenText()) != "group" {
			continue
		}
		if s.Scan() != scanner.Ident {
			continue
		}
		if strings.ToLower(s.TokenText()) == "by" {
			hasGroupBy = true
			break
		}
	}

	if !hasGroupBy {
		return
	}

	for {
		if s.Scan() != scanner.Ident {
			err = errors.New("SQL statement syntax error.")
			return
		}
		col := strings.ToLower(s.TokenText())
		cols = append(cols, col)
		if s.Scan() != ',' {
			break
		}
	}

	return
}

func (rule *Rule) parseGroupBy() (err error) {
	cols, e := parseGroupBy(rule.SQL)
	if e == nil {
		rule.GroupByCols = cols
	}
	return nil
}

func (rule *Rule) getAlert(values map[string]interface{}) *Alert {
	sb := strings.Builder{}
	for _, name := range rule.GroupByCols {
		value := values[name]
		if value == nil {
		} else {
			sb.WriteString(fmt.Sprint(value))
		}
		sb.WriteByte('_')
	}

	var alert *Alert
	key := sb.String()

	if v, ok := rule.Alerts.Load(key); ok {
		alert = v.(*Alert)
	}
	if alert == nil {
		alert = &Alert{Labels: map[string]string{}}
		for k, v := range rule.Labels {
			alert.Labels[k] = v
		}
		for _, name := range rule.GroupByCols {
			value := values[name]
			if value == nil {
				alert.Labels[name] = ""
			} else {
				alert.Labels[name] = fmt.Sprint(value)
			}
		}
		rule.Alerts.Store(key, alert)
	}

	return alert
}

func (rule *Rule) preRun(tm time.Time) bool {
	if tm.Before(rule.NextRunTime) {
		return false
	}
	rule.setNextRunTime(tm)

	for {
		state := atomic.LoadUint32(&rule.State)
		if state != RuleStateNormal {
			return false
		}
		if atomic.CompareAndSwapUint32(&rule.State, state, RuleStateRunning) {
			break
		}
	}
	return true
}

func (rule *Rule) run(db *sql.DB) {
	rows, e := db.Query(rule.SQL)
	if e != nil {
		log.Errorf("[%s]: failed to query TDengine: %s", rule.Name, e.Error())
		return
	}

	cols, e := rows.ColumnTypes()
	if e != nil {
		log.Errorf("[%s]: unable to get column information: %s", rule.Name, e.Error())
		return
	}

	for rows.Next() {
		values := make([]interface{}, 0, len(cols))
		for range cols {
			var v interface{}
			values = append(values, &v)
		}
		rows.Scan(values...)

		m := make(map[string]interface{})
		for i, col := range cols {
			name := strings.ToLower(col.Name())
			m[name] = *(values[i].(*interface{}))
		}

		alert := rule.getAlert(m)
		alert.refresh(rule, m)
	}

	now := time.Now()
	rule.Alerts.Range(func(k, v interface{}) bool {
		alert := v.(*Alert)
		if now.Sub(alert.LastRefreshAt) > rule.Period.Duration*10 {
			rule.Alerts.Delete(k)
		}
		return true
	})
}

func (rule *Rule) postRun() {
	for {
		old := atomic.LoadUint32(&rule.State)
		new := old & ^uint32(RuleStateRunning)
		if atomic.CompareAndSwapUint32(&rule.State, old, new) {
			break
		}
	}
}

func newRule(str string) (*Rule, error) {
	rule := Rule{}

	e := json.NewDecoder(strings.NewReader(str)).Decode(&rule)
	if e != nil {
		return nil, e
	}

	if rule.Period.Nanoseconds() <= 0 {
		rule.Period = Duration{time.Minute}
	}
	rule.setNextRunTime(time.Now())

	if rule.For.Nanoseconds() < 0 {
		rule.For = Duration{0}
	}

	if e = rule.parseGroupBy(); e != nil {
		return nil, e
	}

	if expr, e := expr.Compile(rule.RawExpr); e != nil {
		return nil, e
	} else {
		rule.Expr = expr
	}

	rule.Annotations = map[string]*template.Template{}
	for k, v := range rule.RawAnnotations {
		v = reValue.ReplaceAllStringFunc(v, func(s string) string {
			// as column name in query result is always in lower case,
			// we need to convert value reference in annotations to
			// lower case
			return strings.ToLower(s)
		})
		text := "{{$labels := .Labels}}{{$values := .Values}}" + v
		tmpl, e := template.New(k).Parse(text)
		if e != nil {
			return nil, e
		}
		rule.Annotations[k] = tmpl
	}

	return &rule, nil
}

const (
	batchSize = 1024
)

var (
	rules   sync.Map
	wg      sync.WaitGroup
	chStop  = make(chan struct{})
	chAlert = make(chan string, batchSize)
	reValue = regexp.MustCompile(`\$values\.[_a-zA-Z0-9]+`)
)

func runRules() {
	defer wg.Done()

	db, e := sql.Open("taosSql", utils.Cfg.TDengine)
	if e != nil {
		log.Fatal("failed to connect to TDengine: ", e.Error())
	}
	defer db.Close()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

LOOP:
	for {
		var tm time.Time
		select {
		case <-chStop:
			close(chAlert)
			break LOOP
		case tm = <-ticker.C:
		}

		rules.Range(func(k, v interface{}) bool {
			rule := v.(*Rule)
			if !rule.preRun(tm) {
				return true
			}

			wg.Add(1)
			go func(rule *Rule) {
				defer wg.Done()
				defer rule.postRun()
				rule.run(db)
			}(rule)

			return true
		})
	}
}

func doPushAlerts(alerts []string) {
	defer wg.Done()

	if len(utils.Cfg.Receivers.AlertManager) == 0 {
		return
	}

	buf := bytes.Buffer{}
	buf.WriteByte('[')
	for i, alert := range alerts {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(alert)
	}
	buf.WriteByte(']')

	log.Debug(buf.String())

	resp, e := http.DefaultClient.Post(utils.Cfg.Receivers.AlertManager, "application/json", &buf)
	if e != nil {
		log.Errorf("failed to push alerts to downstream: %s", e.Error())
		return
	}
	resp.Body.Close()
}

func pushAlerts() {
	defer wg.Done()

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	alerts := make([]string, 0, batchSize)

LOOP:
	for {
		select {
		case alert := <-chAlert:
			if utils.Cfg.Receivers.Console {
				fmt.Print(alert)
			}
			if len(alert) == 0 {
				if len(alerts) > 0 {
					wg.Add(1)
					doPushAlerts(alerts)
				}
				break LOOP
			}
			if len(alerts) == batchSize {
				wg.Add(1)
				go doPushAlerts(alerts)
				alerts = make([]string, 0, batchSize)
			}
			alerts = append(alerts, alert)

		case <-ticker.C:
			if len(alerts) > 0 {
				wg.Add(1)
				go doPushAlerts(alerts)
				alerts = make([]string, 0, batchSize)
			}
		}
	}
}

func loadRuleFromDatabase() error {
	allRules, e := models.LoadAllRule()
	if e != nil {
		log.Error("failed to load rules from database:", e.Error())
		return e
	}

	count := 0
	for _, r := range allRules {
		rule, e := newRule(r.Content)
		if e != nil {
			log.Errorf("[%s]: parse failed: %s", r.Name, e.Error())
			continue
		}
		if !r.Enabled {
			rule.setState(RuleStateDisabled)
		}
		rules.Store(rule.Name, rule)
		count++
	}
	log.Infof("total %d rules loaded", count)
	return nil
}

func loadRuleFromFile() error {
	f, e := os.Open(utils.Cfg.RuleFile)
	if e != nil {
		log.Error("failed to load rules from file:", e.Error())
		return e
	}
	defer f.Close()

	var allRules []Rule
	e = json.NewDecoder(f).Decode(&allRules)
	if e != nil {
		log.Error("failed to parse rule file:", e.Error())
		return e
	}

	for i := 0; i < len(allRules); i++ {
		rule := &allRules[i]
		rules.Store(rule.Name, rule)
	}
	log.Infof("total %d rules loaded", len(allRules))

	return nil
}

func initRule() error {
	if len(utils.Cfg.Database) > 0 {
		if e := loadRuleFromDatabase(); e != nil {
			return e
		}
	} else {
		if e := loadRuleFromFile(); e != nil {
			return e
		}
	}

	wg.Add(2)
	go runRules()
	go pushAlerts()

	return nil
}

func uninitRule() error {
	close(chStop)
	wg.Wait()
	return nil
}
