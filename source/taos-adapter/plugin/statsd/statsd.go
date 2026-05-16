package statsd

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/parsers/graphite"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
)

var sampleConfig string

const (
	UDPMaxPacketSize int = 64 * 1024

	defaultFieldName = "value"

	defaultSeparator = "_"

	parserGoRoutines = 5
)

var errParsing = errors.New("error parsing statsd line")

// Number will get parsed as an int or float depending on what is passed
type Number float64

func (n *Number) UnmarshalTOML(b []byte) error {
	value, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return err
	}

	*n = Number(value)
	return nil
}

// Statsd allows the importing of statsd and dogstatsd data.
type Statsd struct {
	User     string
	Password string
	Token    string
	// Protocol used on listener - udp or tcp
	Protocol string `toml:"protocol"`

	// Address & Port to serve from
	ServiceAddress string

	// Number of messages allowed to queue up in between calls to Gather. If this
	// fills up, packets will get dropped until the next Gather interval is ran.
	AllowedPendingMessages int

	// Percentiles specifies the percentiles that will be calculated for timing
	// and histogram stats.
	Percentiles     []Number
	PercentileLimit int

	DeleteGauges   bool
	DeleteCounters bool
	DeleteSets     bool
	DeleteTimings  bool
	ConvertNames   bool `toml:"convert_names" deprecated:"0.12.0;2.0.0;use 'metric_separator' instead"`

	// MetricSeparator is the separator between parts of the metric name.
	MetricSeparator string
	// This flag enables parsing of tags in the dogstatsd extension to the
	// statsd protocol (http://docs.datadoghq.com/guides/dogstatsd/)
	ParseDataDogTags bool `toml:"parse_data_dog_tags" deprecated:"1.10.0;use 'datadog_extensions' instead"`

	// Parses extensions to statsd in the datadog statsd format
	// currently supports metrics and datadog tags.
	// http://docs.datadoghq.com/guides/dogstatsd/
	DataDogExtensions bool `toml:"datadog_extensions"`

	// Parses distribution metrics in the datadog statsd format.
	// Requires the DataDogExtension flag to be enabled.
	// https://docs.datadoghq.com/developers/metrics/types/?tab=distribution#definition
	DataDogDistributions bool `toml:"datadog_distributions"`

	// UDPPacketSize is deprecated, it's only here for legacy support
	// we now always create 1 max size buffer and then copy only what we need
	// into the in channel
	// see https://github.com/influxdata/telegraf/pull/992
	UDPPacketSize int `toml:"udp_packet_size" deprecated:"0.12.1;2.0.0;option is ignored"`

	ReadBufferSize int `toml:"read_buffer_size"`

	SanitizeNamesMethod string `toml:"sanitize_name_method"`

	sync.Mutex
	// Lock for preventing a data race during resource cleanup
	cleanup sync.Mutex
	wg      sync.WaitGroup
	// accept channel tracks how many active connections there are, if there
	// is an available bool in accept, then we are below the maximum and can
	// accept the connection
	accept chan bool
	// drops tracks the number of dropped metrics.
	drops int

	// Channel for all incoming statsd packets
	in   chan input
	done chan struct{}

	// Cache gauges, counters & sets so they can be aggregated as they arrive
	// gauges and counters map measurement/tags hash -> field name -> metrics
	// sets and timings map measurement/tags hash -> metrics
	// distributions aggregate measurement/tags and are published directly
	gauges        map[string]cachedgauge
	counters      map[string]cachedcounter
	sets          map[string]cachedset
	timings       map[string]cachedtimings
	distributions []cacheddistributions

	// bucket -> influx templates
	Templates []string

	// Protocol listeners
	UDPlistener *net.UDPConn
	TCPlistener *net.TCPListener

	// track current connections so we can close them in Stop()
	conns map[string]*net.TCPConn

	MaxTCPConnections int `toml:"max_tcp_connections"`

	TCPKeepAlive       bool           `toml:"tcp_keep_alive"`
	TCPKeepAlivePeriod *time.Duration `toml:"tcp_keep_alive_period"`

	// Max duration for each metric to stay cached without being updated.
	MaxTTL time.Duration `toml:"max_ttl"`

	graphiteParser *graphite.Parser

	acc telegraf.Accumulator

	Log *logrus.Entry

	// A pool of byte slices to handle parsing
	bufPool sync.Pool
}

type input struct {
	*bytes.Buffer
	time.Time
	Addr string
}

// One statsd metric, form is <bucket>:<value>|<mtype>|@<samplerate>
type metric struct {
	name       string
	field      string
	bucket     string
	hash       string
	intvalue   int64
	floatvalue float64
	strvalue   string
	mtype      string
	additive   bool
	samplerate float64
	tags       map[string]string
}

type cachedset struct {
	name      string
	fields    map[string]map[string]bool
	tags      map[string]string
	expiresAt time.Time
}

type cachedgauge struct {
	name      string
	fields    map[string]interface{}
	tags      map[string]string
	expiresAt time.Time
}

type cachedcounter struct {
	name      string
	fields    map[string]interface{}
	tags      map[string]string
	expiresAt time.Time
}

type cachedtimings struct {
	name      string
	fields    map[string]RunningStats
	tags      map[string]string
	expiresAt time.Time
}

type cacheddistributions struct {
	name  string
	value float64
	tags  map[string]string
}

func (*Statsd) SampleConfig() string {
	return sampleConfig
}

func (s *Statsd) Gather(acc telegraf.Accumulator) error {
	s.Lock()
	defer s.Unlock()
	now := time.Now()

	for _, m := range s.distributions {
		fields := map[string]interface{}{
			defaultFieldName: m.value,
		}
		acc.AddFields(m.name, fields, m.tags, now)
	}
	s.distributions = make([]cacheddistributions, 0)

	for _, m := range s.timings {
		// Defining a template to parse field names for timers allows us to split
		// out multiple fields per timer. In this case we prefix each stat with the
		// field name and store these all in a single measurement.
		fields := make(map[string]interface{})
		for fieldName, stats := range m.fields {
			var prefix string
			if fieldName != defaultFieldName {
				prefix = fieldName + "_"
			}
			fields[prefix+"mean"] = stats.Mean()
			fields[prefix+"stddev"] = stats.Stddev()
			fields[prefix+"sum"] = stats.Sum()
			fields[prefix+"upper"] = stats.Upper()
			fields[prefix+"lower"] = stats.Lower()
			fields[prefix+"count"] = stats.Count()
			for _, percentile := range s.Percentiles {
				name := fmt.Sprintf("%s%v_percentile", prefix, percentile)
				fields[name] = stats.Percentile(float64(percentile))
			}
		}

		acc.AddFields(m.name, fields, m.tags, now)
	}
	if s.DeleteTimings {
		s.timings = make(map[string]cachedtimings)
	}

	for _, m := range s.gauges {
		acc.AddGauge(m.name, m.fields, m.tags, now)
	}
	if s.DeleteGauges {
		s.gauges = make(map[string]cachedgauge)
	}

	for _, m := range s.counters {
		acc.AddCounter(m.name, m.fields, m.tags, now)
	}
	if s.DeleteCounters {
		s.counters = make(map[string]cachedcounter)
	}

	for _, m := range s.sets {
		fields := make(map[string]interface{})
		for field, set := range m.fields {
			fields[field] = int64(len(set))
		}
		acc.AddFields(m.name, fields, m.tags, now)
	}
	if s.DeleteSets {
		s.sets = make(map[string]cachedset)
	}

	s.expireCachedMetrics()

	return nil
}

func (s *Statsd) Start(ac telegraf.Accumulator) error {
	if s.ParseDataDogTags {
		s.DataDogExtensions = true
	}

	s.acc = ac

	// Make data structures
	s.gauges = make(map[string]cachedgauge)
	s.counters = make(map[string]cachedcounter)
	s.sets = make(map[string]cachedset)
	s.timings = make(map[string]cachedtimings)
	s.distributions = make([]cacheddistributions, 0)

	s.Lock()
	defer s.Unlock()

	s.in = make(chan input, s.AllowedPendingMessages)
	s.done = make(chan struct{})
	s.accept = make(chan bool, s.MaxTCPConnections)
	s.conns = make(map[string]*net.TCPConn)
	s.bufPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	for i := 0; i < s.MaxTCPConnections; i++ {
		s.accept <- true
	}

	if s.MetricSeparator == "" {
		s.MetricSeparator = defaultSeparator
	}

	if s.isUDP() {
		address, err := net.ResolveUDPAddr(s.Protocol, s.ServiceAddress)
		if err != nil {
			return err
		}

		conn, err := net.ListenUDP(s.Protocol, address)
		if err != nil {
			return err
		}

		s.Log.Debugf("UDP listening on %q", conn.LocalAddr().String())
		s.UDPlistener = conn

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.udpListen(conn); err != nil {
				ac.AddError(err)
			}
		}()
	} else {
		address, err := net.ResolveTCPAddr("tcp", s.ServiceAddress)
		if err != nil {
			return err
		}
		listener, err := net.ListenTCP("tcp", address)
		if err != nil {
			return err
		}

		s.Log.Debugf("TCP listening on %q", listener.Addr().String())
		s.TCPlistener = listener

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.tcpListen(listener); err != nil {
				ac.AddError(err)
			}
		}()
	}

	for i := 1; i <= parserGoRoutines; i++ {
		// Start the line parser
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.parser(); err != nil {
				ac.AddError(err)
			}
		}()
	}
	s.Log.Debugf("Started the statsd service on %q", s.ServiceAddress)
	return nil
}

// tcpListen() starts listening for TCP packets on the configured port.
func (s *Statsd) tcpListen(listener *net.TCPListener) error {
	for {
		select {
		case <-s.done:
			return nil
		default:
			// Accept connection:
			conn, err := listener.AcceptTCP()
			if err != nil {
				return err
			}
			if conn.RemoteAddr() == nil {
				s.Log.Errorf("RemoteAddr is nil")
			}
			authed, valid, poolExists := commonpool.VerifyClientIP(s.User, s.Password, s.Token, conn.RemoteAddr().(*net.TCPAddr).IP)
			if !poolExists {
				taosConn, err := commonpool.GetConnection(s.User, s.Password, s.Token, conn.RemoteAddr().(*net.TCPAddr).IP)
				if err != nil {
					s.Log.Errorf("GetConnection error: %v", err)
					_ = conn.Close()
					continue
				}
				err = taosConn.Put()
				if err != nil {
					s.Log.Errorf("PutConnection error: %v", err)
					_ = conn.Close()
					continue
				}
				authed = true
				valid = true
			}
			if !authed {
				s.Log.Errorf("wrong password")
				_ = conn.Close()
				continue
			}
			if !valid {
				s.Log.WithField("user", s.User).WithField("clientIP", conn.RemoteAddr().(*net.TCPAddr).IP.String()).Error("forbidden clientIP")
				_ = conn.Close()
				continue
			}
			if s.TCPKeepAlive {
				if err = conn.SetKeepAlive(true); err != nil {
					return err
				}

				if s.TCPKeepAlivePeriod != nil {
					if err = conn.SetKeepAlivePeriod(*s.TCPKeepAlivePeriod); err != nil {
						return err
					}
				}
			}

			select {
			case <-s.accept:
				// not over connection limit, handle the connection properly.
				s.wg.Add(1)
				// generate a random id for this TCPConn
				id := RandomString(6)
				s.remember(id, conn)
				go s.handler(conn, id)
			default:
				// We are over the connection limit, refuse & close.
				s.refuser(conn)
			}
		}
	}
}

// udpListen starts listening for UDP packets on the configured port.
func (s *Statsd) udpListen(conn *net.UDPConn) error {
	if s.ReadBufferSize > 0 {
		if err := s.UDPlistener.SetReadBuffer(s.ReadBufferSize); err != nil {
			return err
		}
	}

	buf := make([]byte, UDPMaxPacketSize)
	for {
		select {
		case <-s.done:
			return nil
		default:
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				if !strings.Contains(err.Error(), "closed network") {
					s.Log.Errorf("Error reading: %s", err.Error())
					continue
				}
				return err
			}
			if addr == nil {
				s.Log.Errorf("RemoteAddr is nil")
				continue
			}
			authed, valid, poolExists := commonpool.VerifyClientIP(s.User, s.Password, s.Token, addr.IP)
			if !poolExists {
				taosConn, err := commonpool.GetConnection(s.User, s.Password, s.Token, addr.IP)
				if err != nil {
					s.Log.Errorf("GetConnection error: %v", err)
					_ = conn.Close()
					continue
				}
				err = taosConn.Put()
				if err != nil {
					s.Log.Errorf("PutConnection error: %v", err)
					_ = conn.Close()
					continue
				}
				authed = true
				valid = true
			}
			if !authed {
				s.Log.Errorf("wrong password")
				_ = conn.Close()
				continue
			}
			if !valid {
				s.Log.WithField("user", s.User).WithField("clientIP", addr.IP.String()).Error("forbidden clientIP")
				_ = conn.Close()
				continue
			}

			b, ok := s.bufPool.Get().(*bytes.Buffer)
			if !ok {
				return fmt.Errorf("bufPool is not a bytes buffer")
			}
			b.Reset()
			if _, err := b.Write(buf[:n]); err != nil {
				return err
			}
			select {
			case s.in <- input{
				Buffer: b,
				Time:   time.Now(),
				Addr:   addr.IP.String()}:
			default:
				s.drops++
				if s.drops == 1 || s.AllowedPendingMessages == 0 || s.drops%s.AllowedPendingMessages == 0 {
					s.Log.Errorf("Statsd message queue full. "+
						"We have dropped %d messages so far. "+
						"You may want to increase statsd.allowPendingMessages in the config", s.drops)
				}
			}
		}
	}
}

// parser monitors the s.in channel, if there is a packet ready, it parses the
// packet into statsd strings and then calls parseStatsdLine, which parses a
// single statsd metric into a struct.
func (s *Statsd) parser() error {
	for {
		select {
		case <-s.done:
			return nil
		case in := <-s.in:
			lines := strings.Split(in.Buffer.String(), "\n")
			s.bufPool.Put(in.Buffer)
			for _, line := range lines {
				line = strings.TrimSpace(line)
				switch {
				case line == "":
				case s.DataDogExtensions && strings.HasPrefix(line, "_e"):
					if err := s.parseEventMessage(in.Time, line, in.Addr); err != nil {
						return err
					}
				default:
					if err := s.parseStatsdLine(line); err != nil {
						if errors.Cause(err) == errParsing {
							// parsing errors log when the error occurs
							continue
						}
						return err
					}
				}
			}
		}
	}
}

// parseStatsdLine will parse the given statsd line, validating it as it goes.
// If the line is valid, it will be cached for the next call to Gather()
func (s *Statsd) parseStatsdLine(line string) error {
	lineTags := make(map[string]string)
	if s.DataDogExtensions {
		recombinedSegments := make([]string, 0)
		// datadog tags look like this:
		// users.online:1|c|@0.5|#country:china,environment:production
		// users.online:1|c|#sometagwithnovalue
		// we will split on the pipe and remove any elements that are datadog
		// tags, parse them, and rebuild the line sans the datadog tags
		pipesplit := strings.Split(line, "|")
		for _, segment := range pipesplit {
			if len(segment) > 0 && segment[0] == '#' {
				// we have ourselves a tag; they are comma separated
				parseDataDogTags(lineTags, segment[1:])
			} else {
				recombinedSegments = append(recombinedSegments, segment)
			}
		}
		line = strings.Join(recombinedSegments, "|")
	}

	// Validate splitting the line on ":"
	bits := strings.Split(line, ":")
	if len(bits) < 2 {
		s.Log.Errorf("Splitting ':', unable to parse metric: %s", line)
		return errParsing
	}

	// Extract bucket name from individual metric bits
	bucketName, bits := bits[0], bits[1:]

	// Add a metric for each bit available
	for _, bit := range bits {
		m := metric{}

		m.bucket = bucketName

		// Validate splitting the bit on "|"
		pipesplit := strings.Split(bit, "|")
		if len(pipesplit) < 2 {
			s.Log.Errorf("Splitting '|', unable to parse metric: %s", line)
			return errParsing
		} else if len(pipesplit) > 2 {
			sr := pipesplit[2]

			if strings.Contains(sr, "@") && len(sr) > 1 {
				samplerate, err := strconv.ParseFloat(sr[1:], 64)
				if err != nil {
					s.Log.Errorf("Parsing sample rate: %s", err.Error())
				} else {
					// sample rate successfully parsed
					m.samplerate = samplerate
				}
			} else {
				s.Log.Debugf("Sample rate must be in format like: "+
					"@0.1, @0.5, etc. Ignoring sample rate for line: %s", line)
			}
		}

		// Validate metric type
		switch pipesplit[1] {
		case "g", "c", "s", "ms", "h", "d":
			m.mtype = pipesplit[1]
		default:
			s.Log.Errorf("Metric type %q unsupported", pipesplit[1])
			return errParsing
		}

		// Parse the value
		if strings.HasPrefix(pipesplit[0], "-") || strings.HasPrefix(pipesplit[0], "+") {
			if m.mtype != "g" && m.mtype != "c" {
				s.Log.Errorf("+- values are only supported for gauges & counters, unable to parse metric: %s", line)
				return errParsing
			}
			m.additive = true
		}

		switch m.mtype {
		case "g", "ms", "h", "d":
			v, err := strconv.ParseFloat(pipesplit[0], 64)
			if err != nil {
				s.Log.Errorf("Parsing value to float64, unable to parse metric: %s", line)
				return errParsing
			}
			m.floatvalue = v
		case "c":
			var v int64
			v, err := strconv.ParseInt(pipesplit[0], 10, 64)
			if err != nil {
				v2, err2 := strconv.ParseFloat(pipesplit[0], 64)
				if err2 != nil {
					s.Log.Errorf("Parsing value to int64, unable to parse metric: %s", line)
					return errParsing
				}
				v = int64(v2)
			}
			// If a sample rate is given with a counter, divide value by the rate
			if m.samplerate != 0 && m.mtype == "c" {
				v = int64(float64(v) / m.samplerate)
			}
			m.intvalue = v
		case "s":
			m.strvalue = pipesplit[0]
		}

		// Parse the name & tags from bucket
		m.name, m.field, m.tags = s.parseName(m.bucket)
		switch m.mtype {
		case "c":
			m.tags["metric_type"] = "counter"
		case "g":
			m.tags["metric_type"] = "gauge"
		case "s":
			m.tags["metric_type"] = "set"
		case "ms":
			m.tags["metric_type"] = "timing"
		case "h":
			m.tags["metric_type"] = "histogram"
		case "d":
			m.tags["metric_type"] = "distribution"
		}
		if len(lineTags) > 0 {
			for k, v := range lineTags {
				m.tags[k] = v
			}
		}

		// Make a unique key for the measurement name/tags
		var tg []string
		for k, v := range m.tags {
			tg = append(tg, k+"="+v)
		}
		sort.Strings(tg)
		tg = append(tg, m.name)
		m.hash = strings.Join(tg, "")

		s.aggregate(m)
	}

	return nil
}

// parseName parses the given bucket name with the list of bucket maps in the
// config file. If there is a match, it will parse the name of the metric and
// map of tags.
// Return values are (<name>, <field>, <tags>)
func (s *Statsd) parseName(bucket string) (name string, field string, tags map[string]string) {
	s.Lock()
	defer s.Unlock()
	tags = make(map[string]string)

	bucketparts := strings.Split(bucket, ",")
	// Parse out any tags in the bucket
	if len(bucketparts) > 1 {
		for _, btag := range bucketparts[1:] {
			k, v := parseKeyValue(btag)
			if k != "" {
				tags[k] = v
			}
		}
	}

	name = bucketparts[0]
	switch s.SanitizeNamesMethod {
	case "":
	case "upstream":
		whitespace := regexp.MustCompile(`\s+`)
		name = whitespace.ReplaceAllString(name, "_")
		name = strings.ReplaceAll(name, "/", "-")
		allowedChars := regexp.MustCompile(`[^a-zA-Z_\-0-9\.;=]`)
		name = allowedChars.ReplaceAllString(name, "")
	default:
		s.Log.Errorf("Unknown sanitizae name method: %s", s.SanitizeNamesMethod)
	}

	p := s.graphiteParser
	var err error

	if p == nil || s.graphiteParser.Separator != s.MetricSeparator {
		p = &graphite.Parser{Separator: s.MetricSeparator, Templates: s.Templates}
		err = p.Init()
		s.graphiteParser = p
	}

	if err == nil {
		p.DefaultTags = tags
		name, tags, field, _ = p.ApplyTemplate(name)
	}

	if s.ConvertNames {
		name = strings.ReplaceAll(name, ".", "_")
		name = strings.ReplaceAll(name, "-", "__")
	}
	if field == "" {
		field = defaultFieldName
	}

	return name, field, tags
}

// Parse the key,value out of a string that looks like "key=value"
func parseKeyValue(keyValue string) (key string, val string) {
	split := strings.Split(keyValue, "=")
	// Must be exactly 2 to get anything meaningful out of them
	if len(split) == 2 {
		key = split[0]
		val = split[1]
	} else if len(split) == 1 {
		val = split[0]
	} else if len(split) > 2 {
		// fix: https://github.com/influxdata/telegraf/issues/10113
		// fix: value has "=" parse error
		// uri=/service/endpoint?sampleParam={paramValue} parse value key="uri", val="/service/endpoint?sampleParam\={paramValue}"
		key = split[0]
		val = strings.Join(split[1:], "=")
	}

	return key, val
}

// aggregate takes in a metric. It then
// aggregates and caches the current value(s). It does not deal with the
// Delete* options, because those are dealt with in the Gather function.
func (s *Statsd) aggregate(m metric) {
	s.Lock()
	defer s.Unlock()

	switch m.mtype {
	case "d":
		if s.DataDogExtensions && s.DataDogDistributions {
			cached := cacheddistributions{
				name:  m.name,
				value: m.floatvalue,
				tags:  m.tags,
			}
			s.distributions = append(s.distributions, cached)
		}
	case "ms", "h":
		// Check if the measurement exists
		cached, ok := s.timings[m.hash]
		if !ok {
			cached = cachedtimings{
				name:   m.name,
				fields: make(map[string]RunningStats),
				tags:   m.tags,
			}
		}
		// Check if the field exists. If we've not enabled multiple fields per timer
		// this will be the default field name, eg. "value"
		field, ok := cached.fields[m.field]
		if !ok {
			field = RunningStats{
				PercLimit: s.PercentileLimit,
			}
		}
		if m.samplerate > 0 {
			for i := 0; i < int(1.0/m.samplerate); i++ {
				field.AddValue(m.floatvalue)
			}
		} else {
			field.AddValue(m.floatvalue)
		}
		cached.fields[m.field] = field
		cached.expiresAt = time.Now().Add(time.Duration(s.MaxTTL))
		s.timings[m.hash] = cached
	case "c":
		// check if the measurement exists
		cached, ok := s.counters[m.hash]
		if !ok {
			cached = cachedcounter{
				name:   m.name,
				fields: make(map[string]interface{}),
				tags:   m.tags,
			}
		}
		// check if the field exists
		_, ok = cached.fields[m.field]
		if !ok {
			cached.fields[m.field] = int64(0)
		}
		cached.fields[m.field] = cached.fields[m.field].(int64) + m.intvalue
		cached.expiresAt = time.Now().Add(time.Duration(s.MaxTTL))
		s.counters[m.hash] = cached
	case "g":
		// check if the measurement exists
		cached, ok := s.gauges[m.hash]
		if !ok {
			cached = cachedgauge{
				name:   m.name,
				fields: make(map[string]interface{}),
				tags:   m.tags,
			}
		}
		// check if the field exists
		_, ok = cached.fields[m.field]
		if !ok {
			cached.fields[m.field] = float64(0)
		}
		if m.additive {
			cached.fields[m.field] = cached.fields[m.field].(float64) + m.floatvalue
		} else {
			cached.fields[m.field] = m.floatvalue
		}

		cached.expiresAt = time.Now().Add(time.Duration(s.MaxTTL))
		s.gauges[m.hash] = cached
	case "s":
		// check if the measurement exists
		cached, ok := s.sets[m.hash]
		if !ok {
			cached = cachedset{
				name:   m.name,
				fields: make(map[string]map[string]bool),
				tags:   m.tags,
			}
		}
		// check if the field exists
		_, ok = cached.fields[m.field]
		if !ok {
			cached.fields[m.field] = make(map[string]bool)
		}
		cached.fields[m.field][m.strvalue] = true
		cached.expiresAt = time.Now().Add(time.Duration(s.MaxTTL))
		s.sets[m.hash] = cached
	}
}

// handler handles a single TCP Connection
func (s *Statsd) handler(conn *net.TCPConn, id string) {
	// connection cleanup function
	defer func() {
		s.wg.Done()

		// Ignore the returned error as we cannot do anything about it anyway
		//nolint:errcheck,revive
		conn.Close()

		// Add one connection potential back to channel when this one closes
		s.accept <- true
		s.forget(id)
	}()

	var remoteIP string
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		remoteIP = addr.IP.String()
	}
	ip := conn.RemoteAddr().(*net.TCPAddr).IP

	var n int
	scanner := bufio.NewScanner(conn)
	for {
		select {
		case <-s.done:
			return
		default:
			if !scanner.Scan() {
				return
			}
			n = len(scanner.Bytes())
			if n == 0 {
				continue
			}

			b := s.bufPool.Get().(*bytes.Buffer)
			b.Reset()
			// Writes to a bytes buffer always succeed, so do not check the errors here
			//nolint:errcheck,revive
			b.Write(scanner.Bytes())
			//nolint:errcheck,revive
			b.WriteByte('\n')
			taosConn, err := commonpool.GetConnection(s.User, s.Password, s.Token, ip)
			if err != nil {
				if errors.Is(err, commonpool.ErrWhitelistForbidden) {
					s.Log.Errorf("Whitelist forbidden for %s", ip)
					return
				}
				s.Log.Errorf("get connection error, err:%s", err)
				return
			}
			err = taosConn.Put()
			if err != nil {
				s.Log.Errorf("PutConnection error: %v", err)
				return
			}
			select {
			case s.in <- input{Buffer: b, Time: time.Now(), Addr: remoteIP}:
			default:
				s.drops++
				if s.drops == 1 || s.drops%s.AllowedPendingMessages == 0 {
					s.Log.Errorf("Statsd message queue full. "+
						"We have dropped %d messages so far. "+
						"You may want to increase allowed_pending_messages in the config", s.drops)
				}
			}
		}
	}
}

// refuser refuses a TCP connection
func (s *Statsd) refuser(conn *net.TCPConn) {
	// Ignore the returned error as we cannot do anything about it anyway
	//nolint:errcheck,revive
	conn.Close()
	s.Log.Debugf("Refused TCP Connection from %s", conn.RemoteAddr())
	s.Log.Warn("Maximum TCP Connections reached, you may want to adjust max_tcp_connections")
}

// forget a TCP connection
func (s *Statsd) forget(id string) {
	s.cleanup.Lock()
	defer s.cleanup.Unlock()
	delete(s.conns, id)
}

// remember a TCP connection
func (s *Statsd) remember(id string, conn *net.TCPConn) {
	s.cleanup.Lock()
	defer s.cleanup.Unlock()
	s.conns[id] = conn
}

func (s *Statsd) Stop() {
	s.Lock()
	s.Log.Debugf("Stopping the statsd service")
	close(s.done)
	if s.isUDP() {
		// Ignore the returned error as we cannot do anything about it anyway
		//nolint:errcheck,revive
		s.UDPlistener.Close()
	} else {
		// Ignore the returned error as we cannot do anything about it anyway
		//nolint:errcheck,revive
		s.TCPlistener.Close()
		// Close all open TCP connections
		//  - get all conns from the s.conns map and put into slice
		//  - this is so the forget() function doesnt conflict with looping
		//    over the s.conns map
		var conns []*net.TCPConn
		s.cleanup.Lock()
		for _, conn := range s.conns {
			conns = append(conns, conn)
		}
		s.cleanup.Unlock()
		for _, conn := range conns {
			// Ignore the returned error as we cannot do anything about it anyway
			//nolint:errcheck,revive
			conn.Close()
		}
	}
	s.Unlock()

	s.wg.Wait()

	s.Lock()
	close(s.in)
	s.Log.Debugf("Stopped listener service on %q", s.ServiceAddress)
	s.Unlock()
}

// IsUDP returns true if the protocol is UDP, false otherwise.
func (s *Statsd) isUDP() bool {
	return strings.HasPrefix(s.Protocol, "udp")
}

func (s *Statsd) expireCachedMetrics() {
	// If Max TTL wasn't configured, skip expiration.
	if s.MaxTTL == 0 {
		return
	}

	now := time.Now()

	for key, cached := range s.gauges {
		if now.After(cached.expiresAt) {
			delete(s.gauges, key)
		}
	}

	for key, cached := range s.sets {
		if now.After(cached.expiresAt) {
			delete(s.sets, key)
		}
	}

	for key, cached := range s.timings {
		if now.After(cached.expiresAt) {
			delete(s.timings, key)
		}
	}

	for key, cached := range s.counters {
		if now.After(cached.expiresAt) {
			delete(s.counters, key)
		}
	}
}

const alphanum string = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func RandomString(n int) string {
	var bs = make([]byte, n)
	//nolint:staticcheck
	rand.Read(bs)
	for i, b := range bs {
		bs[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bs)
}
