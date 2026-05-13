package opentsdbtelnet

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/joinerror"
)

var logger = log.GetLogger("PLG").WithField("mod", "telnet")
var versionCommand = "version"

type Plugin struct {
	conf         Config
	done         chan struct{}
	wg           sync.WaitGroup
	TCPListeners []*TCPListener
}

type TCPListener struct {
	plugin    *Plugin
	index     int
	listener  *net.TCPListener
	id        uint64
	connList  map[uint64]*Connection
	accept    chan bool
	keepalive bool
	cleanup   sync.Mutex
	done      chan struct{}
	wg        sync.WaitGroup
}

func NewTCPListener(plugin *Plugin, index int, listener *net.TCPListener, maxConnections int, keepalive bool) *TCPListener {
	l := &TCPListener{plugin: plugin, index: index, listener: listener, keepalive: keepalive}
	l.done = make(chan struct{})
	l.connList = make(map[uint64]*Connection)
	l.accept = make(chan bool, maxConnections)
	for i := 0; i < maxConnections; i++ {
		l.accept <- true
	}
	return l
}

func (l *TCPListener) start() error {
	for {
		select {
		case <-l.done:
			return nil
		default:
			// Accept connection:
			conn, err := l.listener.AcceptTCP()
			if err != nil {
				return err
			}
			if conn.RemoteAddr() == nil {
				logger.Errorf("RemoteAddr is nil")
			}
			_, valid, poolExists := commonpool.VerifyClientIP(l.plugin.conf.User, l.plugin.conf.Password, l.plugin.conf.Token, conn.RemoteAddr().(*net.TCPAddr).IP)
			if poolExists && !valid {
				logger.WithField("user", l.plugin.conf.User).WithField("clientIP", conn.RemoteAddr().(*net.TCPAddr).IP.String()).Error("forbidden clientIP")
				_ = conn.Close()
				continue
			}
			if l.keepalive {
				if err = conn.SetKeepAlive(true); err != nil {
					return err
				}
			}

			select {
			case <-l.accept:
				l.wg.Add(1)
				id := atomic.AddUint64(&l.id, 1)
				connection := &Connection{
					l:         l,
					conn:      conn,
					id:        id,
					db:        l.plugin.conf.DBList[l.index],
					clientIP:  conn.RemoteAddr().(*net.TCPAddr).IP,
					batchSize: l.plugin.conf.BatchSize,
					exit:      make(chan struct{}),
					once:      sync.Once{},
					closed:    false,
				}
				l.remember(id, connection)
				go connection.handle()
			default:
				l.refuser(conn)
			}
		}
	}
}

func (l *TCPListener) forget(id uint64) {
	l.cleanup.Lock()
	defer l.cleanup.Unlock()
	delete(l.connList, id)
}

func (l *TCPListener) remember(id uint64, conn *Connection) {
	l.cleanup.Lock()
	defer l.cleanup.Unlock()
	l.connList[id] = conn
}

func (l *TCPListener) refuser(conn *net.TCPConn) {
	_ = conn.Close()
	logger.Debugf("Refused TCP Connection from %s", conn.RemoteAddr())
	logger.Warn("Maximum TCP Connections reached")
}

type Connection struct {
	l         *TCPListener
	conn      *net.TCPConn
	id        uint64
	db        string
	clientIP  net.IP
	batchSize int
	exit      chan struct{}
	once      sync.Once
	closed    bool
}

func (c *Connection) handle() {
	defer func() {
		c.l.wg.Done()
		_ = c.conn.Close()
		c.l.accept <- true
		c.l.forget(c.id)
	}()
	ip := c.conn.RemoteAddr().(*net.TCPAddr).IP
	for {
		select {
		case <-c.l.done:
			return
		case <-c.exit:
			return
		default:
			b := bufio.NewReader(c.conn)
			cache := make([]string, 0, c.batchSize)
			dataChan := make(chan string, c.batchSize*2)
			flushInterval := c.l.plugin.conf.FlushInterval
			if flushInterval <= 0 {
				flushInterval = math.MaxInt64
			}
			ticker := time.NewTicker(flushInterval)
			go func() {
				for {
					select {
					case <-c.exit:
						ticker.Stop()
						unConsumedLen := len(dataChan)
						for i := 0; i < unConsumedLen; i++ {
							data := <-dataChan
							cache = append(cache, data)
							if len(cache) >= c.l.plugin.conf.BatchSize {
								c.l.plugin.handleData(c, cache, ip)
								cache = cache[:0]
							}
						}
						if len(cache) > 0 {
							c.l.plugin.handleData(c, cache, ip)
							cache = cache[:0]
						}
						return
					case <-c.l.done:
						ticker.Stop()
						return
					case <-ticker.C:
						if len(cache) > 0 {
							c.l.plugin.handleData(c, cache, ip)
							cache = cache[:0]
						}
					case data := <-dataChan:
						cache = append(cache, data)
						if len(cache) >= c.batchSize {
							c.l.plugin.handleData(c, cache, ip)
							cache = cache[:0]
						}
					}
				}
			}()
			for {
				if c.closed {
					return
				}
				s, err := b.ReadString('\n')
				if err != nil {
					if err != io.EOF {
						logger.WithError(err).Error("conn read")
					}
					c.close()
					return
				}
				if monitor.AllPaused() {
					continue
				}
				if len(s) == 0 {
					continue
				}
				s = s[:len(s)-1]
				if s == versionCommand {
					_, err = c.conn.Write([]byte{'1'})
					if err != nil {
						logger.WithError(err).Error("conn write")
						c.close()
						return
					}
					continue
				}
				dataChan <- s
			}
		}
	}
}

func (c *Connection) close() {
	c.once.Do(func() {
		close(c.exit)
		c.closed = true
	})
}

func (l *TCPListener) stop() error {
	close(l.done)
	var tcpConnList []*Connection
	l.cleanup.Lock()
	for _, conn := range l.connList {
		tcpConnList = append(tcpConnList, conn)
	}
	l.cleanup.Unlock()
	for _, conn := range tcpConnList {
		conn.close()
	}
	l.wg.Wait()
	return l.listener.Close()
}

func (p *Plugin) Init(_ gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Debug("opentsdb_telnet disabled")
		return nil
	}
	if len(p.conf.PortList) == 0 {
		return errors.New("no port set")
	}
	if len(p.conf.PortList) != len(p.conf.DBList) {
		return errors.New("the number of dbs is not equal ports")
	}
	return nil
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}
	p.TCPListeners = make([]*TCPListener, len(p.conf.PortList))
	p.done = make(chan struct{})
	for i := 0; i < len(p.conf.PortList); i++ {
		err := p.tcp(p.conf.PortList[i], i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Plugin) Stop() error {
	if !p.conf.Enable {
		return nil
	}
	if p.done != nil {
		close(p.done)
	}
	var errs []error
	for _, listener := range p.TCPListeners {
		err := listener.stop()
		if err != nil {
			errs = append(errs, err)
		}
	}
	p.wg.Wait()
	if len(errs) > 0 {
		return joinerror.Join(errs...)
	}
	return nil
}

func (p *Plugin) String() string {
	return "opentsdb_telnet"
}

func (p *Plugin) Version() string {
	return "v1"
}

func (p *Plugin) tcp(port int, index int) error {
	address, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		return err
	}

	logger.Debugf("TCP listening on %q", listener.Addr().String())
	tcpListener := NewTCPListener(p, index, listener, p.conf.MaxTCPConnections, p.conf.TCPKeepAlive)
	p.TCPListeners[index] = tcpListener
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := tcpListener.start(); err != nil {
			select {
			case <-tcpListener.done:
				return
			default:
				logger.WithError(err).Panic()
			}
		}
	}()
	return nil
}

func (p *Plugin) handleData(connection *Connection, line []string, clientIP net.IP) {
	taosConn, err := commonpool.GetConnection(p.conf.User, p.conf.Password, p.conf.Token, clientIP)
	if err != nil {
		logger.WithError(err).Error("connect server error")
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			connection.close()
		}
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("connect pool put error")
		}
	}()
	isDebug := log.IsDebug()
	var start = log.GetLogNow(isDebug)
	reqID := generator.GetReqID()
	logger := logger.WithField(config.ReqIDKey, reqID)
	logger.Debugf("insert telnet payload, lines:%s", line)
	err = inserter.InsertOpentsdbTelnetBatch(taosConn.TaosConnection, line, connection.db, p.conf.TTL, uint64(reqID), "", logger)
	if err != nil {
		logger.WithError(err).Errorln("insert telnet payload error :", line)
	}
	logger.Debug("insert telnet payload cost:", log.GetLogDuration(isDebug, start))
}

func init() {
	plugin.Register(&Plugin{})
}
