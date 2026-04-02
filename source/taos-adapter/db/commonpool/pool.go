package commonpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/monitor/metrics"
	"github.com/taosdata/taosadapter/v3/tools/connectpool"
	"golang.org/x/sync/singleflight"
)

type AuthInfo struct {
	User     string
	Password string
	Token    string
}

type ConnectorPool struct {
	changePassChan        chan int32
	whitelistChan         chan int64
	dropUserChan          chan struct{}
	user                  string
	password              string
	token                 string
	pool                  *connectpool.ConnectPool
	logger                *logrus.Entry
	once                  sync.Once
	ctx                   context.Context
	cancel                context.CancelFunc
	ipNetsLock            sync.RWMutex
	allowNets             []*net.IPNet
	blockNets             []*net.IPNet
	changePassHandle      cgo.Handle
	whitelistChangeHandle cgo.Handle
	dropUserHandle        cgo.Handle
	gauge                 *metrics.Gauge
}

func NewConnectorPool(user, password, token string) (*ConnectorPool, error) {
	changePassChan, changePassHandle := tool.GetRegisterChangePassHandle()
	whitelistChangeChan, whitelistChangeHandle := tool.GetRegisterChangeWhiteListHandle()
	dropUserChan, dropUserHandle := tool.GetRegisterDropUserHandle()
	var logger *logrus.Entry
	if token != "" {
		logger = log.GetLogger("CNP").WithField("conn_type", "token")
	} else {
		logger = log.GetLogger("CNP").WithFields(logrus.Fields{"conn_type": "password", "user": user})
	}
	cp := &ConnectorPool{
		user:                  user,
		password:              password,
		token:                 token,
		changePassChan:        changePassChan,
		changePassHandle:      changePassHandle,
		whitelistChan:         whitelistChangeChan,
		whitelistChangeHandle: whitelistChangeHandle,
		dropUserChan:          dropUserChan,
		dropUserHandle:        dropUserHandle,
		logger:                logger,
	}
	maxConnect := config.Conf.Pool.MaxConnect
	if maxConnect == 0 {
		maxConnect = runtime.GOMAXPROCS(0) * 2
	}
	maxWait := config.Conf.Pool.MaxWait
	if maxWait < 0 {
		maxWait = 0
	}
	waitTimeout := config.Conf.Pool.WaitTimeout
	if waitTimeout < 0 {
		waitTimeout = config.DefaultWaitTimeout
	}
	poolConfig := &connectpool.Config{
		InitialCap:  1,
		MaxWait:     maxWait,
		WaitTimeout: time.Second * time.Duration(waitTimeout),
		MaxCap:      maxConnect,
		Factory:     cp.factory,
		Close:       cp.close,
	}
	p, err := connectpool.NewConnectPool(poolConfig)

	if err != nil {
		// failed to create connection pool, maybe the user is not exist or password is wrong
		// put the handles back to pool
		cp.putHandle()
		return nil, err
	}
	isDebug := log.IsDebug()
	cp.pool = p
	v, _ := p.Get()
	cleanWhenError := func() {
		_ = p.Put(v)
		p.Release()
		cp.putHandle()
	}
	// get user info from connection
	if token != "" {
		userName, code := syncinterface.TaosGetConnectionUserName(v, logger, isDebug)
		if code != 0 {
			logger.Errorf("get connection user name failed, code: %d", code)
			cleanWhenError()
			return nil, err
		}
		cp.user = userName
		cp.logger = cp.logger.WithField("user", userName)
	}
	// notify modify
	cp.ctx, cp.cancel = context.WithCancel(context.Background())
	err = tool.RegisterChangePass(v, cp.changePassHandle, cp.logger, isDebug)
	if err != nil {
		logger.Errorf("register change pass failed, %s", err)
		cleanWhenError()
		return nil, err
	}
	// notify drop
	err = tool.RegisterDropUser(v, cp.dropUserHandle, cp.logger, isDebug)
	if err != nil {
		logger.Errorf("register drop user failed, %s", err)
		cleanWhenError()
		return nil, err
	}
	// whitelist
	allowlist, blocklist, err := tool.GetWhitelist(v, cp.logger, isDebug)
	if err != nil {
		logger.Errorf("get whitelist failed, %s", err)
		cleanWhenError()
		return nil, err
	}
	cp.allowNets = allowlist
	cp.blockNets = blocklist
	// register whitelist modify callback
	err = tool.RegisterChangeWhitelist(v, cp.whitelistChangeHandle, cp.logger, isDebug)
	if err != nil {
		logger.Errorf("register change whitelist failed, %s", err)
		cleanWhenError()
		return nil, err
	}
	_ = p.Put(v)
	go func() {
		defer func() {
			cp.logger.Warn("connector pool exit")
			cp.putHandle()
		}()
		for {
			select {
			case <-cp.changePassChan:
				// password changed
				cp.logger.Trace("password changed")
				cp.Release()
				return
			case <-cp.dropUserChan:
				// user dropped
				cp.logger.Trace("user dropped")
				cp.Release()
				return
			case <-cp.whitelistChan:
				// whitelist changed
				cp.logger.Trace("whitelist change")
				allowlist, blocklist, err = tool.GetWhitelist(v, cp.logger, log.IsDebug())
				if err != nil {
					// fetch whitelist error, keep the old one
					cp.logger.WithError(err).Error("fetch whitelist error!")
				} else {
					cp.ipNetsLock.Lock()
					cp.allowNets = allowlist
					cp.blockNets = blocklist

					cp.logger.Debugf("whitelist change, allowlist:%s, blocklist:%s", tool.IpNetSliceToString(cp.allowNets), tool.IpNetSliceToString(cp.blockNets))
					cp.ipNetsLock.Unlock()
				}
			case <-cp.ctx.Done():
				return
			}
		}
	}()
	gauge := monitor.RecordNewConnectionPool(user)
	cp.gauge = gauge
	return cp, nil
}

func (cp *ConnectorPool) factory() (unsafe.Pointer, error) {
	var conn unsafe.Pointer
	var err error
	if cp.token != "" {
		conn, err = syncinterface.TaosConnectToken("", cp.token, "", 0, cp.logger, log.IsDebug())
	} else {
		conn, err = syncinterface.TaosConnect("", cp.user, cp.password, "", 0, cp.logger, log.IsDebug())
	}
	if err != nil {
		cp.logger.Errorf("connect to taos failed: %s", err.Error())
	}
	return conn, err
}

func (cp *ConnectorPool) close(v unsafe.Pointer) {
	if v != nil {
		syncinterface.TaosClose(v, cp.logger, log.IsDebug())
	}
}

var AuthFailureError = tErrors.NewError(httperror.TSDB_CODE_MND_AUTH_FAILURE, "Authentication failure")

func (cp *ConnectorPool) Get() (unsafe.Pointer, error) {
	v, err := cp.pool.Get()
	if err != nil {
		if err == connectpool.ErrClosed {
			cp.logger.Warn("connect poll closed return Authentication failure")
			return nil, AuthFailureError
		}
		return nil, err
	}
	if cp.gauge != nil {
		cp.gauge.Inc()
	}
	return v, nil
}

func (cp *ConnectorPool) Put(c unsafe.Pointer) error {
	isDebug := log.IsDebug()
	syncinterface.TaosResetCurrentDB(c, cp.logger, isDebug)
	syncinterface.TaosOptionsConnection(c, common.TSDB_OPTION_CONNECTION_CLEAR, nil, cp.logger, isDebug)
	s := log.GetLogNow(isDebug)
	if err := cp.pool.Put(c); err != nil {
		return err
	}
	cp.logger.Debugf("put connection back to pool, cost:%s", log.GetLogDuration(isDebug, s))
	if cp.gauge != nil {
		cp.gauge.Dec()
	}
	return nil
}

func (cp *ConnectorPool) Close(c unsafe.Pointer) error {
	return cp.pool.Close(c)
}

func (cp *ConnectorPool) verifyAuth(password string, token string) bool {
	if cp.token != "" {
		return token == cp.token
	}
	return password == cp.password
}

func (cp *ConnectorPool) verifyIP(ip net.IP) bool {
	cp.ipNetsLock.RLock()
	defer cp.ipNetsLock.RUnlock()
	return tool.CheckWhitelist(cp.allowNets, cp.blockNets, ip)
}

func (cp *ConnectorPool) Release() {
	cp.once.Do(func() {
		cp.cancel()
		authKey := cp.user
		if cp.token != "" {
			authKey = cp.token
		}
		connectionMap.CompareAndDelete(authKey, cp)
		cp.pool.Release()
		cp.logger.Warn("connector released")
	})
}

func (cp *ConnectorPool) putHandle() {
	tool.PutRegisterChangePassHandle(cp.changePassHandle)
	tool.PutRegisterChangeWhiteListHandle(cp.whitelistChangeHandle)
	tool.PutRegisterDropUserHandle(cp.dropUserHandle)
}

var connectionMap = sync.Map{}

type Conn struct {
	TaosConnection unsafe.Pointer
	pool           *ConnectorPool
}

func (c *Conn) Put() error {
	return c.pool.Put(c.TaosConnection)
}

var singleGroup singleflight.Group
var ErrWhitelistForbidden = errors.New("whitelist prohibits current IP access")

func GetConnection(user, password, token string, clientIp net.IP) (*Conn, error) {
	cp, err := getConnectionPool(user, password, token)
	if err != nil {
		return nil, err
	}
	return getConnectDirect(cp, clientIp)
}

func getConnectionPool(user, password, token string) (*ConnectorPool, error) {
	authKey := user
	singleflightKey := fmt.Sprintf("%s:%s", user, password)
	if token != "" {
		authKey = token
		singleflightKey = token
	}
	p, exist := connectionMap.Load(authKey)
	if exist {
		connectionPool := p.(*ConnectorPool)
		if connectionPool.verifyAuth(password, token) {
			return connectionPool, nil
		}
		cp, err, _ := singleGroup.Do(singleflightKey, func() (interface{}, error) {
			return getConnectorPoolSafe(user, password, token)
		})
		if err != nil {
			return nil, err
		}
		return cp.(*ConnectorPool), nil
	}
	cp, err, _ := singleGroup.Do(singleflightKey, func() (interface{}, error) {
		return getConnectorPoolSafe(user, password, token)
	})
	if err != nil {
		return nil, err
	}
	return cp.(*ConnectorPool), nil
}

func VerifyClientIP(user, password, token string, clientIP net.IP) (authed bool, valid bool, connectionPoolExits bool) {
	authKey := user
	if token != "" {
		authKey = token
	}
	p, exist := connectionMap.Load(authKey)
	if !exist {
		return
	}
	connectionPoolExits = true
	if !p.(*ConnectorPool).verifyAuth(password, token) {
		return
	}
	authed = true
	if !p.(*ConnectorPool).verifyIP(clientIP) {
		return
	}
	valid = true
	return
}

func getConnectDirect(connectionPool *ConnectorPool, clientIP net.IP) (*Conn, error) {
	if !connectionPool.verifyIP(clientIP) {
		return nil, ErrWhitelistForbidden
	}
	c, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	ipStr := clientIP.String()
	// ignore error, because we have checked the ip
	syncinterface.TaosOptionsConnection(c, common.TSDB_OPTION_CONNECTION_USER_IP, &ipStr, connectionPool.logger, log.IsDebug())
	return &Conn{
		TaosConnection: c,
		pool:           connectionPool,
	}, nil
}

type keyLock struct {
	mu   sync.Mutex
	refs int32
}

var poolLocks sync.Map // map[string]*keyLock

// acquireLock returns the lock for the given key and increments its reference count.
func acquireLock(key string) *keyLock {
	v, _ := poolLocks.LoadOrStore(key, &keyLock{})
	kl := v.(*keyLock)
	atomic.AddInt32(&kl.refs, 1)
	return kl
}

// releaseLock decrements the reference count for the given keyLock and removes it from the map if the count reaches zero.
func releaseLock(key string, kl *keyLock) {
	if atomic.AddInt32(&kl.refs, -1) == 0 {
		poolLocks.CompareAndDelete(key, kl)
	}
}

func getConnectorPoolSafe(user, password, token string) (*ConnectorPool, error) {
	authKey := user
	if token != "" {
		authKey = token
	}

	kl := acquireLock(authKey)
	kl.mu.Lock()
	defer func() {
		kl.mu.Unlock()
		releaseLock(authKey, kl)
	}()

	p, exist := connectionMap.Load(authKey)
	if exist {
		connectionPool := p.(*ConnectorPool)
		if connectionPool.verifyAuth(password, token) {
			return connectionPool, nil
		}
		newPool, err := NewConnectorPool(user, password, token)
		if err != nil {
			return nil, err
		}
		connectionPool.Release()
		connectionMap.Store(authKey, newPool)
		return newPool, nil
	}
	newPool, err := NewConnectorPool(user, password, token)
	if err != nil {
		return nil, err
	}
	connectionMap.Store(authKey, newPool)
	return newPool, nil
}
