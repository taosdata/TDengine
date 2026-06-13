package sqlparser

import (
	"fmt"
	"net"
	"sqlparser/tool"
	"strconv"
	"strings"
	"time"
)

type IpRange struct {
	IPNet *net.IPNet
	Neg   int8
}

func NewIpRange(bs []byte, neg int8) (*IpRange, error) {
	ip, ipnet, err := net.ParseCIDR(string(bs))
	if err != nil {
		return nil, err
	}
	ipnet.IP = ip
	return &IpRange{
		IPNet: ipnet,
		Neg:   neg,
	}, nil
}

func AppendIpRange(lexer yyLexer, ipRanges []*IpRange, ipRange Token) []*IpRange {
	ipRangeObj, err := NewIpRange(ipRange.Bytes, 0)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid IP range: %s, err:%s", ipRange.Bytes, err))
		return ipRanges
	}
	return append(ipRanges, ipRangeObj)
}

type DateTimeRange struct {
	Year     int16
	Month    int8
	Day      int8
	Hour     int8
	Minute   int8
	Neg      int8
	Duration int32
}

func parseDateTimeRange(bs []byte) (*DateTimeRange, error) {
	s := strings.TrimSpace(string(bs))
	if s == "" {
		return nil, fmt.Errorf("empty datetime range")
	}
	neg := int8(0)
	if strings.HasPrefix(s, "-") {
		neg = 1
		s = strings.TrimSpace(strings.TrimPrefix(s, "-"))
	}

	if tool.HasDurationUnit([]byte(s)) {
		d, err := tool.ParseDuration([]byte(s))
		if err != nil {
			return nil, err
		}
		seconds := int64(d.FixedDuration / time.Second)
		if d.Month > 0 {
			seconds += d.Month * 30 * 24 * 3600
		}
		return &DateTimeRange{Neg: neg, Duration: int32(seconds)}, nil
	}

	if days, err := strconv.ParseInt(s, 10, 32); err == nil {
		return &DateTimeRange{Neg: neg, Duration: int32(days) * 24 * 3600}, nil
	}

	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if tm, err := time.Parse(layout, s); err == nil {
			return &DateTimeRange{
				Neg:    neg,
				Year:   int16(tm.Year()),
				Month:  int8(tm.Month()),
				Day:    int8(tm.Day()),
				Hour:   int8(tm.Hour()),
				Minute: int8(tm.Minute()),
			}, nil
		}
	}

	return nil, fmt.Errorf("invalid datetime range: %s", s)
}
func AppendDateTimeRange(lexer yyLexer, timeRanges []*DateTimeRange, timeRange Token) []*DateTimeRange {
	dateTimeRange, err := parseDateTimeRange(timeRange.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid TIME range: %s, err:%s", timeRange.Bytes, err))
		return nil
	}
	return append(timeRanges, dateTimeRange)
}

type UserOptions struct {
	HasPassword            bool
	HasTotpseed            bool
	HasEnable              bool
	HasSysinfo             bool
	HasIsImport            bool
	HasCreatedb            bool
	HasChangepass          bool
	HasSessionPerUser      bool
	HasConnectTime         bool
	HasConnectIdleTime     bool
	HasCallPerSession      bool
	HasVnodePerCall        bool
	HasFailedLoginAttempts bool
	HasPasswordLifeTime    bool
	HasPasswordReuseTime   bool
	HasPasswordReuseMax    bool
	HasPasswordLockTime    bool
	HasPasswordGraceTime   bool
	HasInactiveAccountTime bool
	HasAllowTokenNum       bool

	Password            string
	Totpseed            string
	Enable              int8
	Sysinfo             int8
	IsImport            int8
	Createdb            int8
	Changepass          int8
	SessionPerUser      int32
	ConnectTime         int32
	ConnectIdleTime     int32
	CallPerSession      int32
	VnodePerCall        int32
	FailedLoginAttempts int32
	PasswordLifeTime    int32
	PasswordReuseTime   int32
	PasswordReuseMax    int32
	PasswordLockTime    int32
	PasswordGraceTime   int32
	InactiveAccountTime int32
	AllowTokenNum       int32

	IpRanges       []*IpRange
	DropIpRanges   []*IpRange // only for alter user
	TimeRanges     []*DateTimeRange
	DropTimeRanges []*DateTimeRange // only for alter user
}

func CreateDefaultUserOptions() *UserOptions {
	options := &UserOptions{}
	options.Enable = 1
	options.Sysinfo = 1
	options.Createdb = 0
	options.IsImport = 0
	options.Changepass = 2
	options.SessionPerUser = TSDB_USER_SESSION_PER_USER_DEFAULT
	options.ConnectTime = TSDB_USER_CONNECT_TIME_DEFAULT
	options.ConnectIdleTime = TSDB_USER_CONNECT_IDLE_TIME_DEFAULT
	options.CallPerSession = TSDB_USER_CALL_PER_SESSION_DEFAULT
	options.VnodePerCall = TSDB_USER_VNODE_PER_CALL_DEFAULT
	options.FailedLoginAttempts = TSDB_USER_FAILED_LOGIN_ATTEMPTS_DEFAULT
	options.PasswordLifeTime = TSDB_USER_PASSWORD_LIFE_TIME_DEFAULT
	options.PasswordReuseTime = TSDB_USER_PASSWORD_REUSE_TIME_DEFAULT
	options.PasswordReuseMax = TSDB_USER_PASSWORD_REUSE_MAX_DEFAULT
	options.PasswordLockTime = TSDB_USER_PASSWORD_LOCK_TIME_DEFAULT
	options.PasswordGraceTime = TSDB_USER_PASSWORD_GRACE_TIME_DEFAULT
	options.InactiveAccountTime = TSDB_USER_INACTIVE_ACCOUNT_TIME_DEFAULT
	options.AllowTokenNum = TSDB_USER_ALLOW_TOKEN_NUM_DEFAULT
	return options
}
func MergeUserOptions(yylex yyLexer, a, b *UserOptions) *UserOptions {
	if a == nil && b == nil {
		return CreateDefaultUserOptions()
	}
	if a == nil {
		return cloneUserOptions(b)
	}
	if b == nil {
		return cloneUserOptions(a)
	}

	out := cloneUserOptions(a)

	if b.HasPassword {
		out.HasPassword = true
		out.Password = b.Password
	}
	if b.HasTotpseed {
		out.HasTotpseed = true
		out.Totpseed = b.Totpseed
	}
	if b.HasEnable {
		out.HasEnable = true
		out.Enable = b.Enable
	}
	if b.HasSysinfo {
		out.HasSysinfo = true
		out.Sysinfo = b.Sysinfo
	}
	if b.HasIsImport {
		out.HasIsImport = true
		out.IsImport = b.IsImport
	}
	if b.HasCreatedb {
		out.HasCreatedb = true
		out.Createdb = b.Createdb
	}
	if b.HasChangepass {
		out.HasChangepass = true
		out.Changepass = b.Changepass
	}
	if b.HasSessionPerUser {
		out.HasSessionPerUser = true
		out.SessionPerUser = b.SessionPerUser
	}
	if b.HasConnectTime {
		out.HasConnectTime = true
		out.ConnectTime = b.ConnectTime
	}
	if b.HasConnectIdleTime {
		out.HasConnectIdleTime = true
		out.ConnectIdleTime = b.ConnectIdleTime
	}
	if b.HasCallPerSession {
		out.HasCallPerSession = true
		out.CallPerSession = b.CallPerSession
	}
	if b.HasVnodePerCall {
		out.HasVnodePerCall = true
		out.VnodePerCall = b.VnodePerCall
	}
	if b.HasFailedLoginAttempts {
		out.HasFailedLoginAttempts = true
		out.FailedLoginAttempts = b.FailedLoginAttempts
	}
	if b.HasPasswordLifeTime {
		out.HasPasswordLifeTime = true
		out.PasswordLifeTime = b.PasswordLifeTime
	}
	if b.HasPasswordReuseTime {
		out.HasPasswordReuseTime = true
		out.PasswordReuseTime = b.PasswordReuseTime
	}
	if b.HasPasswordReuseMax {
		out.HasPasswordReuseMax = true
		out.PasswordReuseMax = b.PasswordReuseMax
	}
	if b.HasPasswordLockTime {
		out.HasPasswordLockTime = true
		out.PasswordLockTime = b.PasswordLockTime
	}
	if b.HasPasswordGraceTime {
		out.HasPasswordGraceTime = true
		out.PasswordGraceTime = b.PasswordGraceTime
	}
	if b.HasInactiveAccountTime {
		out.HasInactiveAccountTime = true
		out.InactiveAccountTime = b.InactiveAccountTime
	}
	if b.HasAllowTokenNum {
		out.HasAllowTokenNum = true
		out.AllowTokenNum = b.AllowTokenNum
	}
	if b.IpRanges != nil {
		out.IpRanges = append([]*IpRange(nil), b.IpRanges...)
	}
	if b.DropIpRanges != nil {
		out.DropIpRanges = append([]*IpRange(nil), b.DropIpRanges...)
	}
	if b.TimeRanges != nil {
		out.TimeRanges = append([]*DateTimeRange(nil), b.TimeRanges...)
	}
	if b.DropTimeRanges != nil {
		out.DropTimeRanges = append([]*DateTimeRange(nil), b.DropTimeRanges...)
	}

	return out
}

func cloneUserOptions(src *UserOptions) *UserOptions {
	if src == nil {
		return nil
	}
	out := *src
	if src.IpRanges != nil {
		out.IpRanges = append([]*IpRange(nil), src.IpRanges...)
	}
	if src.DropIpRanges != nil {
		out.DropIpRanges = append([]*IpRange(nil), src.DropIpRanges...)
	}
	if src.TimeRanges != nil {
		out.TimeRanges = append([]*DateTimeRange(nil), src.TimeRanges...)
	}
	if src.DropTimeRanges != nil {
		out.DropTimeRanges = append([]*DateTimeRange(nil), src.DropTimeRanges...)
	}
	return &out
}

type CreateUserStmt struct {
	UserName string
	Password string
	Totpseed string
	// Preserve host/datetime option negation for stable formatting.
	IPRangeOptions       []*IpRange
	DateTimeRangeOptions []*DateTimeRange

	IgnoreExists bool
	Sysinfo      int8
	CreateDb     int8
	IsImport     int8
	Changepass   int8
	Enable       int8

	SessionPerUser      int32
	ConnectTime         int32
	ConnectIdleTime     int32
	CallPerSession      int32
	VnodePerCall        int32
	FailedLoginAttempts int32
	PasswordLifeTime    int32
	PasswordReuseTime   int32
	PasswordReuseMax    int32
	PasswordLockTime    int32
	PasswordGraceTime   int32
	InactiveAccountTime int32
	AllowTokenNum       int32

	NumIpRanges int32
	IpRanges    []string

	NumTimeRanges int32
	TimeRanges    []string
}

func (c *CreateUserStmt) iStatement() {}

func formatUserOptionValue(v int32, unitDiv int32, allowUnlimited bool) string {
	if allowUnlimited && v == -1 {
		return "unlimited"
	}
	if unitDiv <= 1 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatInt(int64(v/unitDiv), 10)
}

func formatDateTimeRangeLiteral(tr *DateTimeRange) string {
	return formatDateTimeRangeLiteralWithSign(tr, true)
}

func formatDateTimeRangeLiteralWithSign(tr *DateTimeRange, withSign bool) string {
	if tr == nil {
		return ""
	}
	if tr.Duration != 0 {
		prefix := ""
		if withSign && tr.Neg != 0 {
			prefix = "-"
		}
		return prefix + strconv.FormatInt(int64(tr.Duration), 10) + "s"
	}
	prefix := ""
	if withSign && tr.Neg != 0 {
		prefix = "-"
	}
	if tr.Hour != 0 || tr.Minute != 0 {
		return fmt.Sprintf("%s%04d-%02d-%02d %02d:%02d", prefix, tr.Year, tr.Month, tr.Day, tr.Hour, tr.Minute)
	}
	return fmt.Sprintf("%s%04d-%02d-%02d", prefix, tr.Year, tr.Month, tr.Day)
}

func formatIPRangeList(buf *TrackedBuffer, ranges []*IpRange) {
	for i, r := range ranges {
		if i > 0 {
			buf.Myprintf(", ")
		}
		if r == nil || r.IPNet == nil {
			buf.Myprintf("''")
			continue
		}
		buf.Myprintf("'%s'", r.IPNet.String())
	}
}

func formatDateTimeRangeList(buf *TrackedBuffer, ranges []*DateTimeRange) {
	formatDateTimeRangeListWithSign(buf, ranges, true)
}

func formatDateTimeRangeListWithSign(buf *TrackedBuffer, ranges []*DateTimeRange, withSign bool) {
	for i, tr := range ranges {
		if i > 0 {
			buf.Myprintf(", ")
		}
		buf.Myprintf("'%s'", formatDateTimeRangeLiteralWithSign(tr, withSign))
	}
}

func (c *CreateUserStmt) Format(buf *TrackedBuffer) {
	if c == nil {
		return
	}
	buf.Myprintf("create user ")
	if c.IgnoreExists {
		buf.Myprintf("if not exists ")
	}
	buf.Myprintf("%s", c.UserName)
	if c.Password != "" {
		buf.Myprintf(" pass '%s'", c.Password)
	}
	def := CreateDefaultUserOptions()
	if c.Enable != def.Enable {
		buf.Myprintf(" enable %s", strconv.FormatInt(int64(c.Enable), 10))
	}
	if c.AllowTokenNum != def.AllowTokenNum {
		buf.Myprintf(" allow_token_num %s", formatUserOptionValue(c.AllowTokenNum, 1, true))
	}
	if c.CreateDb != def.Createdb {
		buf.Myprintf(" createdb %s", strconv.FormatInt(int64(c.CreateDb), 10))
	}
	if c.Changepass != def.Changepass {
		buf.Myprintf(" changepass %s", strconv.FormatInt(int64(c.Changepass), 10))
	}
	if c.Sysinfo != def.Sysinfo {
		buf.Myprintf(" sysinfo %s", strconv.FormatInt(int64(c.Sysinfo), 10))
	}
	if c.IsImport != def.IsImport {
		buf.Myprintf(" is_import %s", strconv.FormatInt(int64(c.IsImport), 10))
	}
	if c.SessionPerUser != def.SessionPerUser {
		buf.Myprintf(" session_per_user %s", formatUserOptionValue(c.SessionPerUser, 1, true))
	}
	if c.ConnectTime != def.ConnectTime {
		buf.Myprintf(" connect_time %s", formatUserOptionValue(c.ConnectTime, 60, true))
	}
	if c.ConnectIdleTime != def.ConnectIdleTime {
		buf.Myprintf(" connect_idle_time %s", formatUserOptionValue(c.ConnectIdleTime, 60, true))
	}
	if c.CallPerSession != def.CallPerSession {
		buf.Myprintf(" call_per_session %s", formatUserOptionValue(c.CallPerSession, 1, true))
	}
	if c.VnodePerCall != def.VnodePerCall {
		buf.Myprintf(" vnode_per_call %s", formatUserOptionValue(c.VnodePerCall, 1, true))
	}
	if c.FailedLoginAttempts != def.FailedLoginAttempts {
		buf.Myprintf(" failed_login_attempts %s", formatUserOptionValue(c.FailedLoginAttempts, 1, true))
	}
	if c.PasswordLifeTime != def.PasswordLifeTime {
		buf.Myprintf(" password_life_time %s", formatUserOptionValue(c.PasswordLifeTime, 24*60*60, true))
	}
	if c.PasswordReuseTime != def.PasswordReuseTime {
		buf.Myprintf(" password_reuse_time %s", formatUserOptionValue(c.PasswordReuseTime, 24*60*60, false))
	}
	if c.PasswordReuseMax != def.PasswordReuseMax {
		buf.Myprintf(" password_reuse_max %s", formatUserOptionValue(c.PasswordReuseMax, 1, false))
	}
	if c.PasswordLockTime != def.PasswordLockTime {
		buf.Myprintf(" password_lock_time %s", formatUserOptionValue(c.PasswordLockTime, 60, true))
	}
	if c.PasswordGraceTime != def.PasswordGraceTime {
		buf.Myprintf(" password_grace_time %s", formatUserOptionValue(c.PasswordGraceTime, 24*60*60, true))
	}
	if c.InactiveAccountTime != def.InactiveAccountTime {
		buf.Myprintf(" inactive_account_time %s", formatUserOptionValue(c.InactiveAccountTime, 24*60*60, true))
	}
	if len(c.IPRangeOptions) > 0 {
		if c.IPRangeOptions[0] != nil && c.IPRangeOptions[0].Neg != 0 {
			buf.Myprintf(" not_allow_host ")
		} else {
			buf.Myprintf(" host ")
		}
		formatIPRangeList(buf, c.IPRangeOptions)
	}
	if len(c.DateTimeRangeOptions) > 0 {
		if c.DateTimeRangeOptions[0] != nil && c.DateTimeRangeOptions[0].Neg != 0 {
			buf.Myprintf(" not_allow_datetime ")
			formatDateTimeRangeListWithSign(buf, c.DateTimeRangeOptions, false)
		} else {
			buf.Myprintf(" allow_datetime ")
			formatDateTimeRangeListWithSign(buf, c.DateTimeRangeOptions, true)
		}
	}
}

func (c *CreateUserStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewCreateUserStmt(lexer yyLexer, userName string, opts *UserOptions, ignoreExists bool) *CreateUserStmt {
	if opts == nil {
		opts = CreateDefaultUserOptions()
	}
	stmt := &CreateUserStmt{
		UserName:             userName,
		Password:             opts.Password,
		Totpseed:             opts.Totpseed,
		IPRangeOptions:       append([]*IpRange(nil), opts.IpRanges...),
		DateTimeRangeOptions: append([]*DateTimeRange(nil), opts.TimeRanges...),
		IgnoreExists:         ignoreExists,
		Sysinfo:              opts.Sysinfo,
		CreateDb:             opts.Createdb,
		IsImport:             opts.IsImport,
		Changepass:           opts.Changepass,
		Enable:               opts.Enable,
		SessionPerUser:       opts.SessionPerUser,
		ConnectTime:          opts.ConnectTime,
		ConnectIdleTime:      opts.ConnectIdleTime,
		CallPerSession:       opts.CallPerSession,
		VnodePerCall:         opts.VnodePerCall,
		FailedLoginAttempts:  opts.FailedLoginAttempts,
		PasswordLifeTime:     opts.PasswordLifeTime,
		PasswordReuseTime:    opts.PasswordReuseTime,
		PasswordReuseMax:     opts.PasswordReuseMax,
		PasswordLockTime:     opts.PasswordLockTime,
		PasswordGraceTime:    opts.PasswordGraceTime,
		InactiveAccountTime:  opts.InactiveAccountTime,
		AllowTokenNum:        opts.AllowTokenNum,
		NumIpRanges:          int32(len(opts.IpRanges)),
		IpRanges:             make([]string, 0, len(opts.IpRanges)),
		NumTimeRanges:        int32(len(opts.TimeRanges)),
		TimeRanges:           make([]string, 0, len(opts.TimeRanges)),
	}
	for _, r := range opts.IpRanges {
		if r != nil && r.IPNet != nil {
			stmt.IpRanges = append(stmt.IpRanges, r.IPNet.String())
		}
	}
	for _, tr := range opts.TimeRanges {
		if tr != nil {
			stmt.TimeRanges = append(stmt.TimeRanges, formatDateTimeRangeLiteral(tr))
		}
	}
	return stmt
}

func (opt *UserOptions) setUserOptionsTotpseed(lexer yyLexer, totpCode Token) {
	opt.HasTotpseed = true
	if totpCode.Bytes == nil {
		opt.Totpseed = ""
	} else {
		opt.Totpseed = tool.BytesToString(totpCode.Bytes)
	}
}

func (opt *UserOptions) setEnable(lexer yyLexer, enable Token) {
	opt.HasEnable = true
	i8Val, err := tool.ConvertBytesToInt8(enable.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid ENABLE value: %s, err:%s", enable.Bytes, err))
		return
	}
	opt.Enable = i8Val
}

func (opt *UserOptions) setSysinfo(lexer yyLexer, sysinfo Token) {
	opt.HasSysinfo = true
	i8Val, err := tool.ConvertBytesToInt8(sysinfo.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid SYSINFO value: %s, err:%s", sysinfo.Bytes, err))
		return
	}
	opt.Sysinfo = i8Val
}

func (opt *UserOptions) setIsImport(lexer yyLexer, isImport Token) {
	opt.HasIsImport = true
	i8Val, err := tool.ConvertBytesToInt8(isImport.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid IS_IMPORT value: %s, err:%s", isImport.Bytes, err))
		return
	}
	opt.IsImport = i8Val
}

func (opt *UserOptions) setCreatedb(lexer yyLexer, createdb Token) {
	opt.HasCreatedb = true
	i8Val, err := tool.ConvertBytesToInt8(createdb.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid CREATEDB value: %s, err:%s", createdb.Bytes, err))
		return
	}
	opt.Createdb = i8Val
}

func (opt *UserOptions) setChangepass(lexer yyLexer, changepass Token) {
	opt.HasChangepass = true
	i8Val, err := tool.ConvertBytesToInt8(changepass.Bytes)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid CHANGEPASS value: %s, err:%s", changepass.Bytes, err))
		return
	}
	opt.Changepass = i8Val
}

func (opt *UserOptions) setSessionPerUser(lexer yyLexer, sessionPerUser Token) {
	opt.HasSessionPerUser = true
	switch sessionPerUser.Type {
	case DEFAULT:
		opt.SessionPerUser = TSDB_USER_SESSION_PER_USER_DEFAULT
	case UNLIMITED:
		opt.SessionPerUser = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(sessionPerUser.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid SESSION_PER_USER value: %s, err:%s", sessionPerUser.Bytes, err))
			return
		}
		opt.SessionPerUser = i32Val
	}
}

func (opt *UserOptions) setConnectTime(lexer yyLexer, connectTime Token) {
	opt.HasConnectTime = true
	switch connectTime.Type {
	case DEFAULT:
		opt.ConnectTime = TSDB_USER_CONNECT_TIME_DEFAULT
	case UNLIMITED:
		opt.ConnectTime = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(connectTime.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid CONNECT_TIME value: %s, err:%s", connectTime.Bytes, err))
			return
		}
		// Lemon semantics: default unit is minute, stored in seconds.
		opt.ConnectTime = i32Val * 60
	}
}

func (opt *UserOptions) setConnectIdleTime(lexer yyLexer, connectIdleTime Token) {
	opt.HasConnectIdleTime = true
	switch connectIdleTime.Type {
	case DEFAULT:
		opt.ConnectIdleTime = TSDB_USER_CONNECT_IDLE_TIME_DEFAULT
	case UNLIMITED:
		opt.ConnectIdleTime = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(connectIdleTime.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid CONNECT_IDLE_TIME value: %s, err:%s", connectIdleTime.Bytes, err))
			return
		}
		// Lemon semantics: default unit is minute, stored in seconds.
		opt.ConnectIdleTime = i32Val * 60
	}
}

func (opt *UserOptions) setCallPerSession(lexer yyLexer, callPerSession Token) {
	opt.HasCallPerSession = true
	switch callPerSession.Type {
	case DEFAULT:
		opt.CallPerSession = TSDB_USER_CALL_PER_SESSION_DEFAULT
	case UNLIMITED:
		opt.CallPerSession = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(callPerSession.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid CALL_PER_SESSION value: %s, err:%s", callPerSession.Bytes, err))
			return
		}
		opt.CallPerSession = i32Val
	}
}

func (opt *UserOptions) setVnodePerCall(lexer yyLexer, vnodePerCall Token) {
	opt.HasVnodePerCall = true
	switch vnodePerCall.Type {
	case DEFAULT:
		opt.VnodePerCall = TSDB_USER_VNODE_PER_CALL_DEFAULT
	case UNLIMITED:
		opt.VnodePerCall = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(vnodePerCall.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid VNODE_PER_CALL value: %s, err:%s", vnodePerCall.Bytes, err))
			return
		}
		opt.VnodePerCall = i32Val
	}
}

func (opt *UserOptions) setFailedLoginAttempts(lexer yyLexer, failedLoginAttempts Token) {
	opt.HasFailedLoginAttempts = true
	switch failedLoginAttempts.Type {
	case DEFAULT:
		opt.FailedLoginAttempts = TSDB_USER_FAILED_LOGIN_ATTEMPTS_DEFAULT
	case UNLIMITED:
		opt.FailedLoginAttempts = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(failedLoginAttempts.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid FAILED_LOGIN_ATTEMPTS value: %s, err:%s", failedLoginAttempts.Bytes, err))
			return
		}
		opt.FailedLoginAttempts = i32Val
	}
}

func (opt *UserOptions) setPasswordLifeTime(lexer yyLexer, passwordLifeTime Token) {
	opt.HasPasswordLifeTime = true
	switch passwordLifeTime.Type {
	case DEFAULT:
		opt.PasswordLifeTime = TSDB_USER_PASSWORD_LIFE_TIME_DEFAULT
	case UNLIMITED:
		opt.PasswordLifeTime = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(passwordLifeTime.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid PASSWORD_LIFE_TIME value: %s, err:%s", passwordLifeTime.Bytes, err))
			return
		}
		// Lemon semantics: default unit is day, stored in seconds.
		opt.PasswordLifeTime = i32Val * 1440 * 60
	}
}

func (opt *UserOptions) setPasswordReuseTime(lexer yyLexer, passwordReuseTime Token) {
	opt.HasPasswordReuseTime = true
	switch passwordReuseTime.Type {
	case DEFAULT:
		opt.PasswordReuseTime = TSDB_USER_PASSWORD_REUSE_TIME_DEFAULT
	case UNLIMITED:
		lexer.Error("invalid PASSWORD_REUSE_TIME value: UNLIMITED")
		return
	default:
		i32Val, err := tool.ConvertBytesToInt32(passwordReuseTime.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid PASSWORD_REUSE_TIME value: %s, err:%s", passwordReuseTime.Bytes, err))
			return
		}
		// Lemon semantics: default unit is day, stored in seconds.
		opt.PasswordReuseTime = i32Val * 1440 * 60
	}
}

func (opt *UserOptions) setPasswordReuseMax(lexer yyLexer, passwordReuseMax Token) {
	opt.HasPasswordReuseMax = true
	switch passwordReuseMax.Type {
	case DEFAULT:
		opt.PasswordReuseMax = TSDB_USER_PASSWORD_REUSE_MAX_DEFAULT
	case UNLIMITED:
		lexer.Error("invalid PASSWORD_REUSE_MAX value: UNLIMITED")
		return
	default:
		i32Val, err := tool.ConvertBytesToInt32(passwordReuseMax.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid PASSWORD_REUSE_MAX value: %s, err:%s", passwordReuseMax.Bytes, err))
			return
		}
		opt.PasswordReuseMax = i32Val
	}
}

func (opt *UserOptions) setPasswordLockTime(lexer yyLexer, passwordLockTime Token) {
	opt.HasPasswordLockTime = true
	switch passwordLockTime.Type {
	case DEFAULT:
		opt.PasswordLockTime = TSDB_USER_PASSWORD_LOCK_TIME_DEFAULT
	case UNLIMITED:
		opt.PasswordLockTime = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(passwordLockTime.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid PASSWORD_LOCK_TIME value: %s, err:%s", passwordLockTime.Bytes, err))
			return
		}
		// Lemon semantics: default unit is minute, stored in seconds.
		opt.PasswordLockTime = i32Val * 60
	}
}

func (opt *UserOptions) setPasswordGraceTime(lexer yyLexer, passwordGraceTime Token) {
	opt.HasPasswordGraceTime = true
	switch passwordGraceTime.Type {
	case DEFAULT:
		opt.PasswordGraceTime = TSDB_USER_PASSWORD_GRACE_TIME_DEFAULT
	case UNLIMITED:
		opt.PasswordGraceTime = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(passwordGraceTime.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid PASSWORD_GRACE_TIME value: %s, err:%s", passwordGraceTime.Bytes, err))
			return
		}
		// Lemon semantics: default unit is day, stored in seconds.
		opt.PasswordGraceTime = i32Val * 1440 * 60
	}
}

func (opt *UserOptions) setInactiveAccountTime(lexer yyLexer, inactiveAccountTime Token) {
	opt.HasInactiveAccountTime = true
	switch inactiveAccountTime.Type {
	case DEFAULT:
		opt.InactiveAccountTime = TSDB_USER_INACTIVE_ACCOUNT_TIME_DEFAULT
	case UNLIMITED:
		opt.InactiveAccountTime = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(inactiveAccountTime.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid INACTIVE_ACCOUNT_TIME value: %s, err:%s", inactiveAccountTime.Bytes, err))
			return
		}
		// Lemon semantics: default unit is day, stored in seconds.
		opt.InactiveAccountTime = i32Val * 1440 * 60
	}
}

func (opt *UserOptions) setAllowTokenNum(lexer yyLexer, allowTokenNum Token) {
	opt.HasAllowTokenNum = true
	switch allowTokenNum.Type {
	case DEFAULT:
		opt.AllowTokenNum = TSDB_USER_ALLOW_TOKEN_NUM_DEFAULT
	case UNLIMITED:
		opt.AllowTokenNum = -1
	default:
		i32Val, err := tool.ConvertBytesToInt32(allowTokenNum.Bytes)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid ALLOW_TOKEN_NUM value: %s, err:%s", allowTokenNum.Bytes, err))
			return
		}
		opt.AllowTokenNum = i32Val
	}
}

func (opt *UserOptions) setHostList(lexer yyLexer, hostList []*IpRange) {
	opt.IpRanges = hostList
}

func (opt *UserOptions) setNotAllowHostList(lexer yyLexer, hostList []*IpRange) {
	for _, ipRange := range hostList {
		ipRange.Neg = 1
	}
	opt.IpRanges = hostList
}

func (opt *UserOptions) setAllowDateTimeList(lexer yyLexer, timeRanges []*DateTimeRange) {
	opt.TimeRanges = timeRanges
}

func (opt *UserOptions) setNotAllowDateTimeList(lexer yyLexer, timeRanges []*DateTimeRange) {
	for _, timeRange := range timeRanges {
		timeRange.Neg = 1
	}
	opt.TimeRanges = timeRanges
}

func (opt *UserOptions) setUserOptionsPassword(lexer yyLexer, password Token) {
	// todo: check password strength
	opt.HasPassword = true
	if password.Bytes == nil {
		opt.Password = ""
	} else {
		opt.Password = tool.BytesToString(password.Bytes)
	}
}

func (opt *UserOptions) setDropHostList(lexer yyLexer, hostList []*IpRange) {
	opt.DropIpRanges = hostList
}

func (opt *UserOptions) setDropAllowDateTimeList(lexer yyLexer, timeRanges []*DateTimeRange) {
	opt.DropTimeRanges = timeRanges
}

func (opt *UserOptions) setDropNotAllowHostList(lexer yyLexer, hostList []*IpRange) {
	for _, ipRange := range hostList {
		ipRange.Neg = 1
	}
	opt.DropIpRanges = hostList
}

func (opt *UserOptions) setDropNotAllowDateTimeList(lexer yyLexer, timeRanges []*DateTimeRange) {
	for _, timeRange := range timeRanges {
		timeRange.Neg = 1
	}
	opt.DropTimeRanges = timeRanges
}

type AlterUserStmt struct {
	UserName    string
	UserOptions *UserOptions
}

func NewAlterUserStmt(lexer yyLexer, userName string, opts *UserOptions) *AlterUserStmt {
	return &AlterUserStmt{
		UserName:    userName,
		UserOptions: opts,
	}
}

func (a *AlterUserStmt) iStatement() {}

func (a *AlterUserStmt) Format(buf *TrackedBuffer) {
	if a == nil {
		return
	}
	buf.Myprintf("alter user %s", a.UserName)
	if a.UserOptions == nil {
		return
	}
	if a.UserOptions.HasPassword {
		buf.Myprintf(" pass '%s'", a.UserOptions.Password)
	}
	if a.UserOptions.HasTotpseed {
		if a.UserOptions.Totpseed == "" {
			buf.Myprintf(" totpseed null")
		} else {
			buf.Myprintf(" totpseed '%s'", a.UserOptions.Totpseed)
		}
	}
	if a.UserOptions.HasEnable {
		buf.Myprintf(" enable %s", strconv.FormatInt(int64(a.UserOptions.Enable), 10))
	}
	if a.UserOptions.HasSysinfo {
		buf.Myprintf(" sysinfo %s", strconv.FormatInt(int64(a.UserOptions.Sysinfo), 10))
	}
	if a.UserOptions.HasIsImport {
		buf.Myprintf(" is_import %s", strconv.FormatInt(int64(a.UserOptions.IsImport), 10))
	}
	if a.UserOptions.HasCreatedb {
		buf.Myprintf(" createdb %s", strconv.FormatInt(int64(a.UserOptions.Createdb), 10))
	}
	if a.UserOptions.HasChangepass {
		buf.Myprintf(" changepass %s", strconv.FormatInt(int64(a.UserOptions.Changepass), 10))
	}
	if a.UserOptions.HasSessionPerUser {
		buf.Myprintf(" session_per_user %s", formatUserOptionValue(a.UserOptions.SessionPerUser, 1, true))
	}
	if a.UserOptions.HasConnectTime {
		buf.Myprintf(" connect_time %s", formatUserOptionValue(a.UserOptions.ConnectTime, 60, true))
	}
	if a.UserOptions.HasConnectIdleTime {
		buf.Myprintf(" connect_idle_time %s", formatUserOptionValue(a.UserOptions.ConnectIdleTime, 60, true))
	}
	if a.UserOptions.HasCallPerSession {
		buf.Myprintf(" call_per_session %s", formatUserOptionValue(a.UserOptions.CallPerSession, 1, true))
	}
	if a.UserOptions.HasVnodePerCall {
		buf.Myprintf(" vnode_per_call %s", formatUserOptionValue(a.UserOptions.VnodePerCall, 1, true))
	}
	if a.UserOptions.HasFailedLoginAttempts {
		buf.Myprintf(" failed_login_attempts %s", formatUserOptionValue(a.UserOptions.FailedLoginAttempts, 1, true))
	}
	if a.UserOptions.HasPasswordLifeTime {
		buf.Myprintf(" password_life_time %s", formatUserOptionValue(a.UserOptions.PasswordLifeTime, 24*60*60, true))
	}
	if a.UserOptions.HasPasswordReuseTime {
		buf.Myprintf(" password_reuse_time %s", formatUserOptionValue(a.UserOptions.PasswordReuseTime, 24*60*60, false))
	}
	if a.UserOptions.HasPasswordReuseMax {
		buf.Myprintf(" password_reuse_max %s", formatUserOptionValue(a.UserOptions.PasswordReuseMax, 1, false))
	}
	if a.UserOptions.HasPasswordLockTime {
		buf.Myprintf(" password_lock_time %s", formatUserOptionValue(a.UserOptions.PasswordLockTime, 60, true))
	}
	if a.UserOptions.HasPasswordGraceTime {
		buf.Myprintf(" password_grace_time %s", formatUserOptionValue(a.UserOptions.PasswordGraceTime, 24*60*60, true))
	}
	if a.UserOptions.HasInactiveAccountTime {
		buf.Myprintf(" inactive_account_time %s", formatUserOptionValue(a.UserOptions.InactiveAccountTime, 24*60*60, true))
	}
	if a.UserOptions.HasAllowTokenNum {
		buf.Myprintf(" allow_token_num %s", formatUserOptionValue(a.UserOptions.AllowTokenNum, 1, true))
	}
	if len(a.UserOptions.IpRanges) > 0 {
		if a.UserOptions.IpRanges[0] != nil && a.UserOptions.IpRanges[0].Neg != 0 {
			buf.Myprintf(" add not_allow_host ")
		} else {
			buf.Myprintf(" add host ")
		}
		formatIPRangeList(buf, a.UserOptions.IpRanges)
	}
	if len(a.UserOptions.DropIpRanges) > 0 {
		if a.UserOptions.DropIpRanges[0] != nil && a.UserOptions.DropIpRanges[0].Neg != 0 {
			buf.Myprintf(" drop not_allow_host ")
		} else {
			buf.Myprintf(" drop host ")
		}
		formatIPRangeList(buf, a.UserOptions.DropIpRanges)
	}
	if len(a.UserOptions.TimeRanges) > 0 {
		if a.UserOptions.TimeRanges[0] != nil && a.UserOptions.TimeRanges[0].Neg != 0 {
			buf.Myprintf(" add not_allow_datetime ")
		} else {
			buf.Myprintf(" add allow_datetime ")
		}
		formatDateTimeRangeList(buf, a.UserOptions.TimeRanges)
	}
	if len(a.UserOptions.DropTimeRanges) > 0 {
		if a.UserOptions.DropTimeRanges[0] != nil && a.UserOptions.DropTimeRanges[0].Neg != 0 {
			buf.Myprintf(" drop not_allow_datetime ")
		} else {
			buf.Myprintf(" drop allow_datetime ")
		}
		formatDateTimeRangeList(buf, a.UserOptions.DropTimeRanges)
	}
}

func (a *AlterUserStmt) walkSubtree(visit Visit) error {
	return nil
}

type DropUserStmt struct {
	UserName       string
	IgnoreNotExist bool
}

func NewDropUserStmt(lexer yyLexer, userName string, ignoreNotExist bool) *DropUserStmt {
	return &DropUserStmt{
		UserName:       userName,
		IgnoreNotExist: ignoreNotExist,
	}
}

func (d *DropUserStmt) iStatement() {}

func (d *DropUserStmt) Format(buf *TrackedBuffer) {
	if d == nil {
		return
	}
	buf.Myprintf("drop user ")
	if d.IgnoreNotExist {
		buf.Myprintf("if exists ")
	}
	buf.Myprintf("%s", d.UserName)
}

func (d *DropUserStmt) walkSubtree(visit Visit) error {
	return nil
}
