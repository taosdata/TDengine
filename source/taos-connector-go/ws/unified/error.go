package unified

import (
	"errors"
	"fmt"
	"strings"
)

// ErrorType identifies unified package error categories.
type ErrorType string

const (
	ErrorTypeUnknown         ErrorType = "unknown"
	ErrorTypeInvalidConfig   ErrorType = "invalid_config"
	ErrorTypeInvalidDSN      ErrorType = "invalid_dsn"
	ErrorTypeProtocol        ErrorType = "protocol"
	ErrorTypeConnectTimeout  ErrorType = "connect_timeout"
	ErrorTypeMessageTimeout  ErrorType = "message_timeout"
	ErrorTypeClientClosed    ErrorType = "client_closed"
	ErrorTypeInvalidState    ErrorType = "invalid_state"
	ErrorTypeReconnectFailed ErrorType = "reconnect_failed"
)

// Error is the unified package error model.
type Error struct {
	Type                   ErrorType
	Message                string
	Cause                  error
	RequestSummary         string
	ConnectionRelated      bool
	ConnectionDisconnected bool
	ReconnectFailed        bool
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	base := string(e.Type)
	if e.Message != "" {
		base = e.Message
	} else if e.Cause != nil {
		base = e.Cause.Error()
	}
	summary := strings.TrimSpace(e.RequestSummary)
	if summary == "" {
		return base
	}
	if base == "" {
		return "request=" + summary
	}
	return fmt.Sprintf("%s; request=%s", base, summary)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	if t.Type != "" && e.Type != t.Type {
		return false
	}
	if t.Message != "" && e.Message != t.Message {
		return false
	}
	return true
}

var (
	ErrNilConfig = &Error{
		Type:    ErrorTypeInvalidConfig,
		Message: "nil config",
	}
	ErrNoEndpoints = &Error{
		Type:    ErrorTypeInvalidConfig,
		Message: "ws config requires at least one endpoint",
	}
	ErrInvalidEndpointIndex = &Error{
		Type:    ErrorTypeInvalidConfig,
		Message: "invalid endpoint index",
	}
	ErrNilRuntime = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "cannot swap to nil runtime",
	}
	ErrInvalidDSNUnescaped = &Error{
		Type:    ErrorTypeInvalidDSN,
		Message: "invalid DSN: did you forget to escape a param value?",
	}
	ErrInvalidDSNAddr = &Error{
		Type:    ErrorTypeInvalidDSN,
		Message: "invalid DSN: network address not terminated (missing closing brace)",
	}
	ErrInvalidDSNPort = &Error{
		Type:    ErrorTypeInvalidDSN,
		Message: "invalid DSN: network port is not a valid number",
	}
	ErrInvalidDSNNoSlash = &Error{
		Type:    ErrorTypeInvalidDSN,
		Message: "invalid DSN: missing the slash separating the database name",
	}
	ErrReqIDNotFound = &Error{
		Type:    ErrorTypeProtocol,
		Message: "req_id not found",
	}
	ErrBinaryMessageTooShort = &Error{
		Type:    ErrorTypeProtocol,
		Message: "binary message too short",
	}
	ErrBinaryMessageExtendedHeaderTooShort = &Error{
		Type:    ErrorTypeProtocol,
		Message: "binary message with extended header too short",
	}
	ErrConnectTimeout = &Error{
		Type:              ErrorTypeConnectTimeout,
		Message:           "connect timeout",
		ConnectionRelated: true,
	}
	ErrSchemalessMessageTimeout = &Error{
		Type:              ErrorTypeMessageTimeout,
		Message:           "schemaless message timeout",
		ConnectionRelated: true,
	}
	ErrQueryMessageTimeout = &Error{
		Type:              ErrorTypeMessageTimeout,
		Message:           "query message timeout",
		ConnectionRelated: true,
	}
	ErrStmtMessageTimeout = &Error{
		Type:              ErrorTypeMessageTimeout,
		Message:           "stmt message timeout",
		ConnectionRelated: true,
	}
	ErrUnifiedClosed = &Error{
		Type:                   ErrorTypeClientClosed,
		Message:                "ws client is closed",
		ConnectionRelated:      true,
		ConnectionDisconnected: true,
	}
	ErrQueryResultConnectionLost = &Error{
		Type:                   ErrorTypeClientClosed,
		Message:                "query result connection lost",
		ConnectionRelated:      true,
		ConnectionDisconnected: true,
	}
	ErrStmtConnectionLost = &Error{
		Type:                   ErrorTypeClientClosed,
		Message:                "stmt connection lost",
		ConnectionRelated:      true,
		ConnectionDisconnected: true,
	}
	ErrQueryResultClosed = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "query result is closed",
	}
	ErrInvalidFetchRawBlockResponse = &Error{
		Type:    ErrorTypeProtocol,
		Message: "invalid fetch raw block response",
	}
	ErrUnifiedConnectFailed = &Error{
		Type:              ErrorTypeReconnectFailed,
		Message:           "ws connect failed",
		ConnectionRelated: true,
		ReconnectFailed:   true,
	}
	ErrStmtTableNameNotRequired = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "table name is not required for this statement",
	}
	ErrStmtTableNameEmpty = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "table name cannot be empty",
	}
	ErrStmtTagsNotNeeded = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "this statement does not need tags",
	}
	ErrStmtTagsNil = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "tags cannot be nil",
	}
	ErrStmtParamsEmpty = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "params cannot be empty",
	}
	ErrStmtNoBatchAdded = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "no batch added. call Bind() before Exec() (or AddBatch() when using compatibility APIs)",
	}
	ErrStmtTableNameNotSet = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "table name is not set",
	}
	ErrStmtTagsNotSet = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "tags are not set",
	}
	ErrStmtColumnsNotSet = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "columns are not set",
	}
	ErrStmtNoRowsToAdd = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "no rows to add",
	}
	ErrStmtReprepareSchemaChanged = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "failed to re-prepare the statement: schema changed, call Prepare() again",
	}
	ErrStmtNotPrepared = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "this statement has not been prepared",
	}
	ErrStmtSchemaChanged = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "the schema has changed, call Prepare() again",
	}
	ErrStmtBindAfterCompatAPI = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "Bind() cannot be used after compatibility APIs (SetTableName/SetTags/BindParam/AddBatch) in the same prepared statement",
	}
	ErrStmtCompatAPIAfterBind = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "compatibility APIs (SetTableName/SetTags/BindParam/AddBatch) cannot be used after Bind() in the same prepared statement",
	}
	ErrStmtQueryRebindBeforeExec = &Error{
		Type:    ErrorTypeInvalidState,
		Message: "query statement does not support multiple Bind() calls before Exec(); call Exec() first",
	}
)

// ErrorTypeOf extracts unified error type from err.
func ErrorTypeOf(err error) ErrorType {
	var target *Error
	if errors.As(err, &target) {
		return target.Type
	}
	return ErrorTypeUnknown
}

// IsErrorType reports whether err is a unified error with the provided type.
func IsErrorType(err error, t ErrorType) bool {
	return ErrorTypeOf(err) == t
}

// IsConnectionRelatedError reports whether err is connection-related.
func IsConnectionRelatedError(err error) bool {
	var target *Error
	if errors.As(err, &target) {
		return target.ConnectionRelated
	}
	return false
}

// IsConnectionDisconnectedError reports whether err indicates connection disconnection/closed.
func IsConnectionDisconnectedError(err error) bool {
	var target *Error
	if errors.As(err, &target) {
		return target.ConnectionDisconnected
	}
	return false
}

// IsReconnectFailedError reports whether err indicates reconnect/connect failure.
func IsReconnectFailedError(err error) bool {
	var target *Error
	if errors.As(err, &target) {
		return target.ReconnectFailed
	}
	return false
}

func newErrorf(errorType ErrorType, format string, args ...interface{}) *Error {
	return &Error{
		Type:    errorType,
		Message: fmt.Sprintf(format, args...),
	}
}

func newInvalidConfigErrorf(format string, args ...interface{}) *Error {
	return newErrorf(ErrorTypeInvalidConfig, format, args...)
}

func newInvalidDSNErrorf(format string, args ...interface{}) *Error {
	return newErrorf(ErrorTypeInvalidDSN, format, args...)
}

func newInvalidStateErrorf(format string, args ...interface{}) *Error {
	return newErrorf(ErrorTypeInvalidState, format, args...)
}

func attachRequestSummary(err error, requestSummary string) error {
	if err == nil {
		return nil
	}
	requestSummary = strings.TrimSpace(requestSummary)
	if requestSummary == "" {
		return err
	}

	var unifiedErr *Error
	if errors.As(err, &unifiedErr) {
		clone := *unifiedErr
		if strings.TrimSpace(clone.RequestSummary) == "" {
			clone.RequestSummary = requestSummary
		}
		// Preserve the full original chain when the matched unified error
		// is wrapped by other errors.
		if matchedErr := error(unifiedErr); matchedErr != err {
			clone.Cause = err
		}
		return &clone
	}

	return &Error{
		Cause:          err,
		RequestSummary: requestSummary,
	}
}
