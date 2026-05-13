package client

import "errors"

type wrappedClosedError struct {
	closedErr error
	cause     error
}

func (e *wrappedClosedError) Error() string {
	return e.cause.Error()
}

func (e *wrappedClosedError) Unwrap() error {
	return e.cause
}

func (e *wrappedClosedError) Is(target error) bool {
	return target == e.closedErr || errors.Is(e.cause, target)
}

// WrapClosedError preserves the original transport error while keeping a stable
// sentinel for closed-connection checks.
func WrapClosedError(closedErr error, cause error) error {
	if cause == nil {
		return closedErr
	}
	if errors.Is(cause, closedErr) {
		return cause
	}
	return &wrappedClosedError{
		closedErr: closedErr,
		cause:     cause,
	}
}
