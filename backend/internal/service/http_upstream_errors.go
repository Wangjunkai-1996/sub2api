package service

import (
	"errors"
	"fmt"
)

// ErrHTTPUpstreamRequestNotSent identifies an upstream failure that happened
// before the HTTP transport was invoked. Callers that must fence duplicate
// POSTs can safely retry these errors; all other transport errors remain
// conservatively ambiguous.
var ErrHTTPUpstreamRequestNotSent = errors.New("http upstream request not sent")

// HTTPUpstreamRequestNotSentError preserves the underlying setup error while
// exposing the explicit pre-send classification across the ports/adapters
// boundary.
type HTTPUpstreamRequestNotSentError struct {
	Err error
}

func (e *HTTPUpstreamRequestNotSentError) Error() string {
	if e == nil || e.Err == nil {
		return ErrHTTPUpstreamRequestNotSent.Error()
	}
	return fmt.Sprintf("%s: %v", ErrHTTPUpstreamRequestNotSent, e.Err)
}

func (e *HTTPUpstreamRequestNotSentError) Unwrap() error {
	if e == nil {
		return ErrHTTPUpstreamRequestNotSent
	}
	return errors.Join(ErrHTTPUpstreamRequestNotSent, e.Err)
}

// MarkHTTPUpstreamRequestNotSent wraps a setup failure once. It is safe to use
// at adapter boundaries and leaves nil errors untouched.
func MarkHTTPUpstreamRequestNotSent(err error) error {
	if err == nil || errors.Is(err, ErrHTTPUpstreamRequestNotSent) {
		return err
	}
	return &HTTPUpstreamRequestNotSentError{Err: err}
}

func IsHTTPUpstreamRequestNotSent(err error) bool {
	return errors.Is(err, ErrHTTPUpstreamRequestNotSent)
}
