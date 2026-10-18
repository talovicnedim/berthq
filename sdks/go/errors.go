package berthq

import "errors"

// Error kinds. All but KindHTTP come from the API's error body; KindHTTP means the broker
// could not be reached or answered something that isn't an API error.
const (
	KindInvalid      = "invalid"      // 400: bad input
	KindUnauthorized = "unauthorized" // 401: missing or unknown API key
	KindForbidden    = "forbidden"    // 403: the API key's scope doesn't allow this
	KindNotFound     = "notFound"     // 404: the queue, consumer or message doesn't exist
	KindConflict     = "conflict"     // 409: it already exists
	KindQueueFull    = "queueFull"    // 507: the queue's backlog hit maxMessages
	KindDiskFull     = "diskFull"     // 507: the broker's disk is almost full, so it takes no new messages
	KindUnavailable  = "unavailable"  // 503: the broker is shutting down
	KindStorage      = "storage"      // 500: writing to disk failed
	KindHTTP         = "http"         // transport error or unexpected response
)

// Error is returned by every Client method. Use errors.As to inspect it:
//
//	var berr *berthq.Error
//	if errors.As(err, &berr) && berr.Kind == berthq.KindQueueFull { ... }
type Error struct {
	// Kind is one of the Kind constants.
	Kind string
	// Status is the HTTP status code, or 0 when the broker could not be reached.
	Status int
	// Message is the broker's (or the transport's) explanation.
	Message string
	// Err is the underlying transport error, if any.
	Err error
}

func (e *Error) Error() string {
	return e.Message
}

func (e *Error) Unwrap() error {
	return e.Err
}

// ErrorKind returns the Kind of err if it is (or wraps) an *Error, and "" otherwise.
func ErrorKind(err error) string {
	var berr *Error
	if errors.As(err, &berr) {
		return berr.Kind
	}
	return ""
}

// IsNotFound reports whether err is a notFound error from the broker.
func IsNotFound(err error) bool {
	return ErrorKind(err) == KindNotFound
}

// IsConflict reports whether err is a conflict error, e.g. creating a queue or consumer that
// exists.
func IsConflict(err error) bool {
	return ErrorKind(err) == KindConflict
}
