package db

import "errors"

var (
	// ErrInternalDBError is used when the internal database returns an error
	ErrInternalDBError = errors.New("internal database returned an error")
	ErrIndexExists     = errors.New("index already exists")
	ErrIndexUnknown    = errors.New("index is unknown")
	ErrUnknownDataType = errors.New("data type not supported")
	ErrFieldUnknown    = errors.New("field not part of the index")
	ErrIndexNotSet     = errors.New("index must be set before lookup")
	ErrLookupFailure   = errors.New("could not complete lookup")
	ErrLookupEmpty     = errors.New("no results for lookup")
	ErrSegmentMissing  = errors.New("segment was not available for lookup")
)
