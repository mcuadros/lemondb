package protocol

import (
	"encoding/json"
	"io"

	"gopkg.in/mgo.v2/bson"
)

// Look at http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/ for the protocol.

// OpCode allow identifying the type of operation:
//
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#request-opcodes
type OpCode int32

// String returns a human readable representation of the OpCode.
func (c OpCode) String() string {
	switch c {
	default:
		return "UNKNOWN"
	case OpReplyCode:
		return "REPLY"
	case OpMessageCode:
		return "MESSAGE"
	case OpUpdateCode:
		return "UPDATE"
	case OpInsertCode:
		return "INSERT"
	case Reserved:
		return "RESERVED"
	case OpQueryCode:
		return "QUERY"
	case OpGetMoreCode:
		return "GET_MORE"
	case OpDeleteCode:
		return "DELETE"
	case OpKillCursorsCode:
		return "KILL_CURSORS"
	}
}

// IsMutation tells us if the operation will mutate data. These operations can
// be followed up by a getLastErr operation.
func (c OpCode) IsMutation() bool {
	return c == OpInsertCode || c == OpUpdateCode || c == OpDeleteCode
}

// HasResponse tells us if the operation will have a response from the server.
func (c OpCode) HasResponse() bool {
	return c == OpQueryCode || c == OpGetMoreCode
}

// The full set of known request op codes:
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#request-opcodes
const (
	OpReplyCode       = OpCode(1)
	OpMessageCode     = OpCode(1000)
	OpUpdateCode      = OpCode(2001)
	OpInsertCode      = OpCode(2002)
	Reserved          = OpCode(2003)
	OpQueryCode       = OpCode(2004)
	OpGetMoreCode     = OpCode(2005)
	OpDeleteCode      = OpCode(2006)
	OpKillCursorsCode = OpCode(2007)
)

type Message interface {
	WriteTo(w io.Writer) error
}

type Document []byte

func (s Document) String() string {
	b, _ := s.ToBSON()
	j, _ := json.Marshal(b.Map())
	return string(j)
}

func (s Document) ToBSON() (bson.D, error) {
	var q bson.D
	if err := bson.Unmarshal(s, &q); err != nil {
		return nil, err
	}

	return q, nil
}

type CSString []byte

func (s CSString) String() string {
	if len(s) == 0 {
		return ""
	}

	return string(s[:len(s)-1])
}
