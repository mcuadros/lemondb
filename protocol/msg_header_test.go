package protocol

import (
	"bytes"
	"encoding/hex"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type ProtocolSuite struct{}

var _ = Suite(&ProtocolSuite{})

func (s *ProtocolSuite) TestReadMsgHeader(c *C) {
	fixture, _ := hex.DecodeString("880000009900000000000000d4070000")
	r := bytes.NewReader(fixture)

	h, err := ReadMsgHeader(r)
	c.Assert(err, IsNil)
	c.Assert(h.MessageLength, Equals, int32(136))
	c.Assert(h.RequestID, Equals, int32(153))
	c.Assert(h.ResponseTo, Equals, int32(0))
	c.Assert(h.OpCode, Equals, OpQueryCode)
}

func (s *ProtocolSuite) TestMsgHeader_toWire(c *C) {
	h := &MsgHeader{
		MessageLength: 136,
		RequestID:     153,
		ResponseTo:    0,
		OpCode:        OpQueryCode,
	}

	w := bytes.NewBuffer([]byte{})
	h.toWire(w)
	c.Assert(
		hex.EncodeToString(w.Bytes()),
		Equals,
		"880000009900000000000000d4070000",
	)
}

func (s *ProtocolSuite) TestMsgHeader_String(c *C) {
	m := &MsgHeader{
		OpCode:        OpQueryCode,
		MessageLength: 10,
		RequestID:     42,
		ResponseTo:    43,
	}
	c.Assert(m.String(), Equals, "opCode:QUERY (2004) msgLen:10 reqID:42 respID:43")
}
