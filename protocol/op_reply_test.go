package protocol

import (
	"bytes"
	"encoding/hex"

	. "gopkg.in/check.v1"
)

var (
	fixtureOpReply = "080000000000000000000000000000000300000021000000075f69640054f341f02ce0555a290041a712780001000000000000000021000000075f69640054f341f22ce05560290041a712780001000000000000000021000000075f69640054f341f52ce05566290041a7127800010000000000000000"
)

func (s *ProtocolSuite) TestOpReply_FromWire(c *C) {
	fixture, _ := hex.DecodeString(fixtureOpReply)

	op, err := ReadOpReply(&MsgHeader{}, bytes.NewReader(fixture))
	c.Assert(err, IsNil)

	c.Assert(op.ResponseFlags, Equals, int32(8))
	c.Assert(op.CursorID, Equals, int64(0))
	c.Assert(op.StartingFrom, Equals, int32(0))
	c.Assert(op.NumberReturned, Equals, int32(3))
	c.Assert(op.Documents, HasLen, 3)

	b, _ := op.Documents[0].ToBSON()
	c.Assert(b.Map()["x"], Equals, int64(1))

	var w bytes.Buffer
	err = op.toWire(&w)
	c.Assert(err, IsNil)
	c.Assert(hex.EncodeToString(w.Bytes()), Equals, fixtureOpReply)
}
