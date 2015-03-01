package protocol

import (
	"bytes"
	"encoding/hex"

	. "gopkg.in/check.v1"
)

var (
	//db.bar.find({qux:"foo"}).skip(42).limit(84)
	fixtureOpQuery = "00000000746573742e626172002a0000005400000012000000027175780004000000666f6f0000"
	//db.bar.find({qux:"foo"}).skip(42).limit(84)
	fixtureOpQueryWithProjection = "00000000746573742e626172002a0000005400000012000000027175780004000000666f6f0000120000000171757800000000000000f03f00"
)

func (s *ProtocolSuite) TestOpQuery_FromWire(c *C) {
	fixture, _ := hex.DecodeString(fixtureOpQuery)

	op, err := ReadOpQuery(&MsgHeader{}, bytes.NewReader(fixture))
	c.Assert(err, IsNil)

	c.Assert(op.Flags, Equals, int32(0))
	c.Assert(op.FullCollectionName.String(), Equals, "test.bar")
	c.Assert(op.NumberToSkip, Equals, int32(42))
	c.Assert(op.NumberToReturn, Equals, int32(84))

	q, err := op.Query.ToBSON()
	c.Assert(err, IsNil)
	c.Assert(q.Map()["qux"], Equals, "foo")

	var w bytes.Buffer
	err = op.toWire(&w)
	c.Assert(err, IsNil)
	c.Assert(hex.EncodeToString(w.Bytes()), Equals, fixtureOpQuery)
}

func (s *ProtocolSuite) TestOpQuery_FromWireWithProjection(c *C) {
	fixture, _ := hex.DecodeString(fixtureOpQueryWithProjection)

	op, err := ReadOpQuery(&MsgHeader{}, bytes.NewReader(fixture))
	c.Assert(err, IsNil)

	q, err := op.ReturnFieldsSelector.ToBSON()
	c.Assert(err, IsNil)
	c.Assert(q.Map()["qux"], Equals, float64(1))

	var w bytes.Buffer
	err = op.toWire(&w)
	c.Assert(err, IsNil)
	c.Assert(hex.EncodeToString(w.Bytes()), Equals, fixtureOpQueryWithProjection)
}

func (s *ProtocolSuite) TestOpQuery_String(c *C) {
	fixture, _ := hex.DecodeString(fixtureOpQueryWithProjection)

	op, err := ReadOpQuery(&MsgHeader{}, bytes.NewReader(fixture))
	c.Assert(err, IsNil)
	c.Assert(
		op.String(),
		Equals,
		"opQuery - collection: test.bar q: {\"qux\":\"foo\"} p: {\"qux\":1} skip:42 limit:84",
	)
}
