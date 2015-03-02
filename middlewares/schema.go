package middlewares

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/mcuadros/lemondb/protocol"
)

var fixtureOpReply = "080000000000000000000000000000000300000021000000075f69640054f341f02ce0555a290041a712780001000000000000000021000000075f69640054f341f22ce05560290041a712780001000000000000000021000000075f69640054f341f52ce05566290041a7127800010000000000000000"

type SchemaMiddleware struct {
	PrevMiddleware *ProxyMiddleware
}

func (m *SchemaMiddleware) Handle(
	msg protocol.Message,
	c io.ReadWriter,
	s io.ReadWriter,
) error {
	if msg.GetOpCode() == protocol.OpQueryCode {
		h := msg.(*protocol.MsgHeader)
		query, _ := protocol.ReadOpQuery(h, bytes.NewReader(h.Message))
		fmt.Println(query.String())

		if query.FullCollectionName.String() == "test.foo" {
			fixture, _ := hex.DecodeString(fixtureOpReply)
			rh := &protocol.MsgHeader{
				MessageLength: protocol.HeaderLen + int32(len(fixture)),
				RequestID:     1111111,
				ResponseTo:    h.RequestID,
				OpCode:        protocol.OpReplyCode,
			}

			op, _ := protocol.ReadOpReply(rh, bytes.NewReader(fixture))
			err := op.WriteTo(c)
			fmt.Println("foo", err)

			return nil
		}
	}

	return m.PrevMiddleware.Handle(msg, c, s)
}
