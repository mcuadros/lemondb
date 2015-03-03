package middlewares

import (
	"bytes"
	"fmt"
	"io"

	"github.com/mcuadros/lemondb/protocol"
)

type PlaygroundMiddleware struct {
	PrevMiddleware *ProxyMiddleware
}

func (m *PlaygroundMiddleware) Handle(
	msg protocol.Message,
	c io.ReadWriter,
	s io.ReadWriter,
) error {
	if msg.GetOpCode() == protocol.OpQueryCode {
		h := msg.(*protocol.MsgHeader)
		query, _ := protocol.ReadOpQuery(h, bytes.NewReader(h.Message))
		fmt.Println(query.String())

		if query.FullCollectionName.String() == "test.foo" {
			op := protocol.NewOpReplay(query, 1111111)
			op.AddDocument(map[string]string{"foo": "bar"})
			op.WriteTo(c)

			return nil
		}
	}

	return m.PrevMiddleware.Handle(msg, c, s)
}
