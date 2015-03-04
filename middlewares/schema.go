package middlewares

import (
	"bytes"
	"io"

	"github.com/mcuadros/lemondb/protocol"
)

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
		if query.FullCollectionName.String() == "test.$cmd" {
			b, _ := query.Query.ToBSON()
			if b.Map()["insert"] == "foo" {
				op := protocol.NewOpReplay(query, 1111111)
				op.AddDocument(&protocol.WriteResult{
					Result:      1,
					WriteErrors: []protocol.WriteError{{0, 42, "foo bar"}},
				})

				return op.WriteTo(c)
			}
		}
	}

	return m.PrevMiddleware.Handle(msg, c, s)
}
