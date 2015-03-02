package middlewares

import (
	"io"

	"github.com/mcuadros/lemondb/protocol"
)

type ProxyMiddleware struct{}

// proxyMessage proxies a message, possibly it's response, and possibly a
// follow up call.
func (m *ProxyMiddleware) Handle(
	msg protocol.Message,
	c io.ReadWriter,
	s io.ReadWriter,
) error {
	if err := msg.WriteTo(s); err != nil {
		return err
	}

	// For Ops with responses we proxy the raw response message over.
	if msg.GetOpCode().HasResponse() {
		if err := protocol.CopyMessage(c, s); err != nil {
			return err
		}
	}

	return nil
}
