package middlewares

import (
	"io"

	"github.com/mcuadros/exmongodb/protocol"
)

const headerLen = 16

type ProxyMiddleware struct{}

// proxyMessage proxies a message, possibly it's response, and possibly a
// follow up call.
func (m *ProxyMiddleware) Handle(
	h *protocol.MsgHeader,
	c io.ReadWriter,
	s io.ReadWriter,
) error {
	if err := h.WriteTo(s); err != nil {
		return err
	}

	if _, err := io.CopyN(s, c, int64(h.MessageLength-headerLen)); err != nil {
		return err
	}

	// For Ops with responses we proxy the raw response message over.
	if h.OpCode.HasResponse() {
		if err := protocol.CopyMessage(c, s); err != nil {
			return err
		}
	}

	return nil
}
