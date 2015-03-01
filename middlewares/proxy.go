package middlewares

import (
	"io"

	"github.com/mcuadros/exmongodb/protocol"
)

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

	if err := m.Copy(h, c, s); err != nil {
		return err
	}

	//if _, err := io.CopyN(s, c, int64(h.MessageLength-protocol.HeaderLen)); err != nil {
	//	return err
	//}

	// For Ops with responses we proxy the raw response message over.
	if h.OpCode.HasResponse() {
		if err := protocol.CopyMessage(c, s); err != nil {
			return err
		}
	}

	return nil
}

func (m *ProxyMiddleware) Copy(
	h *protocol.MsgHeader,
	c io.ReadWriter,
	s io.ReadWriter,
) error {
	b := make([]byte, h.MessageLength-protocol.HeaderLen)
	if _, err := io.ReadFull(c, b); err != nil {
		return err
	}

	//fmt.Printf("HEX %q\n", hex.EncodeToString(b))
	s.Write(b)

	return nil
}
