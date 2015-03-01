package proxy

import (
	"io"

	"github.com/mcuadros/exmongodb/protocol"
)

type Middleware interface {
	Handle(h *protocol.MsgHeader, c io.ReadWriter, s io.ReadWriter) error
}
