package proxy

import (
	"io"

	"github.com/mcuadros/lemondb/protocol"
)

type Middleware interface {
	Handle(m protocol.Message, c io.ReadWriter, s io.ReadWriter) error
}
